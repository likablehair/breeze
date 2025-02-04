import { Worker, QueueEvents, Processor, WorkerOptions, QueueEventsOptions } from 'bullmq'
import type { ApplicationService } from '@adonisjs/core/types'
// import Ws from '../services/ws.js'
import { defineConfig } from '../src/define_config.js'
import { Job } from '../src/job.js'
import winston from 'winston'

const customColors = {
  info: 'green',
  warn: 'yellow',
  error: 'red',
}

winston.addColors(customColors)

const logger = winston.createLogger({
  format: winston.format.combine(
    winston.format.colorize({ all: true }),
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `[${timestamp}] ${level}: ${message}`)
  ),
  transports: [new winston.transports.Console()],
})

export interface EventListener {
  eventName: string
  method: string
}

export class BreezeManager {
  constructor(private app: ApplicationService) {}

  async process() {
    const concurrency: number = 1
    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {})
    const jobs = await this.app.container.make('jobs.list')
    const queues = config.queues || [config.queue]

    logger.info(`Processing jobs from the ${JSON.stringify(queues)} queues.`)

    const workers: Worker[] = []

    this.app.terminating(async () => {
      await Promise.allSettled(workers.map((worker) => worker.close()))
    })

    for (const queueName of queues) {
      const jobClass = jobs[queueName]
      if (!jobClass) {
        logger.error(`Cannot find job ${queueName}`)
      }
      let instance: Job
      try {
        instance = await this.app.container.make(jobClass)
      } catch (error) {
        logger.error(`Cannot initialize job ${queueName}`)
        return
      }

      const workerOptions: WorkerOptions = {
        ...(config.workerOptions || {}),
        connection: config.connection,
        concurrency: concurrency,
      }

      const queueEventsOptions: QueueEventsOptions = {
        connection: config.connection,
      }

      // const events = this._getEventListener(instance)
      // console.log(events)

      const processor: Processor = async (job) => {
        try {
          logger.info(`Job ${queueName} started`)
          return await instance.handle(job.data)
        } catch (error) {
          logger.error(error)
          return Promise.reject(error)
        }
      }

      const worker = new Worker(queueName, processor, workerOptions)
      const queueEvents = new QueueEvents(queueName, queueEventsOptions)

      // instance.workerListener.forEach(function (item) {
      //   worker.on(item.eventName as any, instance.instance[item.method].bind(instance))
      // })

      // instance.queueLister.forEach(function (item) {
      //   queueEvents.on(
      //     item.eventName as any,
      //     instance.instance[item.method].bind(instance)
      //   )
      // })

      this.bindEvents(worker, queueEvents)
      workers.push(worker)
    }
  }

  // private _getListener(job: Job): {
  //   workerListener: EventListener[]
  //   queueListener: EventListener[]
  // } {
  //   const listener = Object.getOwnPropertyNames(Object.getPrototypeOf(job)).reduce(
  //     (events, method: string) => {
  //       console.log(events)
  //       console.log(method)
  //       if (method.startsWith('workerOn')) {
  //         let eventName = method
  //           .replace(/^workerOn(\w)/, (_, group) => group.toLowerCase())
  //           .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`.trim())

  //         if (eventName === 'ioredisclose') {
  //           eventName = 'ioredis:close'
  //         }

  //         events.workerListener.push({ eventName, method })
  //       } else if (method.startsWith('queueOn')) {
  //         let eventName = method
  //           .replace(/^queueOn(\w)/, (_, group) => group.toLowerCase())
  //           .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`.trim())

  //         if (eventName === 'retriesexhausted') {
  //           eventName = 'retries-exhausted'
  //         }

  //         if (eventName === 'waitingchildren') {
  //           eventName = 'waiting-children'
  //         }

  //         events.queueListener.push({ eventName, method })
  //       }

  //       return events
  //     },
  //     {
  //       workerListener: [],
  //       queueListener: [],
  //     } as {
  //       workerListener: EventListener[]
  //       queueListener: EventListener[]
  //     }
  //   )

  //   return listener
  // }

  private bindEvents(worker: Worker, queueEvents: QueueEvents) {
    worker.on('completed', (job) => {
      logger.info(`Job ${job.id} completed`)
      // Ws.io.emit(`jobs:${job.queueName}`, {
      //   event: 'completed',
      //   jobId: job.id,
      // })
    })

    worker.on('failed', (job, err) => {
      logger.error(`Job ${job} failed: ${err.message}`)
      // Ws.io.emit(`jobs:${job}`, {
      //   event: 'failed',
      //   jobId: job,
      //   reason: err.message,
      // })
    })

    worker.on('stalled', (job) => {
      logger.warn(`Job ${job} stalled`)
      // Ws.io.emit(`jobs:${job}`, { event: 'stalled', jobId: job })
    })

    worker.on('progress', (job, progress) => {
      logger.info(`Job ${job.id} progress: ${progress}%`)
      // Ws.io.emit(`jobs:${job.queueName}`, {
      //   event: 'progress',
      //   jobId: job.id,
      //   progress,
      // })
    })

    queueEvents.on('waiting', ({ jobId }) => {
      logger.info(`Job ${jobId} is waiting`)
      // Ws.io.emit(`jobs:${worker.name}`, { event: 'waiting', jobId })
    })

    queueEvents.on('delayed', ({ jobId }) => {
      logger.info(`Job ${jobId} is delayed`)
      // Ws.io.emit(`jobs:${worker.name}`, { event: 'delayed', jobId })
    })

    queueEvents.on('failed', ({ jobId, failedReason }) => {
      logger.error(`Job ${jobId} failed: ${failedReason}`)
      // Ws.io.emit(`jobs:${worker.name}`, {
      //   event: 'failed',
      //   jobId,
      //   reason: failedReason,
      // })
    })

    queueEvents.on('completed', ({ jobId }) => {
      logger.info(`Job ${jobId} completed`)
      // Ws.io.emit(`jobs:${worker.name}`, { event: 'completed', jobId })
    })
  }
}
