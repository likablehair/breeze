import { Worker, QueueEvents } from 'bullmq'
import type { ApplicationService } from '@adonisjs/core/types'
import Ws from '../services/ws.js'
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
    // const logger = await this.app.container.make('logger')
    const jobs = await this.app.container.make('jobs.list')
    const queues = config.queues || [config.queue]

    logger.info(`Processing jobs from the ${JSON.stringify(queues)} queues.`)

    const workers: Worker[] = []

    this.app.terminating(async () => {
      await Promise.allSettled(workers.map((worker) => worker.close()))
    })

    for (const queueName of queues) {
      const worker = new Worker(
        queueName,
        async (job) => {
          const jobClass = jobs[job.name]
          if (!jobClass) {
            logger.error(`Cannot find job ${job.name}`)
          }
          let instance: Job
          try {
            instance = await this.app.container.make(jobClass)
          } catch (error) {
            logger.error(`Cannot instantiate job ${job.name}`)
            return
          }
          instance.job = job
          // instance.logger = logger

          // console.log(instance.job)
          const events = this._getEventListener(instance)
          console.log(events)

          logger.info(`Job ${job.name} started`)
          await instance.handle(job.data)
          logger.info(`Job ${job.name} finished`)
        },
        {
          ...(config.workerOptions || {}),
          connection: config.connection,
          concurrency: concurrency,
        }
      )
      const queueEvents = new QueueEvents(queueName)
      this.bindEvents(worker, queueEvents)
      workers.push(worker)
    }
  }

  private _getEventListener(job: Job): {
    workerEvent: EventListener[]
    queueEvents: EventListener[]
  } {
    const jobEvents = Object.getOwnPropertyNames(Object.getPrototypeOf(job)).reduce(
      (events, method: string) => {
        if (method.startsWith('workerOn')) {
          let eventName = method
            .replace(/^workerOn(\w)/, (_, group) => group.toLowerCase())
            .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`.trim())

          if (eventName === 'ioredisclose') {
            eventName = 'ioredis:close'
          }

          events.workerEvent.push({ eventName, method })
        } else if (method.startsWith('queueOn')) {
          let eventName = method
            .replace(/^queueOn(\w)/, (_, group) => group.toLowerCase())
            .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`.trim())

          if (eventName === 'retriesexhausted') {
            eventName = 'retries-exhausted'
          }

          if (eventName === 'waitingchildren') {
            eventName = 'waiting-children'
          }

          events.queueEvents.push({ eventName, method })
        }

        return events
      },
      {
        workerEvent: [],
        queueEvents: [],
      } as {
        workerEvent: EventListener[]
        queueEvents: EventListener[]
      }
    )

    return jobEvents
  }
  private bindEvents(worker: Worker, queueEvents: QueueEvents) {
    worker.on('completed', (job) => {
      logger.info(`Job ${job.id} completed`)
      Ws.io.emit(`jobs:${job.queueName}`, {
        event: 'completed',
        jobId: job.id,
      })
    })

    worker.on('failed', (job, err) => {
      logger.error(`Job ${job} failed: ${err.message}`)
      Ws.io.emit(`jobs:${job}`, {
        event: 'failed',
        jobId: job,
        reason: err.message,
      })
    })

    worker.on('stalled', (job) => {
      logger.warn(`Job ${job} stalled`)
      Ws.io.emit(`jobs:${job}`, { event: 'stalled', jobId: job })
    })

    worker.on('progress', (job, progress) => {
      logger.info(`Job ${job.id} progress: ${progress}%`)
      Ws.io.emit(`jobs:${job.queueName}`, {
        event: 'progress',
        jobId: job.id,
        progress,
      })
    })

    queueEvents.on('waiting', ({ jobId }) => {
      logger.info(`Job ${jobId} is waiting`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'waiting', jobId })
    })

    queueEvents.on('delayed', ({ jobId }) => {
      logger.info(`Job ${jobId} is delayed`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'delayed', jobId })
    })

    queueEvents.on('failed', ({ jobId, failedReason }) => {
      logger.error(`Job ${jobId} failed: ${failedReason}`)
      Ws.io.emit(`jobs:${worker.name}`, {
        event: 'failed',
        jobId,
        reason: failedReason,
      })
    })

    queueEvents.on('completed', ({ jobId }) => {
      logger.info(`Job ${jobId} completed`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'completed', jobId })
    })
  }
}
