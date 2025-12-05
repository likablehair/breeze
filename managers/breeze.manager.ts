import { Worker, QueueEvents, Processor, WorkerOptions, QueueEventsOptions } from 'bullmq'
import type { ApplicationService } from '@adonisjs/core/types'
import { defineConfig } from '../src/define_config.js'
import { isListener, Breeze, ListenersType } from '../src/breeze.js'
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
  method: ListenersType
}

export class BreezeManager {
  private workers: Worker[] = []

  constructor(private app: ApplicationService) {}

  async process(): Promise<void> {
    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {})
    const jobs = await this.app.container.make('breeze.list')
    const queues: string[] = config.queues || [config.queue]

    logger.info(`Processing jobs from queues: ${JSON.stringify(queues)}`)

    this.app.terminating(async () => {
      await Promise.allSettled(this.workers.map((worker) => worker.close()))
    })

    for (const queueKey of queues) {
      const jobClass = jobs[queueKey]
      if (!jobClass) {
        logger.error(`Cannot find job class for queue: ${queueKey}`)
        continue
      }

      let job: Breeze
      try {
        job = await this.app.container.make(jobClass)
      } catch (error) {
        logger.error(`Failed to initialize job: ${queueKey} - ${error}`)
        continue
      }

      this.run(job, queueKey, config)
    }
  }

  private run(job: Breeze, queueName: string, config: ReturnType<typeof defineConfig>): void {
    const workerOptions: WorkerOptions = {
      ...config.workerOptions,
      connection: config.connection,
      concurrency: job.concurrency ?? 1,
    }
    const queueEventsOptions: QueueEventsOptions = {
      connection: config.connection,
    }

    const processor: Processor = async (process) => {
      try {
        logger.info(`Job ${queueName} started. Job id: ${process.id}`)
        if (config.processor) {
          return await config.processor({ process, job })
        } else {
          let result = await job.handle(process)
          logger.info(`Job ${queueName} completed. Job id: ${process.id}`)
          return result
        }
      } catch (error) {
        logger.error(`Job ${queueName} failed: ${error}`)
        logger.error(error)
        throw error
      }
    }

    const worker = new Worker(queueName, processor, workerOptions)
    const queueEvents = new QueueEvents(queueName, queueEventsOptions)

    const listeners = this.getListeners(job)
    job.workerListener = listeners.workerListener

    for (const { eventName, method } of job.workerListener) {
      worker.on(eventName as any, job[method].bind(job))
    }

    job.queueListener = listeners.queueListener
    for (const { eventName, method } of job.queueListener) {
      queueEvents.on(eventName as any, job[method].bind(job))
    }
    this.workers.push(worker)
  }

  private getListeners(job: Breeze): {
    workerListener: EventListener[]
    queueListener: EventListener[]
  } {
    const listener = Object.getOwnPropertyNames(Object.getPrototypeOf(job))
      .filter((el) => isListener(el))
      .reduce(
        (events, method) => {
          if (method.startsWith('workerOn')) {
            let eventName = method
              .replace(/^workerOn(\w)/, (_, group) => group.toLowerCase())
              .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`.trim())

            if (eventName === 'ioredisclose') {
              eventName = 'ioredis:close'
            }

            events.workerListener.push({ eventName, method })
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

            events.queueListener.push({ eventName, method })
          }

          return events
        },
        {
          workerListener: [] as EventListener[],
          queueListener: [] as EventListener[],
        }
      )

    return listener
  }
}
