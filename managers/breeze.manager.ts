import { Worker, QueueEvents, Processor, WorkerOptions, QueueEventsOptions } from 'bullmq'
import type { ApplicationService } from '@adonisjs/core/types'
import { defineConfig } from '../src/define_config.js'
import { isListener, Job, ListenersType } from '../src/job.js'
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
    const jobs = await this.app.container.make('jobs.list')
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

      let job: Job
      try {
        job = await this.app.container.make(jobClass)
      } catch (error) {
        logger.error(`Failed to initialize job: ${queueKey} - ${error}`)
        continue
      }

      this.run(job, queueKey, config)
    }
  }

  private run(job: Job, queueName: string, config: ReturnType<typeof defineConfig>): void {
    const workerOptions: WorkerOptions = {
      ...config.workerOptions,
      connection: config.connection,
      concurrency: 1,
    }

    const queueEventsOptions: QueueEventsOptions = {
      connection: config.connection,
    }

    const processor: Processor = async (process) => {
      try {
        logger.info(`Job ${queueName} started`)
        return await job.handle(process.data)
      } catch (error) {
        logger.error(`Job ${queueName} failed: ${error}`)
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

  private getListeners(job: Job): {
    workerListener: EventListener[]
    queueListener: EventListener[]
  } {
    return Object.getOwnPropertyNames(Object.getPrototypeOf(job))
      .filter(isListener)
      .reduce(
        (events, method) => {
          const eventName = method.replace(/^(workerOn|queueOn)(\w+)/, (_, group) =>
            `${group.charAt(0).toLowerCase()}${group.slice(1)}`.replace(/([A-Z]+)/, ' $1').trim()
          )

          if (method.startsWith('workerOn')) {
            events.workerListener.push({
              eventName: eventName === 'ioredisclose' ? 'ioredis:close' : eventName,
              method,
            })
          } else {
            events.queueListener.push({ eventName: this.mapQueueEventName(eventName), method })
          }

          return events
        },
        { workerListener: [] as EventListener[], queueListener: [] as EventListener[] }
      )
  }

  private mapQueueEventName(eventName: string): string {
    return eventName
      .replace('retriesexhausted', 'retries-exhausted')
      .replace('waitingchildren', 'waiting-children')
  }
}
