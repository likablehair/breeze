import {
  Worker,
  QueueEvents,
  type Processor,
  type WorkerOptions,
  type QueueEventsOptions,
} from 'bullmq'
import type { ApplicationService } from '@adonisjs/core/types'
import { type defineConfig } from '../src/define_config.js'
import {
  type Breeze,
  type ListenersType,
  type ListenerParamsLookup,
  type ListenerScope,
  type WorkerListenerName,
  type QueueListenerName,
  listenerMapping,
} from '../src/breeze.js'
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

export interface EventListener<K extends ListenersType = ListenersType> {
  eventName: string
  method: K
  params: ListenerParamsLookup[K]
}

type BreezeConfig = ReturnType<typeof defineConfig>

export class BreezeManager {
  private workers: Worker[] = []

  constructor(private app: ApplicationService) {}

  async process(): Promise<void> {
    const config = this.app.config.get<BreezeConfig>('jobs', {})
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

  private run(job: Breeze, queueName: string, config: BreezeConfig): void {
    const workerOptions: WorkerOptions = {
      ...config.workerOptions,
      connection: config.connection,
      concurrency: job.concurrency ?? 1,
      limiter: job.limiter ?? config.limiter,
      telemetry: config.telemetry,
    }
    const queueEventsOptions: QueueEventsOptions = {
      connection: config.connection,
    }

    const processor: Processor = async (process) => {
      try {
        logger.info(`Job ${queueName} started. Job id: ${process.id}`)
        if (config.processor) {
          return await config.processor({ process, job, logger })
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

    const jobListeners = this.getListeners()
    job.workerListener = jobListeners.workerListener

    for (const listener of jobListeners.workerListener) {
      const handler = createListenerHandler(listener, job, 'worker', config)
      worker.on(listener.eventName as any, handler)
    }

    job.queueListener = jobListeners.queueListener
    for (const listener of jobListeners.queueListener) {
      const handler = createListenerHandler(listener, job, 'queue', config)
      queueEvents.on(listener.eventName as any, handler)
    }

    this.workers.push(worker)
  }

  private getListeners(): {
    workerListener: EventListener<WorkerListenerName>[]
    queueListener: EventListener<QueueListenerName>[]
  } {
    const workerListener = Object.entries(listenerMapping.worker).map(([method, definition]) => ({
      method: method as WorkerListenerName,
      eventName: definition.eventName,
      params: definition.params,
    })) as EventListener<WorkerListenerName>[]

    const queueListener = Object.entries(listenerMapping.queue).map(([method, definition]) => ({
      method: method as QueueListenerName,
      eventName: definition.eventName,
      params: definition.params,
    })) as EventListener<QueueListenerName>[]

    return { workerListener, queueListener }
  }
}

export function createListenerHandler<K extends ListenersType>(
  listener: EventListener<K>,
  job: Breeze,
  type: ListenerScope,
  config: BreezeConfig
) {
  return (methodParams: ListenerParamsLookup[K]) => {
    if (type === 'worker') {
      config.onWorkerEvents?.[listener.method]?.({
        job,
        params: methodParams,
      })
    } else {
      config.onQueueEvents?.[listener.method]?.({
        job,
        params: methodParams,
      })
    }

    config.onGlobalEvents?.({
      event: listener,
      job,
      type,
      params: methodParams,
    })

    const jobMethod = job[listener.method] as ((...params: any[]) => any) | undefined
    if (jobMethod) {
      return jobMethod.apply(job, Object.values(methodParams || {}))
    }

    return undefined
  }
}
