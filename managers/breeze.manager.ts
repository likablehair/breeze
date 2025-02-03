import { Worker, QueueEvents } from 'bullmq'
import type { LoggerService } from '@adonisjs/core/types'
import Ws from '../services/ws.js'
import app from '@adonisjs/core/services/app'
import { defineConfig } from '../src/define_config.js'
import { Job } from '../src/job.js'

export class BreezeManager {
  private logger: LoggerService

  constructor() {}

  async process() {
    const concurrency: number = 1
    const config = app.config.get<ReturnType<typeof defineConfig>>('jobs', {})
    const logger = await app.container.make('logger')
    const jobs = await app.container.make('jobs.list')
    const queues = config.queues || [config.queue]

    logger.info(`Processing jobs from the ${JSON.stringify(queues)} queues.`)

    const workers: Worker[] = []

    app.terminating(async () => {
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
            instance = await app.container.make(jobClass)
          } catch (error) {
            logger.error(`Cannot instantiate job ${job.name}`)
            return
          }
          instance.job = job
          instance.logger = logger

          console.log(instance.job)
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
      // this.bindQueueEvent(queueEvents)
      workers.push(worker)
    }
  }

  private bindEvents(worker: Worker, queueEvents: QueueEvents) {
    worker.on('completed', (job) => {
      this.logger.info(`Job ${job.id} completed`)
      Ws.io.emit(`jobs:${job.queueName}`, {
        event: 'completed',
        jobId: job.id,
      })
    })

    worker.on('failed', (job, err) => {
      this.logger.error(`Job ${job} failed: ${err.message}`)
      Ws.io.emit(`jobs:${job}`, {
        event: 'failed',
        jobId: job,
        reason: err.message,
      })
    })

    worker.on('stalled', (job) => {
      this.logger.warn(`Job ${job} stalled`)
      Ws.io.emit(`jobs:${job}`, { event: 'stalled', jobId: job })
    })

    worker.on('progress', (job, progress) => {
      this.logger.info(`Job ${job.id} progress: ${progress}%`)
      Ws.io.emit(`jobs:${job.queueName}`, {
        event: 'progress',
        jobId: job.id,
        progress,
      })
    })

    queueEvents.on('waiting', ({ jobId }) => {
      this.logger.info(`Job ${jobId} is waiting`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'waiting', jobId })
    })

    queueEvents.on('delayed', ({ jobId }) => {
      this.logger.info(`Job ${jobId} is delayed`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'delayed', jobId })
    })

    queueEvents.on('failed', ({ jobId, failedReason }) => {
      this.logger.error(`Job ${jobId} failed: ${failedReason}`)
      Ws.io.emit(`jobs:${worker.name}`, {
        event: 'failed',
        jobId,
        reason: failedReason,
      })
    })

    queueEvents.on('completed', ({ jobId }) => {
      this.logger.info(`Job ${jobId} completed`)
      Ws.io.emit(`jobs:${worker.name}`, { event: 'completed', jobId })
    })
  }
}
