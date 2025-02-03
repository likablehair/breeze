import { BaseCommand, flags } from '@adonisjs/core/ace'
import type { CommandOptions } from '@adonisjs/core/types/ace'
import { Worker } from 'bullmq'
import { QueueEvents } from 'bullmq'
import type { Job, defineConfig } from '../index.js'
// import ws from '../services/ws.js'

export default class JobsListen extends BaseCommand {
  static commandName = 'jobs:listen'
  static description = ''

  static options: CommandOptions = {
    startApp: true,
    staysAlive: true,
  }

  @flags.array({
    description: 'The names of the queues to work',
    parse(input) {
      return input.flatMap((queue) =>
        queue
          .split(',')
          .map((q) => q.trim())
          .filter(Boolean)
      )
    },
  })
  declare queue: string[]

  @flags.number({
    description: 'Amount of jobs that a single worker is allowed to work on in parallel.',
    default: 1,
  })
  declare concurrency: number

  private bindEvents(worker: Worker, queueEvents: QueueEvents) {
    worker.on('completed', (job) => {
      this.logger.info(`Job ${job.id} completed`)
      // ws.io.emit(`jobs:${job.queueName}`, {
      //   event: 'completed',
      //   jobId: job.id,
      // })
    })

    worker.on('failed', (job, err) => {
      this.logger.error(`Job ${job} failed: ${err.message}`)
      // ws.io.emit(`jobs:${job}`, {
      //   event: 'failed',
      //   jobId: job,
      //   reason: err.message,
      // })
    })

    worker.on('stalled', (job) => {
      this.logger.warning(`Job ${job} stalled`)
      // ws.io.emit(`jobs:${job}`, { event: 'stalled', jobId: job })
    })

    worker.on('progress', (job, progress) => {
      this.logger.info(`Job ${job.id} progress: ${progress}%`)
      // ws.io.emit(`jobs:${job.queueName}`, {
      //   event: 'progress',
      //   jobId: job.id,
      //   progress,
      // })
    })

    queueEvents.on('waiting', ({ jobId }) => {
      this.logger.info(`Job ${jobId} is waiting`)
      // ws.io.emit(`jobs:${worker.name}`, { event: 'waiting', jobId })
    })

    queueEvents.on('delayed', ({ jobId }) => {
      this.logger.info(`Job ${jobId} is delayed`)
      // ws.io.emit(`jobs:${worker.name}`, { event: 'delayed', jobId })
    })

    queueEvents.on('failed', ({ jobId, failedReason }) => {
      this.logger.error(`Job ${jobId} failed: ${failedReason}`)
      // ws.io.emit(`jobs:${worker.name}`, {
      //   event: 'failed',
      //   jobId,
      //   reason: failedReason,
      // })
    })

    queueEvents.on('completed', ({ jobId }) => {
      this.logger.info(`Job ${jobId} completed`)
      // ws.io.emit(`jobs:${worker.name}`, { event: 'completed', jobId })
    })
  }

  async run() {
    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {})
    const logger = await this.app.container.make('logger')
    const router = await this.app.container.make('router')
    const jobs = await this.app.container.make('jobs.list')
    const queues = config.queues || [config.queue] || this.queue

    const workers: Worker[] = []

    router.commit()

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
          instance.logger = logger

          logger.info(`Job ${job.name} started`)
          await instance.handle(job.data)
          logger.info(`Job ${job.name} finished`)
        },
        {
          ...(config.workerOptions || {}),
          connection: config.connection,
          concurrency: this.concurrency,
        }
      )
      const queueEvents = new QueueEvents(queueName)
      this.bindEvents(worker, queueEvents)
      workers.push(worker)
    }

    logger.info(`Processing jobs from the ${JSON.stringify(queues)} queues.`)
  }
}
