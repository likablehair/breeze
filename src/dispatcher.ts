import type { ApplicationService } from '@adonisjs/core/types'
import { Queue as BullmqQueue, JobsOptions } from 'bullmq'
import { Job } from './job.js'
import { type defineConfig } from './define_config.js'

export class Dispatcher {
  constructor(private app: ApplicationService) {}

  async dispatch(
    job: typeof Job,
    payload: any,
    options: JobsOptions & { queueName?: string } = {}
  ) {
    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {}) as ReturnType<
      typeof defineConfig
    >
    const queues = await this.app.container.make('jobs.queues')
    const queueName = options.queueName || config.queues[0]
    const queue = queues[queueName] as BullmqQueue

    if (!queue) {
      throw new Error(`Queue ${queueName} not found`)
    }

    const bullmqJob = await queue.add(job.name, payload, options)
    return bullmqJob
  }
}
