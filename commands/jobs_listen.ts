import { BaseCommand, flags } from '@adonisjs/core/ace'
import type { CommandOptions } from '@adonisjs/core/types/ace'
import type { defineConfig } from '../index.js'

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

  async run() {
    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {})
    const logger = await this.app.container.make('logger')
    const router = await this.app.container.make('router')
    const breeze = await this.app.container.make('breeze')
    const queues = config.queues || [config.queue] || this.queue
    router.commit()

    try {
      await breeze.process()
      logger.info(`Processing jobs from the ${JSON.stringify(queues)} queues.`)
    } catch (error) {
      new Error(error)
    }
  }
}
