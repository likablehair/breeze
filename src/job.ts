import { Job as BullmqJob, JobsOptions } from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'

type JobHandle<T> = T extends (payload: infer P) => any ? (undefined extends P ? any : P) : any

export abstract class Job {
  declare job: BullmqJob
  declare logger: LoggerService
  declare static app: ApplicationService

  static async dispatch<T extends Job>(
    this: new () => T,
    payload: JobHandle<T['handle']>,
    options: JobsOptions & { queueName?: string } = {}
  ) {
    console.log('pippo')
    const { default: app } = await import('@adonisjs/core/services/app')
    console.log(app)
    const { dispatch } = await import('../services/main.js')
    console.log(this)
    return await dispatch(this, payload, options)
  }

  static async dispatchSync<T extends Job>(this: new () => T, payload: JobHandle<T['handle']>) {
    const { default: app } = await import('@adonisjs/core/services/app')

    const logger = await app.container.make('logger')
    const instance: Job = await app.container.make(this)

    instance.logger = logger

    await instance.handle(payload)
  }

  abstract handle(payload: any): Promise<void> | void
}
