import { Job as BullmqJob, JobsOptions } from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'

type JobHandle<T> = T extends (payload: infer P) => any ? (undefined extends P ? any : P) : any

export abstract class Job<TPayload = any, TResult = any> {
  declare job: BullmqJob<TPayload, TResult>
  declare logger: LoggerService
  declare static app: ApplicationService

  abstract handle(payload: TPayload): Promise<TResult> | TResult

  static async dispatch<T extends Job>(
    this: new () => T,
    payload: JobHandle<T['handle']>,
    options: JobsOptions & { queueName?: string } = {}
  ) {
    const { dispatch } = await import('../services/main.js')
    return await dispatch(this, payload, options)
  }

  static async dispatchSync<T extends Job>(this: new () => T, payload: JobHandle<T['handle']>) {
    const { default: app } = await import('@adonisjs/core/services/app')

    const logger = await app.container.make('logger')
    const instance: Job = await app.container.make(this)

    instance.logger = logger

    await instance.handle(payload)
  }
}
