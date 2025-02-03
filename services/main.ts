import app from '@adonisjs/core/services/app'
import { JobsOptions } from 'bullmq'
import type { Dispatcher } from '../src/dispatcher.js'
import type { Job } from '../src/job.js'
import { BreezeManager } from '../managers/breeze.manager.js'

let dispatcher: Dispatcher
let breeze: BreezeManager

await app.booted(async () => {
  ;(dispatcher = await app.container.make('jobs.dispatcher')),
    (breeze = await app.container.make('breeze'))
})

export const dispatch = async (
  jobOrClosure: Function | typeof Job,
  payload: any = {},
  options: JobsOptions & { queueName?: string } = {}
) => {
  return await dispatcher.dispatch(jobOrClosure, payload, options)
}

export { dispatcher as default, breeze }
