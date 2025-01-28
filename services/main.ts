// import { JobsOptions } from 'bullmq'
// import { Dispatcher } from '../src/dispatcher.js'
// import type { Job } from '../src/job.js'
// import app, { setApp } from '@adonisjs/core/services/app';

// let dispatcher: Dispatcher;
// // console.log('Breeze > main.ts')
// //console.log(app)
// setApp(app);
// await app.booted(async () => {
//   dispatcher = await app.container.make('jobs.dispatcher')
// })

// export const dispatch = async (
//   jobOrClosure: Function | typeof Job,
//   payload: any = {},
//   options: JobsOptions & { queueName?: string } = {}
// ) => {
//   console.log(jobOrClosure)
//   console.log(payload)
//   console.log(options)
//   await dispatcher.dispatch(jobOrClosure, payload, options)
// }

// export { dispatcher as default }
