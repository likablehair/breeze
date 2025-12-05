import type { ConnectionOptions, Job, JobsOptions, WorkerOptions } from 'bullmq'
import { Breeze } from './breeze.js'

export type Config = {
  connection: ConnectionOptions
  queue: string
  queues: string[]
  options: JobsOptions
  jobsDirectory?: string
  workerOptions?: Omit<WorkerOptions, 'connection' | 'concurrency'>
  processor?: <T = any, R = any>(params: { job: Breeze<T, R>; process: Job<T, R, string> }) => any
}

export function defineConfig(config: Config) {
  return config
}
