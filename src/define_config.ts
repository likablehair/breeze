import type { ConnectionOptions, JobsOptions, WorkerOptions } from 'bullmq'

export type Config = {
  connection: ConnectionOptions
  queue: string
  queues: string[]
  options: JobsOptions
  jobsDirectory?: string
  workerOptions?: Omit<WorkerOptions, 'connection' | 'concurrency'>
}

export function defineConfig(config: Config) {
  return config
}
