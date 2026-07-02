import type { ConnectionOptions, Job, JobsOptions, RateLimiterOptions, WorkerOptions } from 'bullmq'
import {
  type AllPossibleParams,
  type Breeze,
  type ListenerScope,
  type ListenersType,
  type QueueListenerName,
  type QueueListenerParamsMap,
  type WorkerListenerName,
  type WorkerListenerParamsMap,
} from './breeze.js'
import { type EventListener } from '../managers/breeze.manager.js'
import type winston from 'winston'

export type Config = {
  connection: ConnectionOptions
  queue: string
  queues: string[]
  options: JobsOptions
  jobsDirectory?: string
  limiter?: RateLimiterOptions
  workerOptions?: Omit<WorkerOptions, 'connection' | 'concurrency'>
  processor?: <T = any, R = any>(params: {
    job: Breeze<T, R>
    process: Job<T, R, string>
    logger: winston.Logger
  }) => any
  onQueueEvents?: {
    [eventMethod in QueueListenerName]?: <T = any, R = any>(params: {
      job: Breeze<T, R>
      params: QueueListenerParamsMap[eventMethod]
    }) => void
  }
  onWorkerEvents?: {
    [eventMethod in WorkerListenerName]?: <T = any, R = any>(params: {
      job: Breeze<T, R>
      params: WorkerListenerParamsMap[eventMethod]
    }) => void
  }
  onGlobalEvents?: <T = any, R = any, K extends ListenersType = ListenersType>(params: {
    event: EventListener<K>
    job: Breeze<T, R>
    type: ListenerScope
    params: AllPossibleParams
  }) => void
}

export function defineConfig(config: Config) {
  return config
}
