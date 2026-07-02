import {
  type ConnectionOptions,
  type FlowJob,
  FlowProducer,
  type Job,
  type JobSchedulerJson,
  type JobsOptions,
  type Queue,
  RateLimiterOptions,
  type RepeatOptions,
} from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'
import { type EventListener } from '../managers/breeze.manager.js'

export const workerListenerMethods = [
  'workerOnActive',
  'workerOnClosed',
  'workerOnClosing',
  'workerOnCompleted',
  'workerOnDrained',
  'workerOnError',
  'workerOnFailed',
  'workerOnIoredisClose',
  'workerOnPaused',
  'workerOnProgress',
  'workerOnReady',
  'workerOnResumed',
  'workerOnStalled',
] as const

export const queueListenerMethods = [
  'queueOnActive',
  'queueOnAdded',
  'queueOnCleaned',
  'queueOnCompleted',
  'queueOnDebounced',
  'queueOnDeduplicated',
  'queueOnDelayed',
  'queueOnDrained',
  'queueOnDuplicated',
  'queueOnError',
  'queueOnFailed',
  'queueOnPaused',
  'queueOnProgress',
  'queueOnRemoved',
  'queueOnResumed',
  'queueOnRetriesExhausted',
  'queueOnStalled',
  'queueOnWaiting',
  'queueOnWaitingChildren',
] as const

export const listeners = [...workerListenerMethods, ...queueListenerMethods] as const

export type { Job }
export type ListenersType = (typeof listeners)[number]
export const isListener = (x: any): x is ListenersType => listeners.includes(x)
export abstract class Breeze<TPayload = any, TResult = any> {
  declare key: string
  declare concurrency: number
  declare limiter?: RateLimiterOptions
  declare instance?: Job<TPayload, TResult>
  declare logger?: LoggerService
  declare workerListener?: EventListener[]
  declare queueListener?: EventListener[]
  declare static app: ApplicationService
  declare static queues: any

  abstract handle(job: Job<TPayload, TResult>): Promise<TResult> | TResult

  async dispatch(
    payload: TPayload,
    options: JobsOptions & { queueName?: string } = {},
    queueName?: string
  ): Promise<Job<TPayload, TResult>> {
    if (!queueName) queueName = this.key
    const queue = Breeze.queues[queueName] as Queue

    if (!queue) {
      throw new Error(`Queue ${queueName} not found`)
    }

    this.instance = await queue.add(queueName, payload, options)
    return this.instance
  }

  static async addToFlowProducer({
    name,
    queueName,
    data,
    prefix,
    opts,
    children,
    connection,
  }: {
    name: string
    queueName: string
    data?: any
    prefix?: string
    opts?: Omit<JobsOptions, 'parent' | 'repeat'>
    children?: FlowJob[]
    connection: ConnectionOptions
  }) {
    const flowProducer = new FlowProducer({ connection })
    return await flowProducer.add({
      name,
      queueName,
      data,
      prefix,
      opts,
      children,
    })
  }

  static async getRedisJob(queueKey: string, jobId: string) {
    const job = await Breeze.queues[queueKey].getJob(jobId)

    return job as Job
  }

  async upsertJobScheduler(
    key: string,
    schedulerId: string,
    repeatOptions: RepeatOptions,
    jobTemplate?: {
      name?: string
      data?: TPayload
      opts?: Omit<JobsOptions, 'jobId' | 'repeat' | 'delay'>
    }
  ): Promise<Job<TPayload, TResult>> {
    return Breeze.queues[key].upsertJobScheduler(schedulerId, repeatOptions, jobTemplate)
  }

  static async getJobScheduler(key: string, jobId: string): Promise<JobSchedulerJson> {
    return Breeze.queues[key].getJobScheduler(jobId)
  }

  static async remove(key: string, jobId: string): Promise<void> {
    await Breeze.queues[key].remove(jobId)
  }

  static async removeJobScheduler(key: string, jobId: string): Promise<boolean> {
    return Breeze.queues[key].removeJobScheduler(jobId)
  }

  static async removeRecurringJob(key: string, repeatOptions: RepeatOptions): Promise<boolean> {
    return await Breeze.queues[key].removeRepeatable(key, repeatOptions)
  }

  protected boot?: (queue: Queue<TPayload, TResult>) => void

  protected async workerOnActive(_job: Breeze<TPayload, TResult>, _prev: string): Promise<void> {}

  protected async workerOnClosed(): Promise<void> {}

  protected async workerOnClosing(_msg: string): Promise<void> {}

  protected async workerOnCompleted(
    _job: Breeze<TPayload, TResult>,
    _result: TResult,
    _prev: string
  ): Promise<void> {}

  protected async workerOnDrained(): Promise<void> {}

  protected async workerOnError(_failedReason: Error): Promise<void> {}

  protected async workerOnFailed(
    _job: Breeze<TPayload, TResult> | undefined,
    _error: Error,
    _prev: string
  ): Promise<void> {}

  protected async workerOnIoredisClose(): Promise<void> {}

  protected async workerOnPaused(): Promise<void> {}

  protected async workerOnProgress(
    _job: Breeze<TPayload, TResult>,
    _progress: number | object
  ): Promise<void> {}

  protected async workerOnReady(): Promise<void> {}

  protected async workerOnResumed(): Promise<void> {}

  protected async workerOnStalled(_jobId: string, _prev: string): Promise<void> {}

  protected async queueOnActive(
    _args: { jobId: string; prev?: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnAdded(
    _args: { jobId: string; name: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnCleaned(_args: { count: string }, _id: string): Promise<void> {}

  protected async queueOnCompleted(
    _args: { jobId: string; returnvalue: string; prev?: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnDebounced(
    _args: { jobId: string; debounceId: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnDeduplicated(
    _args: { jobId: string; deduplicationId: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnDelayed(
    _args: { jobId: string; delay: number },
    _id: string
  ): Promise<void> {}

  protected async queueOnDrained(_id: string): Promise<void> {}

  protected async queueOnDuplicated(_args: { jobId: string }, _id: string): Promise<void> {}

  protected async queueOnError(args: Error): Promise<void> {
    console.log(`Queue error: ${args.message}`)
  }

  protected async queueOnFailed(
    _args: { jobId: string; failedReason: string; prev?: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnPaused(_args: {}, _id: string): Promise<void> {}

  protected async queueOnProgress(
    _args: { jobId: string; data: number | object },
    _id: string
  ): Promise<void> {}

  protected async queueOnRemoved(
    _args: { jobId: string; prev: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnResumed(_args: {}, _id: string): Promise<void> {}

  protected async queueOnRetriesExhausted(
    _args: { jobId: string; attemptsMade: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnStalled(_args: { jobId: string }, _id: string): Promise<void> {}

  protected async queueOnWaiting(
    _args: { jobId: string; prev?: string },
    _id: string
  ): Promise<void> {}

  protected async queueOnWaitingChildren(_args: { jobId: string }, _id: string): Promise<void> {}
}

export type WorkerListenerName = (typeof workerListenerMethods)[number]
export type QueueListenerName = (typeof queueListenerMethods)[number]
export type ListenerScope = 'worker' | 'queue'

type WorkerListenerParams = {
  workerOnActive: { job: Breeze<any, any>; prev: string }
  workerOnClosed: {}
  workerOnClosing: { msg: string }
  workerOnCompleted: { job: Breeze<any, any>; result: any; prev: string }
  workerOnDrained: {}
  workerOnError: { failedReason: Error }
  workerOnFailed: { job: Breeze<any, any> | undefined; error: Error; prev: string }
  workerOnIoredisClose: {}
  workerOnPaused: {}
  workerOnProgress: { job: Breeze<any, any>; progress: number | object }
  workerOnReady: {}
  workerOnResumed: {}
  workerOnStalled: { jobId: string; prev: string }
}

type QueueListenerParams = {
  queueOnActive: { args: { jobId: string; prev?: string }; id: string }
  queueOnAdded: { args: { jobId: string; name: string }; id: string }
  queueOnCleaned: { args: { count: string }; id: string }
  queueOnCompleted: { args: { jobId: string; returnvalue: string; prev?: string }; id: string }
  queueOnDebounced: { args: { jobId: string; debounceId: string }; id: string }
  queueOnDeduplicated: { args: { jobId: string; deduplicationId: string }; id: string }
  queueOnDelayed: { args: { jobId: string; delay: number }; id: string }
  queueOnDrained: { id: string }
  queueOnDuplicated: { args: { jobId: string }; id: string }
  queueOnError: { args: Error }
  queueOnFailed: { args: { jobId: string; failedReason: string; prev?: string }; id: string }
  queueOnPaused: { args: {}; id: string }
  queueOnProgress: { args: { jobId: string; data: number | object }; id: string }
  queueOnRemoved: { args: { jobId: string; prev: string }; id: string }
  queueOnResumed: { args: {}; id: string }
  queueOnRetriesExhausted: { args: { jobId: string; attemptsMade: string }; id: string }
  queueOnStalled: { args: { jobId: string }; id: string }
  queueOnWaiting: { args: { jobId: string; prev?: string }; id: string }
  queueOnWaitingChildren: { args: { jobId: string }; id: string }
}

type WorkerListenerDefinitions = {
  [K in WorkerListenerName]: {
    eventName: string
    params: WorkerListenerParams[K]
  }
}

type QueueListenerDefinitions = {
  [K in QueueListenerName]: {
    eventName: string
    params: QueueListenerParams[K]
  }
}

export type WorkerListenerParamsMap = WorkerListenerParams
export type QueueListenerParamsMap = QueueListenerParams
export type ListenerParamsLookup = WorkerListenerParamsMap & QueueListenerParamsMap

type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (k: infer I) => void
  ? I
  : never

type DeepPartial<T> = T extends object
  ? {
      [P in keyof T]?: DeepPartial<T[P]>
    }
  : T

export type AllPossibleParams = DeepPartial<
  UnionToIntersection<ListenerParamsLookup[keyof ListenerParamsLookup]>
>

export type ListenerMapping = {
  worker: WorkerListenerDefinitions
  queue: QueueListenerDefinitions
}

const workerEventNames: Record<WorkerListenerName, string> = {
  workerOnActive: 'active',
  workerOnClosed: 'closed',
  workerOnClosing: 'closing',
  workerOnCompleted: 'completed',
  workerOnDrained: 'drained',
  workerOnError: 'error',
  workerOnFailed: 'failed',
  workerOnIoredisClose: 'ioredis:close',
  workerOnPaused: 'paused',
  workerOnProgress: 'progress',
  workerOnReady: 'ready',
  workerOnResumed: 'resumed',
  workerOnStalled: 'stalled',
}

const queueEventNames: Record<QueueListenerName, string> = {
  queueOnActive: 'active',
  queueOnAdded: 'added',
  queueOnCleaned: 'cleaned',
  queueOnCompleted: 'completed',
  queueOnDebounced: 'debounced',
  queueOnDeduplicated: 'deduplicated',
  queueOnDelayed: 'delayed',
  queueOnDrained: 'drained',
  queueOnDuplicated: 'duplicated',
  queueOnError: 'error',
  queueOnFailed: 'failed',
  queueOnPaused: 'paused',
  queueOnProgress: 'progress',
  queueOnRemoved: 'removed',
  queueOnResumed: 'resumed',
  queueOnRetriesExhausted: 'retries-exhausted',
  queueOnStalled: 'stalled',
  queueOnWaiting: 'waiting',
  queueOnWaitingChildren: 'waiting-children',
}

const createWorkerParams = <K extends WorkerListenerName>() =>
  undefined as unknown as WorkerListenerParams[K]

const createQueueParams = <K extends QueueListenerName>() =>
  undefined as unknown as QueueListenerParams[K]

export const listenerMapping: ListenerMapping = {
  worker: {
    workerOnActive: {
      eventName: workerEventNames.workerOnActive,
      params: createWorkerParams<'workerOnActive'>(),
    },
    workerOnClosed: {
      eventName: workerEventNames.workerOnClosed,
      params: createWorkerParams<'workerOnClosed'>(),
    },
    workerOnClosing: {
      eventName: workerEventNames.workerOnClosing,
      params: createWorkerParams<'workerOnClosing'>(),
    },
    workerOnCompleted: {
      eventName: workerEventNames.workerOnCompleted,
      params: createWorkerParams<'workerOnCompleted'>(),
    },
    workerOnDrained: {
      eventName: workerEventNames.workerOnDrained,
      params: createWorkerParams<'workerOnDrained'>(),
    },
    workerOnError: {
      eventName: workerEventNames.workerOnError,
      params: createWorkerParams<'workerOnError'>(),
    },
    workerOnFailed: {
      eventName: workerEventNames.workerOnFailed,
      params: createWorkerParams<'workerOnFailed'>(),
    },
    workerOnIoredisClose: {
      eventName: workerEventNames.workerOnIoredisClose,
      params: createWorkerParams<'workerOnIoredisClose'>(),
    },
    workerOnPaused: {
      eventName: workerEventNames.workerOnPaused,
      params: createWorkerParams<'workerOnPaused'>(),
    },
    workerOnProgress: {
      eventName: workerEventNames.workerOnProgress,
      params: createWorkerParams<'workerOnProgress'>(),
    },
    workerOnReady: {
      eventName: workerEventNames.workerOnReady,
      params: createWorkerParams<'workerOnReady'>(),
    },
    workerOnResumed: {
      eventName: workerEventNames.workerOnResumed,
      params: createWorkerParams<'workerOnResumed'>(),
    },
    workerOnStalled: {
      eventName: workerEventNames.workerOnStalled,
      params: createWorkerParams<'workerOnStalled'>(),
    },
  },
  queue: {
    queueOnActive: {
      eventName: queueEventNames.queueOnActive,
      params: createQueueParams<'queueOnActive'>(),
    },
    queueOnAdded: {
      eventName: queueEventNames.queueOnAdded,
      params: createQueueParams<'queueOnAdded'>(),
    },
    queueOnCleaned: {
      eventName: queueEventNames.queueOnCleaned,
      params: createQueueParams<'queueOnCleaned'>(),
    },
    queueOnCompleted: {
      eventName: queueEventNames.queueOnCompleted,
      params: createQueueParams<'queueOnCompleted'>(),
    },
    queueOnDebounced: {
      eventName: queueEventNames.queueOnDebounced,
      params: createQueueParams<'queueOnDebounced'>(),
    },
    queueOnDeduplicated: {
      eventName: queueEventNames.queueOnDeduplicated,
      params: createQueueParams<'queueOnDeduplicated'>(),
    },
    queueOnDelayed: {
      eventName: queueEventNames.queueOnDelayed,
      params: createQueueParams<'queueOnDelayed'>(),
    },
    queueOnDrained: {
      eventName: queueEventNames.queueOnDrained,
      params: createQueueParams<'queueOnDrained'>(),
    },
    queueOnDuplicated: {
      eventName: queueEventNames.queueOnDuplicated,
      params: createQueueParams<'queueOnDuplicated'>(),
    },
    queueOnError: {
      eventName: queueEventNames.queueOnError,
      params: createQueueParams<'queueOnError'>(),
    },
    queueOnFailed: {
      eventName: queueEventNames.queueOnFailed,
      params: createQueueParams<'queueOnFailed'>(),
    },
    queueOnPaused: {
      eventName: queueEventNames.queueOnPaused,
      params: createQueueParams<'queueOnPaused'>(),
    },
    queueOnProgress: {
      eventName: queueEventNames.queueOnProgress,
      params: createQueueParams<'queueOnProgress'>(),
    },
    queueOnRemoved: {
      eventName: queueEventNames.queueOnRemoved,
      params: createQueueParams<'queueOnRemoved'>(),
    },
    queueOnResumed: {
      eventName: queueEventNames.queueOnResumed,
      params: createQueueParams<'queueOnResumed'>(),
    },
    queueOnRetriesExhausted: {
      eventName: queueEventNames.queueOnRetriesExhausted,
      params: createQueueParams<'queueOnRetriesExhausted'>(),
    },
    queueOnStalled: {
      eventName: queueEventNames.queueOnStalled,
      params: createQueueParams<'queueOnStalled'>(),
    },
    queueOnWaiting: {
      eventName: queueEventNames.queueOnWaiting,
      params: createQueueParams<'queueOnWaiting'>(),
    },
    queueOnWaitingChildren: {
      eventName: queueEventNames.queueOnWaitingChildren,
      params: createQueueParams<'queueOnWaitingChildren'>(),
    },
  },
}
