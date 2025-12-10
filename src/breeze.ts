import {
  ConnectionOptions,
  FlowJob,
  FlowProducer,
  Job,
  JobSchedulerJson,
  JobsOptions,
  Queue,
  RepeatOptions,
} from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'
import { EventListener } from '../managers/breeze.manager.js'

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

  protected async workerOnActive(job: Breeze<TPayload, TResult>, prev: string): Promise<void> {
    console.log(`Worker active - Job ${job}, previous state: ${prev}`)
  }

  protected async workerOnClosed(): Promise<void> {
    console.log('Worker closed')
  }

  protected async workerOnClosing(msg: string): Promise<void> {
    console.log(`Worker closing: ${msg}`)
  }

  protected async workerOnCompleted(
    job: Breeze<TPayload, TResult>,
    result: TResult,
    prev: string
  ): Promise<void> {
    console.log(`Worker completed - Job ${job}, result: ${result}, previous state: ${prev}`)
  }

  protected async workerOnDrained(): Promise<void> {
    console.log('Worker queue drained')
  }

  protected async workerOnError(failedReason: Error): Promise<void> {
    console.log(`Worker error: ${failedReason.message}`)
  }

  protected async workerOnFailed(
    job: Breeze<TPayload, TResult> | undefined,
    error: Error,
    prev: string
  ): Promise<void> {
    console.log(`Worker failed - Job ${job}, error: ${error.message}, previous state: ${prev}`)
  }

  protected async workerOnIoredisClose(): Promise<void> {
    console.log('Worker ioredis closed')
  }

  protected async workerOnPaused(): Promise<void> {
    console.log('Worker paused')
  }

  protected async workerOnProgress(
    job: Breeze<TPayload, TResult>,
    progress: number | object
  ): Promise<void> {
    console.log(`Worker progress - Job ${job}, progress: ${JSON.stringify(progress)}`)
  }

  protected async workerOnReady(): Promise<void> {
    console.log('Worker ready')
  }

  protected async workerOnResumed(): Promise<void> {
    console.log('Worker resumed')
  }

  protected async workerOnStalled(jobId: string, prev: string): Promise<void> {
    console.log(`Worker stalled - Job ${jobId}, previous state: ${prev}`)
  }

  protected async queueOnActive(args: { jobId: string; prev?: string }, id: string): Promise<void> {
    console.log(`Queue active - Job ${args.jobId}, previous state: ${args.prev}, Event id: ${id}`)
  }

  protected async queueOnAdded(args: { jobId: string; name: string }, id: string): Promise<void> {
    console.log(`Queue added - Job ${args.jobId}, Name: ${args.name}, Event id: ${id}`)
  }

  protected async queueOnCleaned(args: { count: string }, id: string): Promise<void> {
    console.log(`Queue cleaned - Count: ${args.count}, Event id: ${id}`)
  }

  protected async queueOnCompleted(
    args: { jobId: string; returnvalue: string; prev?: string },
    id: string
  ): Promise<void> {
    console.log(
      `Queue completed - Job ${args.jobId}, Return value: ${args.returnvalue}, Previous state: ${args.prev}, Event id: ${id}`
    )
  }

  protected async queueOnDebounced(
    args: { jobId: string; debounceId: string },
    id: string
  ): Promise<void> {
    console.log(
      `Queue debounced - Job ${args.jobId}, Debounce ID: ${args.debounceId}, Event id: ${id}`
    )
  }

  protected async queueOnDeduplicated(
    args: { jobId: string; deduplicationId: string },
    id: string
  ): Promise<void> {
    console.log(
      `Queue deduplicated - Job ${args.jobId}, Deduplication ID: ${args.deduplicationId}, Event id: ${id}`
    )
  }

  protected async queueOnDelayed(
    args: { jobId: string; delay: number },
    id: string
  ): Promise<void> {
    console.log(`Queue delayed - Job ${args.jobId}, Delay: ${args.delay}, Event id: ${id}`)
  }

  protected async queueOnDrained(id: string): Promise<void> {
    console.log(`Queue drained - Event id: ${id}`)
  }

  protected async queueOnDuplicated(args: { jobId: string }, id: string): Promise<void> {
    console.log(`Queue duplicated - Job ${args.jobId}, Event id: ${id}`)
  }

  protected async queueOnError(args: Error): Promise<void> {
    console.log(`Queue error: ${args.message}`)
  }

  protected async queueOnFailed(
    args: { jobId: string; failedReason: string; prev?: string },
    id: string
  ): Promise<void> {
    console.log(
      `Queue failed - Job ${args.jobId}, Reason: ${args.failedReason}, Previous state: ${args.prev}, Event id: ${id}`
    )
  }

  protected async queueOnPaused(args: {}, id: string): Promise<void> {
    console.log(`Queue paused - Event id: ${id}, ${args}`)
  }

  protected async queueOnProgress(
    args: { jobId: string; data: number | object },
    id: string
  ): Promise<void> {
    console.log(
      `Queue progress - Job ${args.jobId}, Progress: ${JSON.stringify(args.data)}, Event id: ${id}`
    )
  }

  protected async queueOnRemoved(args: { jobId: string; prev: string }, id: string): Promise<void> {
    console.log(`Queue removed - Job ${args.jobId}, Previous state: ${args.prev}, Event id: ${id}`)
  }

  protected async queueOnResumed(args: {}, id: string): Promise<void> {
    console.log(`Queue resumed - Event id: ${id}, ${args}`)
  }

  protected async queueOnRetriesExhausted(
    args: { jobId: string; attemptsMade: string },
    id: string
  ): Promise<void> {
    console.log(
      `Queue retries exhausted - Job ${args.jobId}, Attempts made: ${args.attemptsMade}, Event id: ${id}`
    )
  }

  protected async queueOnStalled(args: { jobId: string }, id: string): Promise<void> {
    console.log(`Queue stalled - Job ${args.jobId}, Event id: ${id}`)
  }

  protected async queueOnWaiting(
    args: { jobId: string; prev?: string },
    id: string
  ): Promise<void> {
    console.log(`Queue waiting - Job ${args.jobId}, Previous state: ${args.prev}, Event id: ${id}`)
  }

  protected async queueOnWaitingChildren(args: { jobId: string }, id: string): Promise<void> {
    console.log(`Queue waiting for children - Job ${args.jobId}, Event id: ${id}`)
  }
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
