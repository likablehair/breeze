import {
  Job as BullmqJob,
  JobData,
  JobSchedulerJson,
  JobsOptions,
  Queue,
  RepeatOptions,
} from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'
import { EventListener } from '../managers/breeze.manager.js'
import { defineConfig } from './define_config.js'

type JobHandle<T> = T extends (payload: infer P) => any ? (undefined extends P ? any : P) : any

export const listeners = [
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

export type ListenersType = (typeof listeners)[number]
export const isListener = (x: any): x is ListenersType => listeners.includes(x)
export abstract class Breeze<TPayload = any, TResult = any> {
  declare instance?: BullmqJob<TPayload, TResult>
  declare logger?: LoggerService
  declare workerListener?: EventListener[]
  declare queueListener?: EventListener[]
  declare static app: ApplicationService
  declare static queues: any

  constructor() {
    if (!Breeze.app) {
      // Load app only once
      import('@adonisjs/core/services/app').then(({ default: app }) => {
        Breeze.app = app
        Breeze.app.container.make('breeze.queues').then((queues) => {
          Breeze.queues = queues
        })
      })
    }
  }
  abstract handle(payload: TPayload): Promise<TResult> | TResult

  static async dispatch<T extends Breeze>(
    this: new () => T,
    payload: JobHandle<T['handle']>,
    options: JobsOptions & { queueName?: string } = {}
  ) {
    const config = Breeze.app.config.get<ReturnType<typeof defineConfig>>('jobs', {}) as ReturnType<
      typeof defineConfig
    >
    const queueName = options.queueName || config.queues[0]
    const queue = Breeze.queues[queueName] as Queue

    if (!queue) {
      throw new Error(`Queue ${queueName} not found`)
    }

    const bullmqJob = await queue.add(queueName, payload, options)
    return bullmqJob
  }

  static async getRedisJob(queueKey: string, jobId: string) {
    const job = await Breeze.queues[queueKey].getJob(jobId)

    return job as BullmqJob
  }

  static async upsertJobScheduler<DataType, ResultType>(
    key: string,
    schedulerId: string,
    repeatOptions: RepeatOptions,
    jobTemplate?: {
      name?: string
      data?: DataType
      opts?: Omit<JobsOptions, 'jobId' | 'repeat' | 'delay'>
    }
  ): Promise<BullmqJob<DataType, ResultType>> {
    return Breeze.queues[key].upsertJobScheduler(schedulerId, repeatOptions, jobTemplate)
  }

  static async getJobScheduler(key: string, jobId: string): Promise<JobSchedulerJson<JobData>> {
    return Breeze.queues[key].getJobScheduler(jobId)
  }

  static async remove(key: string, jobId: string): Promise<void> {
    await Breeze.queues[key].remove(jobId)
  }

  static async removeJobScheduler(key: string, jobId: string): Promise<boolean> {
    return Breeze.queues[key].removeJobScheduler(jobId)
  }

  static async dispatchSync<T extends Breeze>(this: new () => T, payload: JobHandle<T['handle']>) {
    const { default: app } = await import('@adonisjs/core/services/app')

    const logger = await app.container.make('logger')
    const instance: Breeze = await app.container.make(this)

    instance.logger = logger

    await instance.handle(payload)
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
