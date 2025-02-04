import { Job as BullmqJob, JobsOptions, Queue } from 'bullmq'
import type { ApplicationService, LoggerService } from '@adonisjs/core/types'
import { EventListener } from '../managers/breeze.manager.js'

type JobHandle<T> = T extends (payload: infer P) => any ? (undefined extends P ? any : P) : any

export abstract class Job<TPayload = any, TResult = any> {
  declare instance: BullmqJob<TPayload, TResult>
  declare logger: LoggerService
  declare workerListener: EventListener[]
  declare queueLister: EventListener[]
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

  boot?: (queue: Queue<TPayload, TResult>) => void
  /**
   * This event is triggered when a job enters the 'active' state
   */
  workerOnActive?: (job: BullmqJob<TPayload, TResult>, result: TResult, prev: string) => void

  /**
   * This event is triggered when the worker is closed.
   */
  workerOnClosed?: () => void

  /**
   * This event is triggered when the worker is closing.
   */
  workerOnClosing?: (msg: string) => void

  /**
   * This event is triggered when a job has successfully completed.
   */
  workerOnCompleted?: (job: BullmqJob<TPayload, TResult>, result: TResult, prev: string) => void

  /**
   * This event is triggered when the queue has drained the waiting list.
   * Note that there could still be delayed jobs waiting their timers to expire and
   * this event will still be triggered as long as the waiting list has emptied.
   */
  workerOnDrained?: () => void

  /**
   * This event is triggered when an error is throw.
   */
  workerOnError?: (failedReason: Error) => void

  /**
   * This event is triggered when a job has thrown an exception.
   * Note: job parameter could be received as undefined when an stalled job reaches
   * the stalled limit and it is deleted by the removeOnFail option.
   */
  workerOnFailed?: (job: BullmqJob<TPayload, TResult>, error: Error, prev: string) => void

  /**
   * This event is triggered when ioredis is closed.
   */
  workerOnIoredisClose?: () => void

  /**
   * This event is triggered when the queue is paused.
   */
  workerOnPaused?: () => void

  /**
   * This event is triggered when a job updates it progress, i.e. the Job##updateProgress() method is called.
   * This is useful to notify progress or any other data from within a processor to the rest of the world.
   */
  workerOnProgress?: (job: BullmqJob<TPayload, TResult>, progress: number | object) => void

  /**
   * This event is triggered when blockingConnection is ready.
   */
  workerOnReady?: () => void

  /**
   * This event is triggered when the queue is resumed.
   */
  workerOnResumed?: () => void

  /**
   * This event is triggered when a job has stalled and has been moved back to the wait list.
   */
  workerOnStalled?: (jobId: string, prev: string) => void

  /**
   * Listen to 'active' event.
   *
   * This event is triggered when a job enters the 'active' state.
   */
  queueOnActive?: (
    args: {
      jobId: string
      prev?: string
    },
    id: string
  ) => void

  /**
   * Listen to 'added' event.
   *
   * This event is triggered when a job is created.
   */
  queueOnAdded?: (
    args: {
      jobId: string
      name: string
    },
    id: string
  ) => void

  /**
   * Listen to 'cleaned' event.
   *
   * This event is triggered when a cleaned method is triggered.
   */
  queueOnCleaned?: (
    args: {
      count: string
    },
    id: string
  ) => void

  /**
   * Listen to 'completed' event.
   *
   * This event is triggered when a job has successfully completed.
   */
  queueOnCompleted?: (
    args: {
      jobId: string
      returnvalue: string
      prev?: string
    },
    id: string
  ) => void

  /**
   * Listen to 'debounced' event.
   * @deprecated use deduplicated event
   *
   * This event is triggered when a job is debounced because debounceId still existed.
   */
  queueOnDebounced?: (
    args: {
      jobId: string
      debounceId: string
    },
    id: string
  ) => void

  /**
   * Listen to 'deduplicated' event.
   *
   * This event is triggered when a job is deduplicated because deduplicatedId still existed.
   */
  queueOnDeduplicated?: (
    args: {
      jobId: string
      deduplicationId: string
    },
    id: string
  ) => void

  /**
   * Listen to 'delayed' event.
   *
   * This event is triggered when a job is delayed.
   */
  queueOnDelayed?: (
    args: {
      jobId: string
      delay: number
    },
    id: string
  ) => void

  /**
   * Listen to 'drained' event.
   *
   * This event is triggered when the queue has drained the waiting list.
   * Note that there could still be delayed jobs waiting their timers to expire
   * and this event will still be triggered as long as the waiting list has emptied.
   */
  queueOnDrained?: (id: string) => void

  /**
   * Listen to 'duplicated' event.
   *
   * This event is triggered when a job is not created because it already exist.
   */
  queueOnDuplicated?: (
    args: {
      jobId: string
    },
    id: string
  ) => void

  /**
   * Listen to 'error' event.
   *
   * This event is triggered when an exception is thrown.
   */
  queueOnError?: (args: Error) => void

  /**
   * Listen to 'failed' event.
   *
   * This event is triggered when a job has thrown an exception.
   */
  queueOnFailed?: (
    args: {
      jobId: string
      failedReason: string
      prev?: string
    },
    id: string
  ) => void

  /**
   * Listen to 'paused' event.
   *
   * This event is triggered when a queue is paused.
   */
  queueOnPaused?: (args: {}, id: string) => void

  /**
   * Listen to 'progress' event.
   *
   * This event is triggered when a job updates it progress, i.e. the
   * Job##updateProgress() method is called. This is useful to notify
   * progress or any other data from within a processor to the rest of the
   * world.
   */
  queueOnProgress?: (
    args: {
      jobId: string
      data: number | object
    },
    id: string
  ) => void

  /**
   * Listen to 'removed' event.
   *
   * This event is triggered when a job has been manually
   * removed from the queue.
   */
  queueOnRemoved?: (
    args: {
      jobId: string
      prev: string
    },
    id: string
  ) => void

  /**
   * Listen to 'resumed' event.
   *
   * This event is triggered when a queue is resumed.
   */
  queueOnResumed?: (args: {}, id: string) => void

  /**
   * Listen to 'retries-exhausted' event.
   *
   * This event is triggered when a job has retried the maximum attempts.
   */
  queueOnRetriesExhausted?: (
    args: {
      jobId: string
      attemptsMade: string
    },
    id: string
  ) => void

  /**
   * Listen to 'stalled' event.
   *
   * This event is triggered when a job has been moved from 'active' back
   * to 'waiting'/'failed' due to the processor not being able to renew
   * the lock on the said job.
   */
  queueOnStalled?: (
    args: {
      jobId: string
    },
    id: string
  ) => void

  /**
   * Listen to 'waiting' event.
   *
   * This event is triggered when a job enters the 'waiting' state.
   */
  queueOnWaiting?: (
    args: {
      jobId: string
      prev?: string
    },
    id: string
  ) => void

  /**
   * Listen to 'waiting-children' event.
   *
   * This event is triggered when a job enters the 'waiting-children' state.
   */
  queueOnWaitingChildren?: (
    args: {
      jobId: string
    },
    id: string
  ) => void
}
