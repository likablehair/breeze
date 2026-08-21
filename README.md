<div align="center">
  <h1><b>Breeze</b></h1>

  <p>Job processing for AdonisJS v6 using <a href="https://bullmq.io/" target="_blank">BullMQ</a></p>
</div>

## Getting Started

This package is available in the npm registry.

```bash
pnpm install breeze
```

Next, configure the package by running the following command.

```bash
node ace configure breeze
```

## Creating Jobs

You can create a new job by running the following command.

```sh
node ace jobs:make SendEmail
```

## Listening for Jobs

First, you need to start the jobs listener, you can spawn multiple listeners to process jobs concurrently.

```sh
node ace jobs:listen  # default queue from env `REDIS_QUEUE`

node ace jobs:listen --queue=high
node ace jobs:listen --queue=high --queue=medium
node ace jobs:listen --queue=high,medium,low

node ace jobs:listen --queue=high --concurrency=3
```

## Dispatching Jobs

Dispatching jobs is as simple as importing the job class and calling

```ts
import SendEmail from 'path/to/jobs/send_email.js'

await SendEmail.dispatch({ ... })

await SendEmail.dispatch({ ... }, { // for more job options check https://docs.bullmq.io/
  attempts: 3,
  delay: 1000,
})
```

## Centralized Event Hooks

You can intercept any worker or queue event once and reuse the logic across every job by providing the optional `onWorkerEvents`, `onQueueEvents`, or `onGlobalEvents` callbacks inside `defineConfig`.

```ts
// config/jobs.ts
import { defineConfig } from '@likable-hair/breeze'

export default defineConfig({
  connection: { host: '127.0.0.1', port: 6379 },
  queue: 'default',
  queues: ['default'],
  options: {},
  onWorkerEvents: {
    workerOnFailed: ({ job, params }) => {
      const { bullJob, error } = params
      job.logger?.error({ jobId: bullJob?.id, error })
    },
  },
  onQueueEvents: {
    queueOnCompleted: ({ job, params }) => {
      const { payload, eventId } = params
      job.logger?.info({ queueEvent: payload, eventId })
    },
  },
  onGlobalEvents: ({ type, event, params, job }) => {
    job.logger?.debug({ scope: type, event: event.method, params })
  },
})
```

Each handler receives the instantiated job plus the raw arguments emitted by BullMQ, so you can log, alert, or execute custom side effects without redefining every `workerOn*` or `queueOn*` method.

If you need full typing for the event payloads in another application, import the `listenerMapping` (or the `WorkerListenerParamsMap` / `QueueListenerParamsMap`) from `@likable-hair/breeze`:

```ts
import { listenerMapping } from '@likable-hair/breeze'

type WorkerFailedArgs = typeof listenerMapping.worker.workerOnFailed.params

const handler = (methodParams: WorkerFailedArgs) => {
  const { job, error, prevStatus } = methodParams
  // fully typed tuple
}
```

## OpenTelemetry (optional)

Breeze supports OpenTelemetry instrumentation: install the optional [`bullmq-otel`](https://www.npmjs.com/package/bullmq-otel) package and pass a `telemetry` instance in `defineConfig` to trace job dispatching and processing.

```ts
// config/jobs.ts
import { BullMQOtel } from 'bullmq-otel'

export default defineConfig({
  // ...
  telemetry: new BullMQOtel('breeze'),
})
```

## Import Aliases (optional)

update your `package.json` and `tsconfig.json` to use import aliases

`package.json`

```json
{
  "imports": {
    "#jobs/*": "./app/jobs/*.js"
  }
}
```

`tsconfig.json`

```json
{
  "compilerOptions": {
    "paths": {
      "#jobs/*": ["./app/jobs/*.js"]
    }
  }
}
```

```ts
import SendEmail from '#jobs/send_email.js'

await SendEmail.dispatch({ ... })
```
