import type { ApplicationService } from '@adonisjs/core/types'
import { fsReadAll, importDefault, slash } from '@poppinss/utils'
import { fileURLToPath } from 'node:url'
import { basename, extname, relative } from 'node:path'
import { Breeze, defineConfig } from '../index.js'
import { RouteGroup } from '@adonisjs/core/http'
import { Queue as BullmqQueue } from 'bullmq'
import { BreezeManager } from '../managers/breeze.manager.js'

const JS_MODULES = ['.js', '.cjs', '.mjs']

export default class JobsProvider {
  constructor(protected app: ApplicationService) {}

  async boot() {
    const jobs: Record<string, typeof Breeze> = {}
    const jobsFiles = await fsReadAll(this.app.relativePath('app/Jobs'), {
      pathType: 'url',
      ignoreMissingRoot: true,
      filter: (filePath: string) => {
        const ext = extname(filePath)

        if (basename(filePath).startsWith('_')) {
          return false
        }

        if (JS_MODULES.includes(ext)) {
          return true
        }

        if (ext === '.ts' && !filePath.endsWith('.d.ts')) {
          return true
        }

        return false
      },
    })

    for (let file of jobsFiles) {
      if (file.endsWith('.ts')) {
        file = file.replace(/\.ts$/, '.js')
      }

      const relativeFileName = slash(
        relative(this.app.relativePath('app/Jobs'), fileURLToPath(file))
      )

      const jobClass = (await importDefault(() => import(file), relativeFileName)) as typeof Breeze
      jobClass.app = this.app
      jobs[jobClass.name] = jobClass
    }

    const config = this.app.config.get<ReturnType<typeof defineConfig>>('jobs', {})

    const queues = config.queues.reduce(
      (acc, name) => {
        const queue = new BullmqQueue(name, {
          connection: config.connection,
          defaultJobOptions: config.options,
        })

        acc[name] = queue

        return acc
      },
      {} as Record<string, BullmqQueue>
    )

    this.app.terminating(async () => {
      for (const queueName in queues) {
        await queues[queueName].close()
      }
    })

    this.app.container.singleton('breeze', () => new BreezeManager(this.app))
    this.app.container.singleton('breeze.list', () => jobs)
    this.app.container.singleton('breeze.queues', () => queues)
    Breeze.queues = queues
  }
}

declare module '@adonisjs/core/http' {
  interface Router {
    jobs: (pattern?: string) => RouteGroup
  }
}

declare module '@adonisjs/core/types' {
  export interface ContainerBindings {
    'breeze.list': Record<string, typeof Breeze>
    'breeze.queues': Record<string, BullmqQueue>
    'breeze': BreezeManager
  }
}
