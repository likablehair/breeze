import app from '@adonisjs/core/services/app'
import { BreezeManager } from '../managers/breeze.manager.js'

let breeze: BreezeManager

await app.booted(async () => {
  breeze = await app.container.make('breeze')
})
export { breeze }
