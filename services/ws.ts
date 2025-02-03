import { Server } from 'socket.io'
import server from '@adonisjs/core/services/server'
import redis from '@adonisjs/redis/services/main'

export type EventName = 'worker:active'
class Ws {
  io: Server
  private booted = false

  boot() {
    if (this.booted) {
      return
    }

    this.booted = true
    this.io = new Server(server.getNodeServer(), {
      cors: {
        origin: '*',
      },
      transports: ['websocket'],
    })

    redis.subscribe('socket:emit', (data) => {
      let parsedData = JSON.parse(data)
      this.io.emit(parsedData.event, parsedData.data)
    })
  }

  emit(event: EventName, data: any) {
    redis.publish(
      `socket:emit`,
      JSON.stringify({
        event: event,
        data: data,
      })
    )
  }
}

export default new Ws()
