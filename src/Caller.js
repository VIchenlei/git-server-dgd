import Logger from './Logger.js'

let config = require('./config/appconfig.js')

export default class Caller {
  constructor () { // eslint-disable-line
  }

  /**
   * 处理发往采集服务器的 CALL 消息，直接广播到采集服务器所在的 room
   * 支持向多个采集服务器广播，只需要把所有采集服务器 join 到同一个 room 即可
   *
   * @method doCall
   *
   * @param  {[type]} socket    [description]
   * @param  {[type]} event_tag [description]
   * @param  {[type]} message      [description]
   *
   * @return {[type]}           [description]
   */
  static call (socket, message) {
    if (socket.auth) {
      // io.sockets.in(config.COLLECTOR).emit('CALL', message)
      if (message.cmd === 'beatheart') {
        socket.emit('CALL', message)
      } else {
        socket.to(config.COLLECTOR).emit('CALL', message)
      }
      Logger.log2db(socket, 3, `CALL ${message.cmd}成功`)
    } else {
      console.warn('用户未登录！')
    }
  }
}
