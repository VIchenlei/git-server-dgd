let config = require('./config/appconfig.js')

export default class Pusher {
  constructor() { // eslint-disable-line
  }

  /**
   * 处理采集服务器过来的 PUSH 消息
   * 广播给所有在线的监控客户端
   *
   * @method doPUSH
   *
   * @param  {[type]} socket    [description]
   * @param  {[type]} message      [description]
   *
   * @return {[type]}           [description]
   */
  static push (socket, message, sepcialID, specialMsg) {
    let userInfo = socket.handshake.session.user
    if (userInfo.name === config.COLLECTOR) {
      if (socket.auth) {
        if (message.cmd === 'pos_map' && sepcialID) {
          let data = JSON.parse(message.data)
          if (data.s.detail.length <= 0) {
            socket.to(config.SPECIAL).emit('PUSH', message)
          }
        }
        socket.to(config.MONITOR).emit('PUSH', message)
        if (message.cmd === 'up_mine' || message.cmd === 'event') {
          socket.to(config.STANDBY).emit('PUSH', message)
        }
        // 对于检查用户和只看白名单用户，告警信息需要做区分
        if (specialMsg) {
          socket.to(config.CHECKRANGE).emit('PUSH', specialMsg)
        } else {
          socket.to(config.CHECKRANGE).emit('PUSH', message)
        }
      } else {
        console.log('用户尚未登录，无法 PUSH 消息！')
      }
    } else {
      console.warn(`用户 ${userInfo.name} 没有 PUSH 权限！`)
    }
  }
}
