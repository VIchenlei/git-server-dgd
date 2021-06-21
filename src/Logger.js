import mysql from './MysqlWraper.js'
import log4js from 'log4js'

export default class Logger {
  constructor () { // eslint-disable-line
    log4js.configure({
      appenders: [{
        type: 'dataFile',
        filename: '',
        pattern: 'yyyy-MM-dd.log',
        alwaysIncludePattern: true
      }]
    })
    this.logger = log4js.getLogger()
  }

  /**
   * 将操作流水记录至 DB 中
   *
   * @method log2db
   *
   * @param  {[type]} socket     [description]
   * @param  {[type]} op_type_id [description]
   * @param  {[type]} detail     [description]
   *
   * @return {[type]}            [description]
   */
  static async log2db (socket, opTypeID, detail) {
    console.log('session-------', socket.handshake.session)
    let userInfo = socket && socket.handshake.session && socket.handshake.session.user
    let userID = userInfo && userInfo.name
      // let time = new Date()
      // new Date().toLocaleString().slice(0, 22).replace('T', ' ');
    let ip = socket && (socket.request.connection.remoteAddress || socket.request.connection.localAddress)
    // let ip = socket.request.connection.remoteAddress
      // execute update on db
    let sql = `insert into his_op_log (op_type_id, user_id, op_time, ip, detail) values(${opTypeID},'${userID}', NOW(), '${ip}', '"${detail}"')`

    try {
      let rows = await mysql.query(sql)  // eslint-disable-line
    } catch (err) {
      console.error(`记录日志至DB失败 : \n\t SQL : ${sql} \n\t ${err}`)
    }
  }

  log4 (desc, data) {
    this.logger && this.logger.info(`${desc}: ${data}`)
  }
}
