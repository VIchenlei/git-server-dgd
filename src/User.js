// logic : there is a new socket, there is a new user.
import Utils from './Utils.js'
import Logger from './Logger.js'

import Pusher from './Pusher.js'
import Caller from './Caller.js'
import XFiler from './XFile.js'
import Meta from './Meta.js'
// import Manager from './Manager.js'
import mysql from './MysqlWraper.js'
// import UserList from './UserList.js'
import Report from './Report.js'
import {getNetIP} from './reader_command_word.js'
// import metadata from './meta_definition.js'
// import TestPusher from './TestPusher.js'
// let ioClient = require('socket.io-client')
let config = require('./config/appconfig.js')

export default class User {
  constructor (userMsg) {
    const {io, userList, metaStore, socket, cardStore, sqlResultStore, callStore, reportFile, workers, callback, index, alarmStore, crypto, sendCollectorList, handleRssiLogWork, powerStore, manager, power, dgdServer} = userMsg
    if (socket) {
      this.userlist = userList
      this.socket = null
      this.session = null

      this.io = io
      this.bind(socket)

      this.name = null // user's name
      this.type = null // collector, monitor
      this.workers = workers
      this.callback = callback

      this.meta = new Meta({reportFile, workers, index, metaStore, sendCollectorList, power})
      this.powerStore = powerStore
      this.power = power

      this.filer = new XFiler(this.meta)
      this.metaStore = metaStore
      this.cardStore = cardStore
      this.callSotre = callStore
      this.reportFile = reportFile
      this.alarmStore = alarmStore
      this.sendCollectorList = sendCollectorList
      this.report = new Report(this, metaStore, sqlResultStore, socket)
      this.manager = manager
      this.index = index
      this.crypto = crypto
      this.handleRssiLogWork = handleRssiLogWork
      this.dgdServer = dgdServer
    } else {
      console.warn('无可用连接！')
    }
  }

  // 监控端登录时，PUSH 采集服务器的状态
  sendCollectorStatus () {
    let collector = this.userlist.get(config.COLLECTOR)

    let status = collector && collector.socket && collector.socket.auth ? 'online' : 'offline'
    let message = {
      cmd: 'collector_status',
      data: {
        status: status,
        time: new Date()
      }
    }

    this.socket.emit('PUSH', message)
  }

  // 采集服务器状态变化时，广播状态
  // 注意，采集server logout 的时候，不广播，直接通过 socket 的 disconncet 事件广播
  broadcastCollectorStatus (status) {
    let message = {
      cmd: 'collector_status',
      data: {
        status: status,
        time: new Date()
      }
    }

    this.io.to(config.MONITOR).emit('PUSH', message)
    this.io.to(config.CHECKRANGE).emit('PUSH', message)
  }

  sendThreeRate (data) {
    console.log('data----------------------', data)
  }

  registerEventHandler (socket) {
    if (!socket) {
      console.warn('注册事件处理器失败：没有可用的网络连接！')
      return
    }

    socket.on('disconnect', (req) => {
      console.log(`USER ${this.name} disconnected.`)

      if (this.name === config.COLLECTOR) {
        this.broadcastCollectorStatus('offline')
      }
    })

    socket.on('USER', (req, callback) => {
      console.log(`Got USER message : \n\t${JSON.stringify(req)}`)

      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }
      switch (req.cmd) {
        case 'login':
          this.login(req, callback)
          break
        case 'logout':
          this.logout(req, callback)
          break
        case 'standby':
          this.standby(req, callback)
          break
        case 'modify':
          this.modify(req, callback)
          break
        default:
          console.warn(`未知的 USER 请求：${req.cmd}`)
          break
      }
    })

    socket.on('PULLMSG', (req) => {
      let workersend = this.getWorker()
      let worker = workersend.worker
      req.workerIndex = workersend.index
      worker.send(req)
    })

    socket.on('FILE', (req) => {
      // console.log(`Got FILE message : \n\t${JSON.stringify(req)}`)

      req = Utils.toJson(req)
      this.filer.dispatch(socket, req)
      if (req.cmd === 'delete-pic') {
        this.meta.dispatch(this.socket, {
          cmd: 'update',
          data: {
            data: req.data.keyValue,
            name: req.data.name,
            op: 'UPDATE',
            sql: `UPDATE dat_${req.data.name} SET pic = '' WHERE ${req.data.name}_id = ${req.data.keyValue}`
          }
        })
      }
    })

    socket.on('META', (req, callback) => {
      // console.log(`Got META message : \n\t${JSON.stringify(req)}`)
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (!socket.auth) {
        let userID = req.username
        console.log('this.socket.auth:::::::', socket.handshake.session.user && socket.handshake.session.user.name, userID)
        if (socket.handshake.session.user && socket.handshake.session.user.name === userID) {
          socket.auth = true
        }
      }

      if (socket.auth) {
        this.callback.set(req.key, callback)
        req.socketID = socket.id
        let workersend = this.getWorker()
        let worker = workersend.worker
        req.workerIndex = workersend.index
        let isAuthed = this.meta.isAuthed(socket, req)
        if (!isAuthed && req.cmd === 'update') return
        req.cmd === 'update' ? worker.send(req) : this.meta.dispatch(socket, req)
        // this.meta.dispatch(this.socket, req)
      } else {
        this.notLogin('META')
      }
    })

    socket.on('FADE', (req, callback) => {
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (!socket.auth) {
        let userID = req.username
        console.log('this.socket.auth:::::::', socket.handshake.session.user.name, userID)
        if (socket.handshake.session.user.name === userID) {
          socket.auth = true
        }
      }

      if (socket.auth) {
        this.callback.set(req.key, callback)
        // let workersend = this.getWorker()
        // let worker = workersend.worker
        // req.workerIndex = workersend.index
        this.handleRssiLogWork.send(req)
      } else {
        this.notLogin('FADE')
      }
    })

    socket.on('REPT', (req, callback) => {
      // console.log(`Got META message : \n\t${JSON.stringify(req)}`)
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (!socket.auth) {
        let userID = req.username
        console.log('this.socket.auth:::::::', socket.handshake.session.user.name, userID)
        if (socket.handshake.session.user.name === userID) {
          socket.auth = true
        }
      }

      if (socket.auth) {
        this.callback.set(req.key, callback)
        let workersend = this.getWorker()
        let worker = workersend.worker
        req.workerIndex = workersend.index

        req.cmd === 'query' ? worker.send(req) : this.report.dispatch(this.socket, req, callback)
        // this.report.dispatch(this.socket, req, callback)
      } else {
        this.notLogin('REPT')
      }
    })

    socket.on('MANAGER', (req, callback) => {
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (!socket.auth) {
        let userID = req.username
        const user = this.userlist.list.get(userID)
        if (!socket.handshake.session.user) {
          const sessionUser = user.session.user
          socket.handshake.session.user = sessionUser
        }
        if (socket.handshake.session.user.name === userID) {
          socket.auth = true
        }
      }

      if (socket.auth) {
        let cmd = req.cmd
        if (cmd === 'get_net_ip') {
          let IPAdress = getNetIP()
          let req = {
            cmd: 'net_ip_resp',
            data: IPAdress
          }
          socket.emit('PUSH', req)
        } else if (cmd === 'get_power_msg') {
          let rows = this.powerStore.getLists(req.data)
          console.log('+++++++++++++++++', rows)
          callback(rows)
        } else {
          this.manager.dispatch(this.socket, req)
        }
      }
    })

    // 客户端发往采集 Server 的消息，Web Server 中转（发送到 room : config.COLLECTOR）
    socket.on('CALL', (req) => {
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (!socket.auth) {
        let userID = req.username
        console.log('this.socket.auth:::::::', socket.handshake.session.user.name, userID)
        if (socket.handshake.session.user.name === userID) {
          socket.auth = true
        }
      }

      if (socket.auth) {
        if (req.cmd === 'helpme_done' || req.cmd === 'req_all_data' || req.cmd === 'gas_done') { // 呼救处理消息通过 CALL 从客户端发送过来，通过 PUSH 广播给其他客户端
          socket.broadcast.emit('PUSH', req)

          socket.emit('PUSH', req)  // response to the sender
          if (req.cmd === 'helpme_done') {
            Caller.call(socket, req)
          }
          if (req.cmd === 'helpme_done' || req.cmd === 'req_all_data') {
            Logger.log2db(socket, 2, `${req.data.user_id}解除${req.data.id}的呼救`)
          }
          // socket.to(config.MONITOR).emit('PUSH', req)
        } else {
          if (req.cmd === 'clear_card') { // 客户端手动升井
            this.cardStore.setHandupdatescards(req)
            this.meta.dispatch(this.socket, req)
          }
          Caller.call(socket, req)
        }
      } else {
        this.notLogin('CALL')
      }
    })

    socket.on('ALARM', (req) => {
      // console.log(`Got ALARM message : \n\t${JSON.stringify(req)}`)

      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.', req)
        return
      }

      if (socket.auth) {
        if (req.cmd === 'alarm_done') { // 告警处理消息通过 ALARM 从客户端发送过来，通过 PUSH 广播给其他客户端
          this.alarmStore.storeHandDisplayEvents(req)
          // console.log(req)
          socket.to(config.CHECKRANGE).emit('PUSH', req)
          // socket.broadcast.emit('PUSH', req)
          socket.emit('PUSH', {
            cmd: 'alarm_done_resp',
            data: JSON.stringify('复位成功')
          })  // response to the sender
          // socket.to(config.MONITOR).emit('PUSH', req)
        } else if (req.cmd === 'recover_alarm') {
          let data = this.alarmStore.recoverAlarm(req)
          socket.to(config.CHECKRANGE).emit('PUSH', data)
          // socket.broadcast.emit('PUSH', data)
          socket.emit('PUSH', {
            cmd: 'recover_alarm_resp',
            data: JSON.stringify('恢复成功')
          })  // response to the sender
        } else {
          Caller.call(socket, req)
        }
      } else {
        this.notLogin('ALARM')
      }
    })

    // 采集 Server 发往客户端的消息，Web Server 中转（发送到 room : config.MONITOR）
    socket.on('PUSH', (req) => {
      // console.log(`Got PUSH message : \n\t${JSON.stringify(req)}`)
      let self = this
      req = Utils.toJson(req)
      if (!req) {
        console.warn('Invalid request.')
        return
      }

      let datas = Utils.toJson(req.data)
      let cmd = req.cmd

      let reqEventRangeCheck = null // 检查用户&只看白名单告警req

      this.storePosmapData(datas, cmd)

      if (socket.auth) {
        req.data = JSON.stringify(datas)
        let isSpecial = this.userlist.get('hxtx')

        if (cmd === 'event' || cmd === 'resp_all_data') {
          reqEventRangeCheck = JSON.parse(JSON.stringify(req))
          let events = datas
          if (cmd === 'resp_all_data') {
            let rows = datas && Array.isArray(datas) && datas.filter(item => {
              if (typeof item === 'string') item = JSON.parse(item)
              return item.cmd === 'event'
            })
            events = rows && rows[0]
            if (events) events = typeof events === 'string' ? JSON.parse(events).data : events.data
          }
          events = this.alarmStore.filterAlarm(socket, events, this.callSotre)
          if (cmd === 'event') reqEventRangeCheck.data = JSON.stringify(events)
          if (cmd === 'resp_all_data') {
            let respRows = []
            for (let i = 0; i < datas.length; i++) {
              let data = datas[i]
              data = typeof data === 'string' ? JSON.parse(data) : data
              if (data.cmd === 'event') {
                data.data = events
              }
              respRows.push(JSON.stringify(data))
            }
            datas = respRows
            reqEventRangeCheck.data = JSON.stringify(datas)
          }
        }

        Pusher.push(socket, req, isSpecial, reqEventRangeCheck)

        if (cmd === 'up_mine' || cmd === 'pos_map' || cmd === 'resp_all_data') {
          let msg = {
            cmd: 'nosignal_staffs',
            data: {
              handuping: this.cardStore.handUpdatescards,
              nosignal: this.cardStore.nosignalscars
            }
          }
          Pusher.push(socket, msg)
        } else if (cmd === 'meta_data_changed_recv') {
          let data = JSON.parse(req.data)
          console.log('采集回复消息：', data)
          this.sendCollectorList.deleteList(data)
        }

        if (cmd === 'pos_map') {
          this.callBreatheart() // 向采集发送心跳
        }
      } else {
        this.notLogin('PUSH')
      }
    })

    socket.on('TIME', (req) => {
      let now = new Date()
      let hour = now.getHours() < 10 ? '0' + now.getHours() : now.getHours()
      let minutes = now.getMinutes() < 10 ? '0' + now.getMinutes() : now.getMinutes()
      let seconds = now.getSeconds() < 10 ? '0' + now.getSeconds() : now.getSeconds()
      let servertime = hour + ':' + minutes + ':' + seconds
      let msg = {
        cmd: 'time',
        data: {
          now: servertime
        }
      }
      this.socket.emit('PUSH', msg)
    })

    // 采集 Server 发往客户端的消息，Web Server 中转（发送到 room : config.MONITOR）
    socket.on('HELP', (req) => {
      req = Utils.toJson(req)
      for (let i = 0; i < req.length; i++) {
        let users = this.userlist.getList()
        for (var [key, value] of users) {
          let name = key
          let type = value.type
          let values = Number(req[i].event_id) + ',' + '"' + name + '"' + ',' + '"' + type + '"'
          let sql = `INSERT into his_help_event_user VALUES (${values})`
          try {
            mysql.query(sql)
          } catch (err) {
            console.error(`更新DB失败。 \n\t ${err}`)
          }
          Pusher.push(socket, req)
        }
      }
    })
  }

  getWorker () {
    let index = this.index % this.workers.length
    let worker = this.workers[index]
    this.index ++
    return {worker: worker, index: index}
  }

  storePosmapData (datas, cmd) {
    let data = null
    if (cmd === 'up_mine') {
      this.cardStore.deleteNosignalCards(datas)
    } else if (cmd === 'pos_map') {
      data = datas
      this.dgdServer.sendData(datas)
      this.cardStore.cardMove(datas)
    } else if (cmd === 'resp_all_data') {
      for (let i = 0; i < datas.length; i++) {
        let row = datas[i]
        if (row.cmd === 'pos_map') {
          data = row.data
          this.cardStore.cardMove(data)
        }
      }
    }
  }

  notLogin (cmd) {
    let res = {
      code: -100,  // -100, 表示尚未登录
      msg: `${cmd} : 用户 ${this.name} 尚未登录！`,
      data: {
        username: this.name
      }
    }

    // info the client
    this.socket.emit('USER', res)
    console.warn(res.msg)
  }

  doCallBack (fn, msg, remark) {
    if (fn && typeof fn === 'function') {
      fn(msg)
      console.debug(`${remark} : callback is done. callback=${fn}, msg=${msg}`)
    } else {
      console.warn(`${remark} : callback is invalid. callback=${fn}, msg=${msg}`)
    }
  }

  judgeSpecialLogin () {
    let now = new Date().getTime()
    let hxtxLoginTime = this.userlist.loginTimeList.get('hxtx') || 0
    let timeInterval = 5 * 60 * 1000 + hxtxLoginTime
    if (now > timeInterval) {
      this.userlist.loginTimeList.set('hxtx', now)
      return true
    }
    return false
  }

  /**
   * login processor
   *
   * @method login
   *
   * @param  {[type]}   req      [login message]
   * @param  {Function} callback [callback the client's processor]
   *
   */
  async login (req, callback) {
    let resMsg = null

    let userName = req.data.user_name
    let userPass = req.data.user_pass
    let usermd5 = req.data.md5
    let isAllowLogin = true
    if (userName === 'hxtx') { // 禁止hxtx用户频繁登录web
      isAllowLogin = this.judgeSpecialLogin()
    }
    if (!isAllowLogin) return
    if (usermd5) { // 判断cookie是否有效
      let result = this.crypto.aesDecrypt(usermd5)
      if (result) {
        let resultDecrypt = result.split('+')
        userName = resultDecrypt[0]
        userPass = resultDecrypt[1]
      }
    }
    // userPass = Utils.sha1(userPass)
    let sql = `select du.user_id, du.dept_id, du.role_id, du.access_id, du.obj_range, du.name, du.is_check,dr.menus from dat_user du left join dat_role dr on dr.role_id = du.role_id where user_id="${userName}" and pwd="${userPass}"`
    // console.log('sql', sql)
    let rows = null

    try {
      console.log('Going to do login-check on DB， please wait... ', userName)
      rows = await mysql.query(sql)
      // console.log('rows-----------------', rows)
      console.log('Login-check on DB done. ', userName)
      // let socket = ioClient.connect('http://127.0.0.1:3000')
      // socket.emit('new user login', userName)
    } catch (err) {
      console.error(`查询DB失败。 \n\t ${err}`)
      resMsg = {
        code: -1,
        msg: '服务器错误，请联系系统管理员！',
        data: {
          name: userName
        }
      }
      this.doCallBack(callback, resMsg, 'User.login')

      return
    }

    if (rows && rows.length > 0) { // loged in
      let crypto = this.crypto.aesEncrypt(userName, userPass)
      this.socket.auth = true
      this.session.user = {
        name: userName,
        deptID: rows[0].dept_id,
        roleID: rows[0].role_id,
        accessID: rows[0].access_id,
        objRange: rows[0].obj_range,
        userCName: rows[0].name,
        ip: this.socket.request.connection.remoteAddress || this.socket.request.connection.localAddress || this.socket.handshake.address
      }
      this.session.save() // save the session info to sessionStore
      this.initContext(userName, req, rows[0].obj_range, rows[0].is_check)
      Logger.log2db(this.socket, 0, '登录成功！')
      let menuSql = `select menu_id from dat_menu`
      let menusData = await mysql.query(menuSql)
      let menusObj = this.getMenus(rows[0].menus, menusData)
      let menus = menusObj && menusObj.menus
      let transerMenus = menusObj && menusObj.transerMenus
      resMsg = {
        code: 0,
        msg: '',
        data: {
          name: userName,
          roleID: rows[0].role_id,
          deptID: rows[0].dept_id,
          accessID: rows[0].access_id,
          objRange: rows[0].obj_range,
          userCName: rows[0].name,
          sid: this.socket.handshake.sessionID,
          ip: this.socket.request.connection.remoteAddress || this.socket.request.connection.localAddress || this.socket.handshake.address,
          md5: crypto,
          isCheck: rows[0].is_check,
          menus: menus,
          transerMenus: transerMenus
        }
      }
      this.metaStore.isCheck = rows[0].is_check
      // info all connections
      if (this.name === config.COLLECTOR) {
        this.broadcastCollectorStatus('online')
      } else if (userName !== 'hxtx') {
        // 只发给刚登录的用户
        this.sendCollectorStatus()
      }
    } else {
      console.log('ERROR: 用户名或密码错误: ' + this.name)
      resMsg = {
        code: -1,
        msg: '用户名或密码错误，请确认后再试。'
      }
    }

    this.doCallBack(callback, resMsg, 'User.login')
  }

  /**
   * 退出登录态
   * socket 退出对应的房间。
   * 注意，这时 client / browser 与 server 之间的 socket 并没有断开。
   *
   * @method doLogout
   *
   * @param  {[type]} socket    [description]
   * @param  {[type]} event_tag [description]
   * @param  {[type]} req       [description]
   *
   * @return {[type]}           [description]
   */
  logout (req, callback) {
    // let resMsg = null

    let userInfo = this.session.user
    console.log('userInfo-------', userInfo)
    if (userInfo) {
      let userName = userInfo.name
      Logger.log2db(this.socket, 1, '退出成功！')

      this.clearContext(userName)

      delete this.socket.handshake.session.user
      this.socket.auth = false
      if (this.name === config.COLLECTOR) {
        this.broadcastCollectorStatus('offline')
      }

      this.name = null
    }
  }

  standby (req, callback) {
    let userName = req.data.username
    let resMsg = null
    if (userName.toLowerCase() === 'hxtx') return
    if (req.data.op === 'enter') {
      this.socket.leave(config.MONITOR)
      console.log(`>> User ${userName} leave ${config.MONITOR}`)
      this.socket.join(config.STANDBY)
      console.log(`>> User ${userName} enter ${config.STANDBY}`)

      resMsg = {
        code: 0,
        op: req.data.op
      }
    } else if (req.data.op === 'leave') {
      this.socket.leave(config.STANDBY)
      console.log(`>> User ${userName} leave ${config.STANDBY}`)
      this.socket.join(config.MONITOR)
      console.log(`>> User ${userName} enter ${config.MONITOR}`)

      resMsg = {
        code: 0,
        op: req.data.op
      }
    } else {
      resMsg = {
        code: -1,
        op: req.data.op
      }
      console.warn('UNKNOWN standby command : ', req.cmd)
    }

    this.doCallBack(callback, resMsg, 'User.standby')
  }

  async modify (req, callback) {
    let resMsg = null

    let username = req.data.username
    let oldpwd = req.data.oldpwd
    // oldpwd = Utils.sha1(oldpwd)

    let newpwd = req.data.newpwd
    // newpwd = Utils.sha1(newpwd)

    let sql = `select user_id from dat_user where user_id="${username}" and pwd="${oldpwd}"`

    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.error(`查询DB失败。 \n\t ${err}`)
      resMsg = {
        code: -1,
        msg: '服务器错误，请联系系统管理员！',
        data: {
          name: username
        }
      }
      this.doCallBack(callback, resMsg, 'User.modify')
      return
    }

    // console.log(`Modify password : \n sql : ${sql} \n rows : `, rows)

    if (rows && rows.length > 0) { // loged in
      sql = `update dat_user set pwd="${newpwd}" where user_id="${username}"`
      // execute update on db
      rows = null
      try {
        rows = await mysql.query(sql)
      } catch (err) {
        console.error(`更新数据库失败 : \n\t SQL : ${sql} \n\t ${err}`)

        resMsg = {
          code: -1,
          msg: '更新数据库失败',
          cmd: req.cmd
        }
        this.doCallBack(callback, resMsg, 'User.modify')

        return
      }

      // 更新 DB 成功
      resMsg = {
        code: 0,
        msg: ''
      }
      Logger.log2db(this.socket, 3, '修改密码成功！')
    } else {
      resMsg = {
        code: -1,
        msg: '密码错误，请确认后再试。'
      }
    }

    this.doCallBack(callback, resMsg, 'User.modify')
  }

  bind (socket) {
    this.socket = socket
    this.session = socket.handshake.session

    this.registerEventHandler(this.socket)
  }

  joinContext (userName, type) {
    this.type = config[type]
    this.socket.join(config[type])
    console.log(`>> User ${userName} enter ${config[type]}`)
    Logger.log2db(this.socket, 3, `User ${userName} enter ${config[type]}`)
  }

  initContext (userName, req, objRange, isCheck) {
    this.name = userName

    if (userName === config.COLLECTOR) {
      this.joinContext(userName, 'COLLECTOR')
    } else if (userName.toLowerCase() === 'hxtx') {
      this.joinContext(userName, 'SPECIAL')
    } else {
      let type = objRange === 1 || isCheck === 1 ? 'CHECKRANGE' : 'MONITOR'
      this.joinContext(userName, type)

      // if a client reconnect, it need to update the client's local meta
      // this.meta.dispatch(this.socket, { cmd: 'meta_definition' })
      this.meta.sendMetaDefinition(this.socket)

      // this.meta.dispatch(this.socket, { cmd: 'card_definition' })  // move to client

      // send all meta data and all init card data one by one
      // let promises = this.meta.sendAllMetaData(this.socket) // 发送meta_definition中的所有数据

      let promises = this.meta.sendDataTable(this.socket) // 发送meta_dat中的数据，基础表更新或删除，每次登陆时，都先发送到客户端
      Promise.all(promises).then(() => {
        console.log(`>>>> Send all meta data DONE for user ${this.name}.`)
        this.requestAllCardPositions()
      }).catch((err) => {
        console.log(`>>>> Send all meta data FAILED for user ${this.name}.\n`, err)
      })
    }

    this.userlist.add(this) // save socket for later usage
  }

  clearContext (userName) {
    // leave room
    if (userName === config.COLLECTOR) {
      this.socket.leave(config.COLLECTOR)
      console.log(`<< User ${userName} left ${config.COLLECTOR}`)
      Logger.log2db(this.socket, 3, `User ${userName} enter ${config.COLLECTOR}`)
    } else {
      this.socket.leave(config.MONITOR)
      console.log(`<< User ${userName} left ${config.MONITOR}`)
    }

    this.userlist.remove(this)
  }

  callBreatheart () {
    let collector = this.userlist.get(config.COLLECTOR)

    let collectorSocket = collector ? collector.socket : null
    if (!collectorSocket) {
      console.warn('当前没有可用的采集服务器，无法获取现场实时动态。')
      return
    }
    let reqcall = {
      cmd: 'beatheart',
      data: {}
    }
    collectorSocket.emit('CALL', reqcall)
    // Logger.log2db(collectorSocket, 3, `CALL ${reqcall.cmd}成功`)
  }

  /**
   * 用户登录后，获取当前井下所有卡的位置信息
   *
   * @method requestAllCardPositions
   *
   * @param  {[type]}                socket [description]
   *
   * @return {[type]}                       [description]
   */
  requestAllCardPositions () {
    console.log(`Going to send all cards' init position for user ${this.name}.`)
    let collector = this.userlist.get(config.COLLECTOR)

    let collectorSocket = collector ? collector.socket : null
    if (!collectorSocket) {
      console.warn('当前没有可用的采集服务器，无法获取现场实时动态。')
      return
    }

    let message = {
      cmd: 'req_all_data',
      data: '0',
      version: '1.0.0.2'
    }
    let self = this
    // console.log('Going to request all data by user ', this.name, message)

    collectorSocket.emit('CALL', message, (data) => {
      console.log('Got collector\'s response on req_all_data: ', data)
      Logger.log2db(collectorSocket, 3, 'CALL req_all_data 成功')
      if (data) {
        let res = Utils.toJson(data)
        Pusher.push(self.socket, res)
        // self.socket.emit('PUSH', res) // 将应答结果以 PUSH 的方式发送给客户端（socket） .
      }
    })
  }

  // 主进程获取子进程查询数据库的数据，发送给client
  sendSqlResult (msg, key) {
    console.log('----------------------主进程接收数据成功')
    if (this.callback) {
      let callback = this.callback.get(key)
      msg['key'] = key
      typeof callback === 'function' && callback(msg)
      this.callback.delete(key)
    }
  }

  // 主进程接收子进程META UPDATE，发送给client
  sendMetaResult (msg) {
    let socketID = msg.req.socketID
    if (this.socket.id === socketID) {
      console.log('+++++++++++++++++++++主进程接收META数据成功')
      let broadcastDatas = msg.broadcastdatas
      let room = msg.room
      this.meta.sendMetaMessage(this.socket, msg.resMsg)
      this.meta.sendMessage(broadcastDatas, room, this.socket, msg.req)
    }
    // if (msg.err) {
    //   this.meta.sendUpdateDBErrorRes(msg.req, this.socket)
    // } else {
    //   this.meta.sendMessage(msg.req, msg.resMsg, this.socket)
    // }
  }

  getMenus (menus, menusData) {
    let transerMenus = [
      { menuID: 'MO', pageName: 'sp_monitor' },
      { menuID: 'HS', pageName: 'sp_history' },
      { menuID: 'RP', pageName: 'sp_report' },
      { menuID: 'MA', pageName: 'sp_manage' },
      { menuID: 'CF', pageName: 'sp_config' }
    ]
    if (!menus || (menus === '0')) {
      menus = menusData && menusData.map(item => item.menu_id)
    } else {
      menus = menus.split(';')
    }
    transerMenus = transerMenus.filter(list => {
      let menuID = list.menuID
      let result = menus.find(item => {
        return item.includes(menuID)
      })
      return !!result
    })
    return {
      menus: menus,
      transerMenus: transerMenus
    }
  }
}
