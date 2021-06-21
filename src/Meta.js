import Logger from './Logger.js'
import mysql from './MysqlWraper.js'
import Pusher from './Pusher.js'
// import ReportFile from './reportFile.js'

let metaDefinition = require('./meta_definition.js')
let cardDefinition = require('./card_definition.js')

// let datatable = require('./meta_dat.js')

let config = require('./config/appconfig.js')

let eventTag = 'META'
// const DATABLENAME = 'dat_mdt_update'

export default class Meta {
  constructor(metaMsg) { // eslint-disable-line
    const {reportFile, workers, index, metaStore, sendCollectorList, power} = metaMsg || {}
    // let self = this
    this.reportFile = reportFile
    this.index = index
    this.metaStore = metaStore
    this.sendCollectorList = sendCollectorList
    this.workers = workers
    this.power = power
    // this.io = io
  }

  dispatch (socket, req) {
    if (!req) return
    this.socket = socket
    switch (req.cmd) {
      // case 'meta_definition':
      //   this.sendMetaDefinition(socket)
      //   break
      case 'card_definition':
        this.sendCardDefinition(socket)
        break
      case 'meta_data':
        let def = metaDefinition[req.data.name]
        this.sendMetaData(socket, def)
        break
        // case 'all_meta_data': // called from login
        //   this.sendAllMetaData(socket, req.user)
        //   break
      case 'clear_card':
        this.saveOpDB(socket, req)
        break
      // case 'update':
        // 这里虽然调用了 async function ，但因为无需等待其返回结果，所以不需要使用 await。
        // 这里相当于提交请求，具体的处理逻辑就都在处理函数中来完成。
        // this.updateDB(socket, req)
        // break
      case 'afresh_metadata':
        this.afreshSendMetadata(socket, req)
        break
      case 'pull_down_metadata':
        this.pullDownMetadata(socket, req)
        break
      default:
        console.warn(`未知的META请求：${req.cmd}`)
        break
    }
  }

  sendNoAuthRes (socket, req) {
    console.warn(`未授权的用户访问。\n\t ${JSON.stringify(req)} \n\t {JSON.stringify(socket)}`)
    let resMsg = {
      code: -1,
      msg: `您没有本次操作的权限，请联系系统管理员处理。`,
      cmd: req.cmd,
      data: {
        op: req.data.op,
        name: req.data.name,
        id: req.data.id
      }
    }
    this.sendMetaMessage(socket, resMsg)
  }

  sendUpdateDBErrorRes (req, err, socket) {
    console.error(`更新数据库失败 : \n\t SQL : ${req.data.sql} \n\t ${err}`)

    let resMsg = {
      code: -1,
      msg: `更新数据库失败，请联系系统管理员处理。`,
      cmd: req.cmd,
      data: {
        op: req.data.op,
        name: req.data.name,
        id: req.data.id
      }
    }
    this.sendMetaMessage(socket || this.socket, resMsg)
  }

  isAuthed (socket, req) {
    let ret = false

    console.log('req.cmd=============req.cmd', req.cmd)

    let roleID = socket.handshake.session.user.roleID
    console.log('roleID', roleID)
    if (!socket.auth) {
      let userID = req.username
      console.log('this.socket.auth:::::::', socket.handshake.session.user.name, userID)
      if (socket.handshake.session.user.name === userID) {
        socket.auth = true
      }
    }
    if (socket.auth) ret = true
    // if (socket.auth) {
    //   if (roleID === 1) { // 系统管理员
    //     ret = true
    //   } else if (roleID === 2) {
    //     if (req.data.sql && (req.data.sql.match(/materiel/ig) || req.data.sql.match(/special_vehicle/ig))) {
    //       ret = true
    //     }
    //   }
    // }

    // if (req.cmd === 'afresh_metadata' || req.cmd === 'pull_down_metadata') ret = true
    // console.log('ret--------------------------',ret)
    // if (!ret) {
    //   this.sendNoAuthRes(socket, req)
    // }

    return ret
  }

  // 将手动升井存入操作日志
  async saveOpDB (socket, req) {
    for (let i = 0, len = req.data.length; i < len; i++) {
      let data = req.data[i]
      Logger.log2db(socket, 2, `手动升井${data.cardid}`)
    }
  }

  async getSql (sql, req) {
    let msg = {}
    if (typeof sql === 'object') {
      let keys = Object.keys(sql)
      let promise = []
      keys.forEach(item => {
        promise.push(mysql.query(sql[item]))
      })
      let rows = Promise.all(promise).then((results) => {
        keys.forEach(key => {
          let index = keys.indexOf(key)
          msg[key] = results[index]
        })
      }).catch((err) => {
        console.log('err<<<<<<<<<<<<<', err)
      })
      return rows
    } else {
      let rows = await mysql.query(sql)
      return rows
    }
  }

  notifyMeta (req, io) {
    let self = this
    let notifyID = req.data.notify || req.data.id
    notifyID = req.data.name === 'rt_person_forbid_down_mine' && req.data.op === 'INSERT' ? `${req.insertId};${notifyID}` : notifyID
    let notifyName = /complex_data/.test(req.data.name) ? `${req.data.name.split('_')[2]}_extend` : req.data.name
    if (req.data.name === 'complex_data_staffs') notifyName = 'staff_extend'
    if (req.data.name === 'complex_data_vehicles') notifyName = 'vehicle_extend'
    notifyName = notifyName === 'area_reader' ? 'reader' : notifyName
    if (req.data.name === 'complex_data_staffs' || req.data.name === 'complex_data_vehicles') {
      let keyvalue = notifyID.split(',')
      keyvalue && keyvalue.forEach(item => {
        self.notifyMetaChanged(self.socket || io, notifyName, item, req.data.op, req.state)
      })
    } else {
      if (req.data.name === 'complex_data_staff' || req.data.name === 'complex_data_vehicle') {
        let cardID = req.data.notifyCard
        cardID && this.notifyMetaChanged(this.socket || io, 'card', cardID, 'INSERT')
      }
      this.notifyMetaChanged(this.socket || io, notifyName, notifyID, req.data.op, req.state)
    }

    if (req.data.name === 'alarm_mange') {
      this.sendPush(this.socket || io, req.data.id, req.data.notice)
    }
    Logger.log2db(this.socket || io, 2, `${req.data.op}.${req.data.name}-${req.data.id}`)
  }

  // 更新dat_setting表中setting_id = 47时，需要重新配置获取电源信息的时间周期
  checkIsSetting (key, rows) {
    if (key !== 'setting') return
    const row = rows.find(item => item.setting_id === 47)
    if (row) {
      const { value } = row
      this.power.dispatch({
        cmd: 'update_search_power_time',
        data: {value}
      })
    }
  }

  sendMessage (datas, room, io, req) {
    if (!io) return
    // socket = this.socket || socket

    let keys = Object.keys(datas)
    let broadcastRoom = 'STANDBY'
    for (let i = 0; i < keys.length; i++) {
      let key = keys[i]
      let rows = datas[key]
      let op = req ? req.data.op : 'update'
      let message = this.composeMetaMessage(metaDefinition[key], rows, op)
      // io.emit(eventTag, message)
      this.broadcastMessage(io, broadcastRoom, eventTag, message)
      // if (room === 'MONITOR') io.emit(eventTag, message)
      this.metaStore.saveMetaData(key, rows)
      this.checkIsSetting(key, rows)
    }

    req && this.notifyMeta(req, io)
  }

  sendPush (socket, key, typeID) {
    let msg = {}
    if (typeID === 24) {
      msg = {
        cmd: 'helpme_down',
        data: {
          id: key
        }
      }
    } else {
      msg = {
        cmd: 'event',
        data: {
          event_id: key,
          status: 100,
          ishand: true
        }
      }
    }
    Pusher.push(socket, msg)
  }

  async getMetaData (def, mdtcondition) {
    //  debugger
    let fstring = ''
    if (!def) return
    let names = def.fields.names
    let index = names.indexOf('geom')
    if (index >= 0) {
      // 需要修改 names，先复制一份
      names = [...def.fields.names]
      names[index] = 'ASTEXT(geom) as geom'
    }
    fstring = names.join(',')

    let condition = ''
    if (def.name === 'driver_arrange') {
      let today = new Date().format('yyyy-MM-dd')
      condition = `where driver_date = '${today}'`
    } else if (mdtcondition) {
      condition = mdtcondition
    }

    let sql = ''
    if (def.name === 'rt_person_forbid_down_mine') {
      sql = 'SELECT id, fdm.staff_id, name, dept_id, start_time, oper_time, oper_user FROM rt_person_forbid_down_mine fdm, dat_staff ds, dat_staff_extend dse WHERE fdm.staff_id = ds.staff_id AND fdm.staff_id = dse.staff_id AND STATUS = 1;'
    } else if (def.name === 'tt_inspection_route_planning') {
      sql = 'SELECT tir.staff_id, tir.status, ds.name, dse.dept_id, route_planning, reader_planning FROM tt_inspection_route_planning tir, dat_staff ds, dat_staff_extend dse WHERE tir.staff_id = ds.staff_id AND tir.staff_id = dse.staff_id;'
    } else {
      sql = `select ${fstring} from ${def.table} ${condition};`
    }
    // if (condition && condition.length > 0) {
    //   console.log('>>>>>> Query the driver_arrange data : ', sql)
    // }

    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.warn('查询 META DB 失败！ \n', err)
    }
    // console.log('rows are ' + rows)
    return rows // Here the 'rows' will converted to be  Promise.resolve(rows)
  }

  composeMetaMessage (def, rows, upMethod) {
    let message = null
    if (rows && rows.length > 0) {
      // JSON中，定义属性的方式有两种：一种是o.f，另外一种是o[f]。第一种f不能为变量，第二种f可以为变量.
      message = {
        code: 0,
        msg: 'OK',
        cmd: 'meta_data',
        upMethod: upMethod,
        data: {
          name: def && def.name,
          key: def.fields && def.fields.names[def.keyIndex],
          rows: rows
        }
      }
    } else {
      message = {
        code: 0,
        msg: '没有符合条件的记录。',
        cmd: 'meta_data',
        data: {
          name: def.name
        }
      }
    }

    return message
  }

  pullDownMetadata (socket, req) {
    let mdtdata = req.data.mdtdata
    let self = this
    if (mdtdata.length > 0) {
      this.checkTableTime(socket, mdtdata)
      if (req.data.objRange === 1) {
        let specialTables = ['staff', 'staff_extend', 'vehicle', 'vehicle_extend']
        specialTables.forEach(item => self.sendMetaData(socket, metaDefinition[item], null, 'DELETE'))
        // this.sendMetaData(socket, metaDefinition['staff'], null, 'DELETE')
      }
    } else {
      let promises = this.sendAllMetaData(socket) // 发送所有数据
      Promise.all(promises).then(() => {
        let msg = {
          code: 0,
          cmd: 'meta_data_all'
        }
        this.sendMetaMessage(socket, msg)
      }).catch((err) => {
        console.log(`>>>> Send all meta data FAILED.\n`, err)
      })
    }
  }

  checkoutTime (value, row, name, socket) {
    if (row) {
      row = row[0]
      let newDeleteTime = row.lastDelete && new Date(row.lastDelete).getTime() // 数据表中的最后删除时间
      let oldDeleteTime = value.lastDelete && new Date(value.lastDelete).getTime() // indexDB中最后删除时间

      let newLastUpdate = row.lastUpdate && new Date(row.lastUpdate).getTime() // 数据表中的最后更新时间
      let oldLastUpdate = value.lastUpdate && new Date(value.lastUpdate).getTime() // indexDB中的最后更新时间

      if (newDeleteTime > oldDeleteTime) { // 全量更新
        this.sendMetaData(socket, metaDefinition[name], null, 'DELETE')
      } else if (newLastUpdate > oldLastUpdate) { // 批量更新
        // let condition = `where lastUpdate >= '${value.lastUpdate}'`
        // this.sendMetaData(socket, metaDefinition[name], condition)
        this.sendMetaData(socket, metaDefinition[name], null, 'DELETE')
      }
    }
  }

  async checkTableTime (socket, rows) {
    let key = metaDefinition['mdt_update']
    let mdtrows = await this.getMetaData(key)
    for (let i = 0, len = rows.length; i < len; i++) {
      let value = rows[i] // 客户端数据
      let tablename = value.tableName
      let name = tablename.slice(4)
      let row = mdtrows.filter(item => item.tableName === tablename) // 数据表数据
      this.checkoutTime(value, row, name, socket)
    }
  }

  // 强制更新元数据
  afreshSendMetadata (socket, def) {
    let promises = this.sendDataTable(socket) // 发送meta_dat中的数据，基础表更新或删除
    Promise.all(promises).then(() => {
      console.log(`>>>> Send all meta data DONE.`)
    }).catch((err) => {
      console.log(`>>>> Send all meta data FAILED.\n`, err)
    })
  }

  /**
   * [sendMetaData description]
   *
   * @method sendMetaData
   *
   * @param  {[type]}     meta_def [description]
   *
   * @return {[type]}              [description]
   */
  async sendMetaData (socket, def, condition, upMethod) {
    // debugger
    // 这里需要等待 getMetaData 返回才能执行后续的逻辑，所以要使用 await
    let rows = await this.getMetaData(def, condition)
    let message = this.composeMetaMessage(def, rows, upMethod)
    if (socket === null) {
      return message
    }
    this.sendMetaMessage(socket, message)
    console.log(`meta: ${def.name}, count: ${rows ? rows.length : 0}`)
  }

  sendDataTable (socket) {
    let promises = []
    let sendTable = ['mdt_update', 'user', 'driver_arrange', 'rt_person_forbid_down_mine', 'tt_inspection_route_planning']
    for (let i = 0; i < sendTable.length; i++) {
      let key = sendTable[i]
      let p = this.sendMetaData(socket, metaDefinition[key])
      promises.push(p)
    }

    return promises
  }

  sendAllMetaData (socket) {
    let promises = []
    for (let key in metaDefinition) {
      if (key !== 'mdt_update') {
        let p = this.sendMetaData(socket, metaDefinition[key])
        promises.push(p)
      }
    }

    return promises
  }

  async sendAllMetaDataForMetaStoreOnServer () {
    let res = []
    for (let key in metaDefinition) {
      let msg = await this.sendMetaData(null, metaDefinition[key])
      // console.log(JSON.stringify(msg))
      res.push(msg)
    }
    // console.log(res)
    return res
  }

  sendMetaMessage (socket, message) {
    socket.emit(eventTag, message)
  }

  /**
   * Broadcast the meta data to ALL clients in the config.STANDBY room
   * @param {*} socket the client connection
   * @param {*} room  the room
   * @param {*} eventTag event tag
   * @param {*} message  message
   */
  broadcastMessage (socket, room, eventTag, message) {
    // socket.to(room)  == socket.broadcast.to(room)
    // socket.broadcast.to(room) 向 room 广播，不包括自己
    // socket.broadcast.emit('user connected')  向 socket 所在的 room 广播，不包括自己
    // this.socket.to(room).emit(etag, message)  // 无法发送给 socket 自己

    // io.emit('this', { will: 'be received by everyone'})
    // io.sockets.in(room) 向 room 广播，包括自己

    // 如何通过 socket 获得对应的 io 对象？
    // socket.server === io
    // console.log(' == broadcast == \n ', message)
    // socket.server.sockets.in(room).emit(eventTag, message)
    if (!socket || !socket.server) socket = this.socket
    // socket = socket || this.socket
    socket && socket.server && socket.server.sockets.emit(eventTag, message) // 向所有用户广播，包括自己
    // socket.broadcast.emit('PUSH', message) // 只广播给其他用户
  }

  async broadcastMetaData (socket, room, metaDef, upMethod) {
    // debugger
    // console.log(' == broadcastMetaData == ENTER')
    let rows = await this.getMetaData(metaDef)
    // console.log(' == broadcastMetaData == await done')

    let message = this.composeMetaMessage(metaDef, rows, upMethod)
    this.broadcastMessage(socket, room, eventTag, message)

    // console.log(' == broadcastMetaData == DONE')
  }

  sendMetaDefinition (socket) {
    if (socket === null) {
      // console.log('metaDefinition is ' + JSON.stringify(metaDefinition))
      return metaDefinition
    }

    let message = {
      cmd: 'meta_definition',
      data: metaDefinition,
      length: Object.keys(metaDefinition).length
    }
    console.log('>>>>>>>>>>>>' + message.length)
    this.sendMetaMessage(socket, message)
  }

  sendCardDefinition (socket) {
    let message = {
      cmd: 'card_definition',
      data: cardDefinition
    }

    this.sendMetaMessage(socket, message)
  }

  sendCollector (dataList, io) {
    console.log('----------------', dataList)
    let socket = this.socket || io
    for (let i = 0; i < dataList.length; i++) {
      let data = dataList[i]
      let message = {
        cmd: 'meta_data_changed',
        data: data
      }
      Logger.log2db(socket, 2, `"重新发送给采集:${data.name}-${data.id}"`)
      this.broadcastMessage(socket, config.COLLECTOR, 'CALL', message)
    }
  }

  /**
   * 通知 采集Server 元数据已更改
   *
   * eventTag = 'CALL'
   * message = {
   *     cmd: 'meta_data_changed',
   *     data : {
   *         name: meta_data_name,  // string: map, area, path, reader, card, staff, vehicle, etc.
   *         id: record_id,    // int: >= 0
   *         op_type:          // enumalate: INSERT | UPDATE | DELETE
   *     }
   * }
   *
   *  // TODO : if there are multi collectors, how to deal with them?
   *
   * @method notifyMetaChange
   *
   * @param  {[type]}       socket [description]
   * @param  {[type]}       mdName   [meta data name]
   * @param  {[type]}       recID     [record id]
   * @param  {[type]}       type   [opertaion type, INSERT / UPDATE / DELETE]
   *
   * @return {[type]}              [description]
   */
  notifyMetaChanged (socket, mdName, recID, type, state) {
    if (!mdName || config.NeedInformCollectorList.indexOf(mdName) < 0) {
      return
    }
    // console.log('recID--------------', recID)
    recID = recID.toString()
    if (mdName === 'card') {
      recID = recID.toString().padStart(13, 0)
    }
    if (!recID) return
    mdName = mdName === 'staff_extend_ck' ? 'staff_extend' : mdName
    state = state || 0
    type = Number(state) ? 'DELETE' : type
    // console.log('Notify meta changes. ')
    let message = {
      cmd: 'meta_data_changed',
      data: {
        name: mdName,
        id: recID,
        op_type: type, // INSERT / UPDATE / DELETE
        state: state // 是否做特殊通知0：非特殊；1特殊
      }
    }
    Logger.log2db(socket, 2, `"发送给采集:${mdName}-${recID}-${state}"`)
    console.log('message___________', message.data)
    this.sendCollectorList.storeList(message)

    console.log(!!socket)
    // do CALL
    this.broadcastMessage(socket, config.COLLECTOR, 'CALL', message)
  }
}
