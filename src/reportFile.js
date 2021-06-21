import mysql from './MysqlWraper.js'
import Manager from './Manager.js'
import { EWOULDBLOCK } from 'constants'
let fs = require('fs')
let metaDefinition = require('./meta_definition.js')

const DATABLENAME = 'dat_mdt_update'

export default class ReportFile {
  constructor () {
    this.reportTime = null
    this.timer = null
    this.scards = 0
    this.manager = new Manager()
    this.registerEventHandler()
  }

  registerEventHandler () {
    let self = this
    process.on('message', (msg, handle, cb) => {
      let cmd = msg.cmd || msg.req.cmd
      let data = msg.data
      if (cmd === 'UPDATE') {
        self.replaceValue(data)
      } else if (cmd === 'pos_map') {
        self.scards = data
      } else if (cmd === 'query') {
        // console.log('----------------------子进程接收REPT成功')
        self.doQueryData(msg)
      } else if (cmd === 'update') {
        // console.log('+++++++++++++++++++++++子进程接收META成功')
        self.updateDB(msg)
      } else if (cmd === 'pull-msg') {
        // console.log('+++++++++++++++++++++++子进程接收PULLMSG成功')
        self.pullImportMsg(msg)
      }
    })
  }

  async getTimeInterval () {
    let sql = `select value from dat_setting where setting_id = 15`
    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.warn('查询 REPT DB 失败！ \n\t', err)
    }
    return rows[0] && rows[0].value
  }

  async replaceValue (sql) {
    if (/value=/.test(sql)) {
      this.reportTime = await this.getTimeInterval()
      this.startInterval()
    }
  }

  writeReportFile () {
    let scards = this.scards
    let data = `当前井下人数：${scards}人`
    fs.writeFile('../reportfile.txt', data, 'utf8', function (err) {
      if (err) console.log('-================', '文件写入失败')
      // if (err) throw (err)
      // console.log('-================', '文件写入成功')
    })
  }

  startInterval () {
    let self = this
    if (this.timer) {
      clearInterval(this.timer)
    }
    let time = this.reportTime * 1000
    this.timer = setInterval(() => {
      self.writeReportFile()
    }, time)
  }

  /**
   * 获取结果集的记录数，用于分页时计算页数。
   */
  async getTotal (sql) {
    let count = 0

    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.warn('查询 REPT 记录数失败！ \n\t', err)
    }

    if (rows) {
      let len = rows.length

      if (len === 1) { // sql NOT include 'group by'
        count = rows[0].total
      } else if (len > 1) {  // sql include 'group by'
        count = len
      } else {
        count = 0
      }
    }

    return count
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
        if (req.data.name === 'three-credentials' || req.data.name === 'efficiency_overview') {
          msg['worktime'] = msg['worktime'] || true
        }
        msg['name'] = req.data.name
        return msg
      }).catch((err) => {
        console.log('err<<<<<<<<<<<<<', err)
      })
      return rows
    } else {
      let rows = await mysql.query(sql)
      return rows
    }
  }

  async doQueryData (req) {
    let message = null

    // init total
    let total = 0
    if (req.data.pageIndex === 0 && req.data.total < 0) {
      total = await this.getTotal(req.data.countSql)
    } else {
      total = req.data.total
    }

    let sql = req.data.sql  // 默认不分页
    if (req.data.pageSize > 0) {  // 如果 pageSize 值有效，则需要分页
      let start = req.data.pageIndex * req.data.pageSize
      let count = req.data.pageSize

      sql = sql && sql.trim()
      if (sql.endsWith(';')) {  // 去掉末尾的 ';'
        sql = sql.slice(0, sql.length - 1)
      }
      sql = `${sql} limit ${start},${count};`
    }

    let name = req.data.name
    let rows = null // eslint-disable-line
    if (sql) {
      try {
        rows = await this.getSql(sql, req)
      } catch (err) {
        console.warn('查询 REPT DB 失败！ \n\t', err)
        message = {
          code: -1,
          msg: '查询失败！'
        }
        process.send({
          cmd: 'REPTREQUEST',
          message: message,
          key: req.key,
          workerIndex: req.workerIndex,
          username: req.username
        })
      }
    }
    message = {
      code: 0,
      msg: 'OK',
      data: rows,
      total: total,
      pageIndex: req.data.pageIndex
    }
    if (req.data.name === 'TrackList' || req.data.name === 'TrackData') {
      this.dealTrackListData(message)
    }
    process.send({
      cmd: 'REPTREQUEST',
      message: message,
      key: req.key,
      socketID: req.socketID,
      workerIndex: req.workerIndex,
      username: req.username
    })
  }

  dealData (item, msg, datas, name, nextpoint) {
    let beginTime = item.begin_time
    let endTime = item.last_time
    beginTime = new Date(beginTime).getTime()
    endTime = new Date(endTime).getTime()
    let speed = item.speed
    msg['speed'] = speed * 2 * 3.6
    let coordinate = item.begin_pt
    coordinate = coordinate.split(',')
    let x = Number(coordinate[0])
    let y = Number(coordinate[1])
    msg['x'] = Number(x.toFixed(2))
    msg['y'] = Number(y.toFixed(2))
    msg['cur_time'] = new Date(beginTime).format('yyyy-MM-dd hh:mm:ss')
    msg['end_time'] = new Date(endTime).format('yyyy-MM-dd hh:mm:ss')
    datas.push(msg)
  }

  dealTrackListData (message) {
    let rows = message.data
    let datas = []
    let type = rows[0]
    let staffID = type && type.staff_id
    let vehicleID = type && type.vehicle_id
    let name = null
    if (staffID) {
      name = 'staff_id'
    } else if (vehicleID) {
      name = 'vehicle_id'
    }
    for (let i = 0, length = rows.length; i < length; i++) {
      let item = rows[i]
      let nextitem = rows[i + 1]
      let nextpoint = nextitem && nextitem.begin_pt
      if (!item.last_time) {
        if (nextitem) {
          item.last_time = nextitem.begin_time
        } else {
          item.last_time = new Date().format('yyyy-MM-dd hh:mm:ss')
        }
      }
      let beginTime = item.begin_time
      if (!beginTime) return
      let msg = {
        card_id: item.card_id,
        map_id: 5,
        speed: item.speed,
        landmark_id: item.landmark_id || 0,
        direction_mapper_id: item.direction_mapper_id || 0,
        landmark_dist: item.landmark_dist || 0,
        area_id: item.area_id
      }
      msg[name] = item[name]
      this.dealData(item, msg, datas, 'trackList', nextpoint)
    }
    if (datas.length > 0) message['data'] = datas
  }

  async getMetaData (def) {
    let fstring = ''
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
    }

    let sql = ''
    if (def.name === 'rt_person_forbid_down_mine') {
      sql = 'SELECT id, fdm.staff_id, name, dept_id, start_time, oper_time, oper_user FROM rt_person_forbid_down_mine fdm, dat_staff ds, dat_staff_extend dse WHERE fdm.staff_id = ds.staff_id AND fdm.staff_id = dse.staff_id AND STATUS = 1;'
    } else if (def.name === 'tt_inspection_route_planning') {
      sql = 'SELECT tir.staff_id, tir.status, ds.name, dse.dept_id, route_planning, reader_planning FROM tt_inspection_route_planning tir, dat_staff ds, dat_staff_extend dse WHERE tir.staff_id = ds.staff_id AND tir.staff_id = dse.staff_id;'
    } else {
      sql = `select ${fstring} from ${def.table} ${condition};`
    }

    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.warn('查询 META DB 失败！ \n', err)
    }
    return rows // Here the 'rows' will converted to be  Promise.resolve(rows)
  }

  async pullImportMsg (msg) {
    let req = msg.data
    let tablename = req.tablename.split('/')
    let name = null
    if (tablename.includes('dat_staff') || tablename.includes('dat_vehicle')) {
      name = tablename.includes('dat_staff') ? 'complex_data_staff' : 'complex_data_vehicle'
    } else {
      name = ['tt_inspection_route_planning', 'rt_person_forbid_down_mine'].includes(tablename[0]) ? tablename[0] : tablename[0].slice(4)
    }
    let broadcastdatas = await this.broadcastDatas(name)
    // console.log('============', broadcastdatas)
    process.send({
      cmd: 'PULLMSGREQUEST',
      broadcastdatas: broadcastdatas && broadcastdatas['datas'],
      room: broadcastdatas && broadcastdatas['room'],
      workerIndex: msg.workerIndex,
      username: req.username
    })
  }

  async broadcastDatas (name, sql) {
    let datas = {}
    let room = 'STANDBY'

    // datas[name] = await this.getMetaData(metaDefinition[name])
    if (!/complex_data/.test(name)) {
      name = name === 'area_reader' ? 'reader' : name
      datas[name] = name === 'area_reader' ? await this.getMetaData(metaDefinition['reader']) : await this.getMetaData(metaDefinition[name])
    }
    if (name === 'coalface') {
      datas['sensor'] = await this.getMetaData(metaDefinition['sensor'])
      datas['coalface_vehicle'] = await this.getMetaData(metaDefinition['coalface_vehicle'])
      datas['sensor_reader_map'] = await this.getMetaData(metaDefinition['sensor_reader_map'])
      datas['sensor_driver_map'] = await this.getMetaData(metaDefinition['sensor_driver_map'])
    } else if (name === 'drivingface') {
      datas['sensor'] = await this.getMetaData(metaDefinition['sensor'])
      datas['drivingface_vehicle'] = await this.getMetaData(metaDefinition['drivingface_vehicle'])
      datas['drivingface_ref_point'] = await this.getMetaData(metaDefinition['drivingface_ref_point'])
      datas['drivingface_warning_point'] = await this.getMetaData(metaDefinition['drivingface_warning_point'])
      datas['sensor_reader_map'] = await this.getMetaData(metaDefinition['sensor_reader_map'])
      datas['sensor_driver_map'] = await this.getMetaData(metaDefinition['sensor_driver_map'])
    } else if (name === 'sensor') {
      datas['sensor_reader_map'] = await this.getMetaData(metaDefinition['sensor_reader_map'])
      datas['sensor_driver_map'] = await this.getMetaData(metaDefinition['sensor_driver_map'])
    } else if ((name === 'dept' || name === 'occupation' || name === 'dept_ck') && typeof sql === 'object') {
      datas['staff_extend'] = await this.getMetaData(metaDefinition['staff_extend'])
      datas['vehicle_extend'] = await this.getMetaData(metaDefinition['vehicle_extend'])
    } else if (/complex_data/.test(name)) {
      let sqlname = /vehicle/.test(name) ? 'vehicle' : 'staff'
      datas[sqlname] = await this.getMetaData(metaDefinition[sqlname])
      datas[`${sqlname}_extend`] = await this.getMetaData(metaDefinition[`${sqlname}_extend`])
      datas['card'] = await this.getMetaData(metaDefinition['card'])
    } else if (name === 'power_levels') {
      datas['power_levels'] = await this.getMetaData(metaDefinition['power_levels'])
      datas['device_power'] = await this.getMetaData(metaDefinition['device_power'])
    } else if (name === 'reader') {
      datas['antenna'] = await this.getMetaData(metaDefinition['antenna'])
      datas['reader_path_tof_n'] = await this.getMetaData(metaDefinition['reader_path_tof_n'])
    } else if (name === 'area') {
      datas['area'] = await this.getMetaData(metaDefinition['area'])
      datas['leave'] = await this.getMetaData(metaDefinition['leave'])
    } else if (name === 'landmark' || name === 'rules') {
      room = 'MONITOR'
    }

    return {
      datas: datas,
      room: room
    }
  }

  async updateDB (req) {
    let unbroadcastname = ['coalfaceWork', 'drivingfaceWork', 'his_regular_cycle_detail', 'his_startup_detail']
    let sql = req.data.sql
    let rows = null // eslint-disable-line

    let updateSQLtime = new Date().format('yyyy-MM-dd hh:mm:ss')
    let mdtSql = null
    let cardMdtSql = null
    let timename = null
    let tablename = `dat_${req.data.name}`
    let updateArr = req.data.updateArr
    let deptArr = req.data.deptArr
    if (req.data.op === 'DELETE') {
      timename = 'lastDelete'
      mdtSql = `UPDATE ${DATABLENAME} SET lastDelete = '${updateSQLtime}' where tableName = '${tablename}'`
      // 批量删除人员、车辆
      if (req.data.name === 'complex_data_staffs' || req.data.name === 'complex_data_vehicles') {
        let tables = req.data.name === 'complex_data_staffs' ? 'dat_staff, dat_staff_extend' : 'dat_vehicle, dat_vehicle_extend'
        mdtSql = `UPDATE ${DATABLENAME} SET lastDelete = '${updateSQLtime}' where tableName in (${tables})`
      }
    } else {
      timename = 'lastUpdate'
      mdtSql = `UPDATE ${DATABLENAME} SET lastUpdate = '${updateSQLtime}' where tableName = '${tablename}'`
      if (tablename === 'dat_staff_extend') {
        let mdtCkSql = `UPDATE ${DATABLENAME} SET lastUpdate = '${updateSQLtime}' where tableName = '${tablename}_ck'`
        await mysql.query(mdtCkSql)
      }
      if (sql.card_sql) {
        cardMdtSql = `UPDATE ${DATABLENAME} SET lastUpdate = '${updateSQLtime}' where tableName = 'dat_card'`
      }
      if (req.data.name === 'reader' && typeof (req.data.sql) === 'object') {
        for (const key in req.data.sql) {
          if (key.includes('reader_path_tof_n')) {
            let beforeUpdateReaderTofSql = `DELETE FROM dat_reader_path_tof_n where reader_id = ${req.data.id}`
            await mysql.query(beforeUpdateReaderTofSql)
          }
        }
      }
      if (req.data.name === 'device_power' && Object.keys(req.data.limit_nets.limit).length !== 0) {
        let limitObj = req.data.limit
        let managerSql = {}
        managerSql.cmd = 'power_limit'
        managerSql.data = {
          nets: req.data.limit_nets.nets,
          limit: req.data.limit_nets.limit,
          deviceType: req.data.limit_nets.deviceType,
          deviceAddress: req.data.limit_nets.deviceAddress
        }
        this.manager.dispatch(null, managerSql)
      }
    }

    if (req.data.name === 'area' && ['UPDATE', 'DELETE'].includes(req.data.op)) {
      let leaveSql = `DELETE FROM dat_leave where area_id = ${req.data.id}`
      await mysql.query(leaveSql)
    }

    if (['dat_staff', 'dat_staff_extend', 'dat_complex_data_staffs', 'dat_complex_data_staff', 'dat_staff_extend_ck'].includes(tablename)) {
      let detail = req.data.detail, userID = req.data.user_id, sql = null
      if (req.data.op === 'DELETE' && !detail) {
        let detailSql = `SELECT ds.staff_id, CONCAT('工号:', ds.staff_id, ';姓名:', ds.name, ';卡号:',card_id, ';部门:',dd.name) AS detail FROM dat_staff_extend dse, dat_staff ds, dat_dept dd WHERE ds.staff_id = dse.staff_id AND dse.dept_id = dd.dept_id and ds.staff_id in (${req.data.id});`
        let detailRows = await mysql.query(detailSql)
        // console.log(detailRows)
        if (detailRows.length > 0) {
          detail = ''
          detailRows.forEach(item => { detail += `,(${item.staff_id}, '${req.data.op}', '${item.detail}', '${userID}')` })
          detail = detail.replace(',', '')
          sql = `INSERT INTO his_staff_op_log (staff_id, op_log, detail, user_id) VALUES ${detail}`
          await mysql.query(sql)
        }
      } else if (req.data.id) {
        sql = `INSERT INTO his_staff_op_log (staff_id, op_log, detail, user_id) VALUES(${req.data.id}, '${req.data.op}', '${detail}', '${userID}');`
        await mysql.query(sql)
      }
    }

    try {
      // rows = await mysql.query(sql)
      rows = await this.getSql(sql, req)
      if (updateArr && rows) {
        for (let i = 0; i < updateArr.length; i++) {
          await mysql.query(`REPLACE into rpt_sanlv_daily_detail (MainID,work_face_id,Analysis) VALUES (${rows.insertId},${deptArr[i]},'${updateArr[i]}')`)
        }
      }

      if (metaDefinition[req.data.name]) {
        await mysql.query(mdtSql)
        cardMdtSql && await mysql.query(cardMdtSql)
      }
    } catch (err) {
      console.warn('更新数据库失败', err)
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

      process.send({
        cmd: 'METAREQUEST',
        resMsg: resMsg,
        req: req,
        key: req.key,
        workerIndex: req.workerIndex,
        username: req.username
      })
      return
    }

    req['insertId'] = rows.insertId
    // 将成功结果返回。
    let resMsg = null // 应答数据
    resMsg = {
      code: 0,
      msg: sql,
      cmd: req.cmd,
      data: {
        op: req.data.op,
        name: req.data.name,
        id: req.data.id
      },
      mdtdata: {
        timename: timename,
        time: updateSQLtime
      },
      insertId: rows.insertId
    }
    let isImport = req['import']
    let broadcastdatas = null
    if (!isImport) {
      broadcastdatas = !unbroadcastname.includes(req.data.name) && await this.broadcastDatas(req.data.name, req.data.sql) // 广播的数据
    }
    process.send({
      cmd: 'METAREQUEST',
      req: req,
      resMsg: resMsg,
      broadcastdatas: broadcastdatas && broadcastdatas['datas'],
      room: broadcastdatas && broadcastdatas['room'],
      key: req.key,
      workerIndex: req.workerIndex,
      username: req.username
    })
  }

  async checkFile () {
    this.reportTime = await this.getTimeInterval()
    this.startInterval()
  }

  // 检查发送采集列表
  checkCollectorList () {
    process.send({
      cmd: 'COLLECTORLIST'
    })
  }
}
