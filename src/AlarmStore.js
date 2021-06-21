import mysql from './MysqlWraper.js'
import Caller from './Caller.js'

export default class AlarmStore {
  constructor () {
    this.handDisplayEvents = new Map()
  }

  async readDBHandDisplayEvent () {
    let self = this
    let sql = `SELECT CONCAT(id, '') AS id, 0 AS status, event_type_id as type_id, obj_type_id, obj_id, dis_type as dis_type_id, map_id, area_id, x, y, limit_value, cur_value, event_id AS cur_time FROM his_event_data WHERE deal = 0 and stat = 0;`
    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      console.log(err)
      return
    }
    rows && rows.forEach(item => self.handDisplayEvents.set(item.id, item))
  }

  async handleDB (eventID, eventTypeID, curTime, statue) {
    let sql = null
    if (statue === 0) {
      sql = `UPDATE his_event_data SET deal = ${statue} where id = ${eventID} and event_type_id = ${eventTypeID} and cur_time = '${new Date(curTime).format('yyyy-MM-dd hh:mm:ss')}.${(curTime % 1000).toString().padStart(3, 0)}' and stat = 0;`
    } else {
      sql = `UPDATE his_event_data SET deal = ${statue} where id = ${eventID} and event_type_id = ${eventTypeID} and stat = 0 and deal = 0;`
    }
    await mysql.query(sql)
  }

  async handleCredentialsDB (eventID) {
    let sql = `UPDATE dat_credentials_staff SET warn_id = 1 where credentials_staff_id = ${eventID};`
    await mysql.query(sql)
  }

  deleteHandDisplayEvent (id, eventTypeID, curTime) {
    this.handDisplayEvents.delete(id)
    this.handleDB(id, eventTypeID, curTime, 1)
  }

  storeHandDisplayEvents (req) {
    if (!req) return
    let self = this
    let data = req.data.forEach(alarm => {
      let eventID = alarm.event_id || alarm.credentials_staff_id
      let eventTypeID = alarm.type_id
      let curTime = alarm.cur_time
      let storeAlarm = JSON.parse(JSON.stringify(alarm))
      self.handDisplayEvents.set(eventID, storeAlarm)
      alarm.event_id ? self.handleDB(eventID, eventTypeID, curTime, 0) : self.handleCredentialsDB(eventID)
      // self.handleDB(eventID, eventTypeID, curTime, 0)
      alarm.status = 100
      return alarm
    })
  }

  async startGasCall (socket, row, callStore) {
    let type = row.type_id
    type = parseInt(type, 10)
    let msg = null
    let time = Number(new Date().getTime())
    let objID = Number(row.obj_id)
    if (Number(row.status) === 0) {
      if (callStore.calling.get(`${type}-${objID}`)) return
      let rows = null
      try {
        let sql = type === 35 ? `select card_id from dat_sensor_driver_map dsdm, dat_staff_extend dse where dsdm.staff_id = dse.staff_id and sensor_id = ${objID};` : `select reader_id from dat_sensor_reader_map where sensor_id = ${objID}`
        rows = await mysql.query(sql)
        if (rows.length <= 0) return
        if (!callStore.calling.get(`${type}-${objID}`)) {
          let ret = new Map()
          callStore.calling.set(`${type}-${objID}`, ret)
        }
        let calling = callStore.calling.get(`${type}-${objID}`)
        for (let i = 0; i < rows.length; i++) {
          let callID = type === 35 ? rows[i].card_id : rows[i].reader_id
          type === 35 ? calling.set(callID, {
            cardid: callID,
            cardtype: 1
          }) : calling.set(callID, {
            stationid: callID
          })
        }
        let callCards = Array.from(calling.values())
        msg = {
          cmd: 'call_card_req',
          data: {
            call_type_id: 1, // 全员呼叫:0 定员呼叫:1
            call_time_out: 5, // 呼叫时长
            call_level_id: 2, // 呼叫类型 一般呼叫:1 紧急呼叫:2
            user_name: 'systrm', // 呼叫人
            call_time: time // 呼叫时间戳
          }
        }
        if (type === 35) {
          msg.data['stations'] = [{ stationid: 0 }]
          msg.data['cards'] = callCards
        } else {
          msg.data['stations'] = callCards
          msg.data['cards'] = []
        }
      } catch (err) {
        console.error(`查询DB失败。 \n\t ${err}`)
        return
      }
    } else if (Number(row.status) === 100) {
      let callCards = callStore.calling.get(`${type}-${objID}`) && Array.from(callStore.calling.get(`${type}-${objID}`).values())
      if (callCards && callCards.length > 0) {
        msg = {
          cmd: 'call_card_cancel_req',
          data: {
            call_type_id: 1, // 全员/定员
            user_name: 'systrm', // 取消人
            call_time: time // 时间戳
          }
        }
        if (type === 35) {
          msg.data['stations'] = [{ stationid: 0 }]
          msg.data['cards'] = callCards
        } else {
          msg.data['stations'] = callCards
          msg.data['cards'] = []
        }
      }
      callStore.calling.delete(`${type}-${objID}`)
    }
    Caller.call(socket, msg)
  }

  filterAlarm (socket, rows, callSotre) {
    let datas = []
    if (!rows) return
    for (let i = 0, len = rows.length; i < len; i++) {
      let row = rows[i]
      let id = row.event_id
      let status = row.status
      if (!this.handDisplayEvents.get(id)) {
        datas.push(row)
      }
      if (status === 100) {
        this.deleteHandDisplayEvent(id, row.type_id, row.cur_time)
      }
      let type = row.type_id
      type = parseInt(type, 10)
      if (type === 34 || type === 35) {
        this.startGasCall(socket, row, callSotre)
      }
    }
    return datas
  }

  // 恢复所有告警
  recoverAlarm (req) {
    let self = this
    let condition = req.data
    let recoverDatas = null
    let datas = Array.from(this.handDisplayEvents.values())
    if (condition) {
      let dataObj = condition.dataObj, dataType = condition.dataType, dataTypeNot = condition.dataTypeNot
      let sqlCondition = `${dataObj ? ` and obj_type_id in (${dataObj.join(',')})` : ''}${dataType ? ` and event_type_id in (${dataType.join(',')})` : ''}${dataTypeNot ? ` and event_type_id not in (${dataTypeNot})` : ''}`
      recoverDatas = datas.filter(item => {
        if (dataObj && dataObj.includes(item.obj_type_id)) {
          if (dataType && dataType.includes(item.type_id)) {
            self.handDisplayEvents.delete(item.event_id)
            return item
          } else if (dataTypeNot && dataTypeNot.includes(item.type_id)) {
            self.handDisplayEvents.delete(item.event_id)
            return item
          } else if (!dataType && !dataTypeNot) {
            self.handDisplayEvents.delete(item.event_id)
            return item
          }
        } else if (!dataObj && !item.obj_type_id) {
          self.handDisplayEvents.delete(item.credentials_staff_id)
          return item
        }
      })

      let sql = dataObj ? `UPDATE his_event_data SET deal = 1 where deal = 0 ${sqlCondition};` : `UPDATE dat_credentials_staff SET warn_id = 0 where warn_id = 1;`
      mysql.query(sql)
    } else {
      recoverDatas = datas
      let sql = `UPDATE his_event_data SET deal = 1 where deal = 0;`
      let credentialSql = `UPDATE dat_credentials_staff SET warn_id = 0 where warn_id = 1;`
      mysql.query(sql)
      mysql.query(credentialSql)
      this.handDisplayEvents.clear()
    }

    let msg = {
      cmd: 'alarm_done',
      data: recoverDatas
    }
    return msg
  }
}
