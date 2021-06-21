import mysql from './MysqlWraper.js'

export default class PowerStore {
  constructor () {
    this.powerStore = new Map()
  }

  storePower (msg) {
    const { data } = msg
    const { deviceAddress } = data
    data['deviceAddress'] = deviceAddress // 电源ID
    this.powerStore.set(deviceAddress, data.data)
  }

  getLists (length) {
    // if (!length) {
    //   const sql = `SELECT count(1) as total FROM dat_device_power;`
    //   try {
    //     const rows = await mysql.query(sql)
    //     length = rows.total
    //   } catch (error) {
    //     console.warn(error)
    //   }
    // }

    const curPowers = Array.from(this.powerStore.values())
    // 当前有多少power数据，就返回多少
    return curPowers
  }
}
