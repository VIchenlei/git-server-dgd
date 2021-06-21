import mysql from './MysqlWraper.js'
import net from 'net'
import {
  handleUnIPBuffer,
  parsingData,
  READER_COMMAND_WORD,
  handleBuffer
} from './reader_command_word.js'

export default class Power {
  constructor (powerStore) {
    this.name = null
    this.timer = null // 定时获取电源信息
    this.interval = 24 * 60 * 60 * 1000
    this.powerStore = powerStore
    // this.registerEventHandler()
  }

  // registerEventHandler () {
  //   process.on('message', async (msg) => {
  //     const { cmd, data } = msg
  //     let self = this
  //     let sql = null
  //     switch (cmd) {
  //       case 'power_msg':
  //         this.getPowerMsg()
  //         this.timer = setInterval(self.getPowerMsg(self), self.interval)
  //         break
  //       case 'power_discharge':
  //         this.name = 'device_configuration'
  //         sql = `SELECT power_levels_id, ddp.device_power_id, discharge_voltage_cycle, discharge_time, ip, device_id as deviceAddress, device_type_id as deviceType FROM dat_power_levels dpl, dat_device_power ddp WHERE dpl.device_power_id = ddp.device_power_id;`
  //         const powersLevel = this.getPowers(sql)
  //         this.testDischarge(powersLevel)
  //         break
  //       case 'update_search_power_time':
  //         clearInterval(this.timer)
  //         this.interval = data.value * 60 * 60 * 1000
  //         this.timer = setInterval(self.getPowerMsg(self), self.interval)
  //         break
  //     }
  //   })
  // }
  dispatch (msg) {
    const { cmd, data } = msg
    let self = this
    let sql = null
    switch (cmd) {
      case 'power_msg':
        this.getPowerMsg()
        this.timer = setInterval(this.getPowerMsg.bind(this), this.interval)
        break
      case 'power_discharge':
        this.name = 'device_configuration'
        sql = `SELECT power_levels_id, ddp.device_power_id, discharge_voltage_cycle, discharge_time, ip, device_id as deviceAddress, device_type_id as deviceType FROM dat_power_levels dpl, dat_device_power ddp WHERE dpl.device_power_id = ddp.device_power_id;`
        const powersLevel = this.getPowers(sql)
        this.testDischarge(powersLevel)
        break
      case 'update_search_power_time':
        clearInterval(this.timer)
        this.interval = data.value * 60 * 60 * 1000
        console.log(this.interval)
        this.timer = setInterval(this.getPowerMsg.bind(this), this.interval)
        break
    }
  }

  // 获取电源信息
  async getPowerMsg () {
    console.log('启动定时器')
    this.name = 'device_power'
    const sql = `SELECT power_levels_id, ddp.device_power_id as deviceAddress, discharge_voltage_cycle, discharge_time, dr.ip, device_id, ddp.device_type_id as deviceType, dr.reader_id AS ipDeviceAddress, dr.reader_type_id AS ipDeviceType FROM dat_power_levels dpl, dat_device_power ddp, dat_reader dr WHERE dpl.device_power_id = ddp.device_power_id AND ddp.device_id = dr.reader_id;`
    const powers = await this.getPowers(sql)
    this.sendPowerCommand(powers, this)
  }

  async testPowerDischarge () {
    this.name = 'power_discharge'
    const sql = `SELECT power_levels_id, ddp.device_power_id as deviceAddress, discharge_voltage_cycle, discharge_time, dr.ip, device_id, ddp.device_type_id as deviceType, dr.reader_id AS ipDeviceAddress, dr.reader_type_id AS ipDeviceType FROM dat_power_levels dpl, dat_device_power ddp, dat_reader dr WHERE dpl.device_power_id = ddp.device_power_id AND ddp.device_id = dr.reader_id AND dpl.power_status = 1;`
    const powersLevel = await this.getPowers(sql)
    this.testDischarge(powersLevel)
  }

  async getPowers (sql) {
    // const sql = `SELECT device_id as deviceAddress, device_type_id as deviceType, ip, device_power_id from dat_device_power;`
    try {
      const rows = await mysql.query(sql)
      return rows
    } catch (err) {
      console.warn(err)
    }
  }

  getDissTime (now, time) {
    return Math.floor((now.getTime() - new Date(time).getTime()) / (24 * 60 * 60 * 1000))
  }

  // 检查放电时间
  testDischarge (powers) {
    if (!powers) return
    const now = new Date()
    for (let i = 0; i < powers.length; i++) {
      const power = powers[i]
      const { power_levels_id, device_power_id, discharge_voltage_cycle, discharge_time, ip, deviceAddress, deviceType } = power
      if (discharge_time) {
        const dissTime = this.getDissTime(now, discharge_time)
        if (dissTime < discharge_voltage_cycle) continue
      }
      this.sendPowerCommand([power])
    }
  }

  updatePowerDB (result) {
    const { power_rode, deviceAddress, excharge_state } = result
    const now = new Date().format('yyyy-MM-dd')
    if (power_rode) {
      const sql = `UPDATE dat_power_levels dpl, dat_device_power ddp SET discharge_time = '${now}', power_status = ${excharge_state} WHERE device_id = ${deviceAddress} AND power_levels_id = ${power_rode} AND dpl.device_power_id = ddp.device_power_id;`
    } else {
      const sql = `UPDATE dat_power_levels dpl, dat_device_power ddp SET power_status = ${excharge_state} WHERE device_id = ${deviceAddress} AND power_levels_id = ${power_rode} AND dpl.device_power_id = ddp.device_power_id;`
    }
    this.getPowers(sql)
  }

  isPowerData (data) {
    const commandWord = data.slice(2, 4)
    if (commandWord.equals(Buffer.from([0xa1, 0x0b]))) {
      const firstcc = data.slice(13, 14)
      if (firstcc.equals(Buffer.from([0x0c]))) return true
    }
    return false
  }

  sendPowerCommand (list, self) {
    const msg = list && list.shift()
    if (!msg) return
    self = self || this
    const { deviceAddress, ip, ipDeviceAddress } = msg
    const sendData = { deviceAddress, deviceType: 12, isIP: false }
    if (this.name === 'power_discharge') {
      const { power_levels_id } = msg
      sendData['data'] = {}
      sendData['data']['power_rode'] = power_levels_id
    }
    const tcpClient = new net.Socket()
    const commandWord = READER_COMMAND_WORD[self.name]

    if (!ip) return
    tcpClient.connect(6000, ip, () => {
      const ipDeviceMap = new Map()
      ipDeviceMap.set(ip, {
        ipDeviceAddress: ipDeviceAddress,
        ipDeviceType: 1
      })
      const sendMag = handleBuffer(commandWord, self.name, sendData)
      const {resultBuffer} = handleUnIPBuffer(commandWord, self.name, sendData, sendMag, ipDeviceMap, ip)
      tcpClient.write(resultBuffer)
    })

    tcpClient.on('data', (data) => {
      const isPowerData = self.isPowerData(data)
      if (!isPowerData) return
      // console.log('接收到的消息', data)
      let parsingResult = parsingData(data, self.name)
      self.updatePowerDB(parsingResult)
      self.powerStore.storePower(parsingResult)
      // process.send({
      //   cmd: 'POWER_RESPONSE',
      //   data: parsingResult,
      //   deviceAddress: deviceAddress
      // })
    })

    tcpClient.on('error', error => {
      // console.warn(error)
    })

    tcpClient.on('close', () => {
      if (list.length > 0) {
        self.sendPowerCommand(list, self)
      }
    })
  }
}
