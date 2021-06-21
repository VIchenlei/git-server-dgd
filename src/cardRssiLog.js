import mysql from './MysqlWraper.js'
let fs = require('fs')
let path = require('path')
let readline = require('readline')

const TOFLOG = path.resolve(__dirname, '../../../tof')
const TIMEDIFF = 60 * 60 * 1000 // 时间间隔为1小时

export default class CardRssiLog {
  constructor () {
    this.time = new Date().getTime() // 第一次统计时间
    this.rssiDistance = 0
    this.rssiData = null

    this.registerEventHandler()
  }

  registerEventHandler () {
    process.on('message', (msg, handle, cb) => {
      let cmd = msg.cmd || msg.req.cmd
      if (cmd === 'fadeArea') {
        let time = new Date().getTime()
        let timeDiff = time - this.time
        this.dealQueryData(msg) // 无论时间差是否大于定义的时间差，都先返回一个结果，否则处理文件时间过长，用户等待时间过长
        if (timeDiff > TIMEDIFF) {
          this.init()
          this.dealQueryData(msg)
        }
      } else if (cmd === 'fadeAreaInit') {
        this.init()
      }
    })
  }

  init () {
		// console.log('starttime', new Date().getTime())
    fs.readdir(TOFLOG, async (err, files) => {
      if (files) {
        let tofFiles = files.filter(file => /^dist-tof/.test(file))
        tofFiles.sort((file1, file2) => {
          let stat1 = fs.statSync(`${TOFLOG}/`, file1)
          let stat2 = fs.statSync(`${TOFLOG}/`, file2)

          return stat2.mtime - stat1.mtime
        })
        // console.log(tofFiles)
        let latestLog = tofFiles[1]
        this.rssiDistance = await this.initDistance()
        this.initData(latestLog)
      }
    })
  }

  async initDistance () {
    let sql = `select rssi_distance from dat_reader_rssi;`
    let rssiDistance = 0
    try {
      let row = await mysql.query(sql)
      rssiDistance = row[0].rssi_distance
    } catch (err) {
      console.log(err)
    }
    return rssiDistance
  }

  initData (log) {
    try {
      let rssiData = new Map()
      let input = fs.createReadStream(`${TOFLOG}/${log}`)
      let rl = readline.createInterface({
        input: input
      })
      rl.on('line', (item) => {
        if (item.includes('card_message')) {
          let line = item.split(/[=,]/)
          let readerID = parseInt(line[1], 10)
          let tofDistance = line[11].split(/[()m]/)
          let distance = parseInt(tofDistance[1], 10)
          let rssi = parseInt(line[17], 10)
          let multiples = Math.ceil(distance / this.rssiDistance)
          let key = readerID * 100000 + multiples * this.rssiDistance
          let multiplesData = rssiData.get(key)
          if (!multiplesData) {
            rssiData.set(key, [])
            multiplesData = rssiData.get(key)
          }
          multiplesData.push({
            reader_id: readerID,
            distance: multiples * this.rssiDistance,
            m: distance,
            rssi: rssi
          })
        }
      })
      rl.on('close', (line) => {
        this.handleData(rssiData)
      })
    } catch (err) {
      console.log(err)
    }
  }

  handleData (data) {
    let handleResult = new Map()
    let keys = Array.from(data.keys())
    keys.forEach(key => {
      if (key) {
        let datas = data.get(key)
        let sum = datas.reduce((acc, cur) => acc + cur.rssi, 0)
        let length = datas.length
        let average = Math.ceil(sum / length)
        let readerID = datas[0].reader_id
        let distance = datas[0].distance
        let readerRssi = handleResult.get(readerID)
        if (!readerRssi) {
          handleResult.set(readerID, {
            reader_id: readerID,
            distance_count: []
          })
          readerRssi = handleResult.get(readerID)
        }
        let ret = readerRssi.distance_count
        ret.push([distance, average])
      }
    })
    this.rssiData = handleResult
    this.time = new Date().getTime()
  }

  dealQueryData (req) {
    let queryReaderID = req.data.readerId
    let message = []
    if (this.rssiData) {
      message = Array.from(this.rssiData.values())
      if (queryReaderID) {
        message = message.filter(item => item.reader_id === queryReaderID)
      }
    }
    process.send({
      cmd: 'FEADRUQUIRE',
      key: req.key,
      message: message,
      socketID: req.socketID,
      workerIndex: req.workerIndex
    })
  }
}
