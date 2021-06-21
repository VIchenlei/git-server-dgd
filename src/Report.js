import mysql from './MysqlWraper.js'
import csv from 'fast-csv'
import PdfPrinter from 'pdfmake'
import Excel from 'ejsexcel'

let pdfDefinition = require('./pdfDefinition.js')
let PdfPrinterDriver = require('./pdfDefintionDriver.js')
let fs = require('fs')
let path = require('path')
// let crypto = require('crypto')
let config = require('./config/appconfig.js')

export default class Report {
  constructor (user, metaStore, sqlResultStore, socket) {  // eslint-disable-line
    this.user = user
    this.metaStore = metaStore
    this.sqlResultStore = sqlResultStore
    this.pdfDefinition = pdfDefinition
    this.pdfDefinitionDriver = PdfPrinterDriver
  }

  dispatch (socket, req, callback) {
    if (!req) {
      console.warn('Report: no req data.')
      return
    }

    switch (req.cmd) {
      case 'query':
        this.sendData(socket, req, callback)
        break
      case 'querywork':
        this.sendDataWork(socket, req, callback)
        break
      case 'file':
        this.data = req.data
        this.sendFile(socket, req, callback)
        break
      case 'queryHistoryScene':
        this.composeData(socket, req, callback)
        break
      case 'queryTrackList':
        this.listData(socket, req, callback)
        break
      case 'historyData':
        this.dataForHistory(socket, req, callback)
        break
      default:
        console.warn(`未知的 REPT 请求：${req.cmd}`)
        break
    }
  }

  formatField (name, value, types, namesInShort, tablename) {
    let type = types[namesInShort.indexOf(name)]
    return this.metaStore.formatField(name, value, type, '', tablename)
  }

  // async preparePath (path) {
  //   let exist = await file.dirExist(path)
  //   if (!exist) {
  //     await file.makeDir(path)
  //   }
  // }

  async sendFile (socket, req, callback) {
    // console.log('REPT : \n ', req)
    let message = null  // return message

    let rows = null
    let isQuery = req.isQuery
    let daysArr = req.daysArr
    let data = req.data ? JSON.parse(req.data) : []
    try {
      if (isQuery) {
        rows = data
      } else {
        rows = await mysql.query(req.sql)
      }
    } catch (err) {
      console.warn('查询 REPT DB 失败！ \n\t', err)
      message = {
        code: -1,
        msg: '查询失败！'
      }
      callback(message)

      return
    }
    if (req.name === 'person_month') {
      rows = rows.map(item => {
        let dataArr = new Map()
        item['concat_day'] && item['concat_day'].split(',').forEach(it => {
          let itArr = it.split(';')
          let itKey = itArr[0].toString()
          let itKeyValue = itArr[1]
          if (dataArr.get(itKey)) {
            itKeyValue = dataArr.get(itKey)[itKey] + ',' + itKeyValue
          }
          dataArr.set(itKey, {
            [itKey]: itKeyValue
          })
        })
        for (let i = 0; i < daysArr.length; i++) {
          let key = daysArr[i].toString()
          item['z' + key] = dataArr.get(key) ? dataArr.get(key)[key] : null
        }
        delete item['concat_day']
        delete item['month']
        return item
      })
    }
    let namesInShort = req.namesInShort
    let types = req.types
    for (let i = 0; i < rows.length; i++) {
      for (let name in rows[i]) {
        let value = rows[i][name]
        let formatTableName = name === 'staff_id' ? 'dat_staff' : req.name
        rows[i][name] = namesInShort && this.formatField(name, value, types, namesInShort, formatTableName)
      }
    }

    let fieldLabels = req.labels

    let reptTitle = req.title  // report reptTitle
    let fileType = req.fileType  // csv, pdf
    let name = req.name

    let time = req.time ? req.time : ''
    // let hash = crypto.createHash('md5')
    // let hashString = hash.digest('hex')
    let date = new Date()
    let fileName = this.user.name + '-' + req.name + '-' + date.format('yyyyMMddhhmmss')
    let fileFullName = `${fileName}.${fileType === 'printPDF' ? 'pdf' : fileType}`

    let fileRelativePath = `/datafiles/${fileType === 'printPDF' ? 'pdf' : fileType}`

    let fileURI = `${fileRelativePath}/${fileFullName}`
    let filePath = path.resolve(config.CLIENT_STATIC_DIR, `.${fileURI}`)

    let isCreateFileOK = false
    switch (fileType) {
      case 'csv':
        isCreateFileOK = this.createCVS(filePath, fieldLabels, rows, reptTitle)
        break
      case 'xlsx':
        isCreateFileOK = this.createExcel(filePath, fieldLabels, rows, reptTitle, name, data, time)
        break
      case 'pdf':
      case 'printPDF':
        // console.log('>>>>>>>>>>>' + namesInShort.length)
        let widthes = []
        widthes[0] = '*'
        for (let i = 1, len = namesInShort.length; i < len; i++) {
          widthes[i] = parseInt(100 / len) + '%'
        }
        if (name !== 'driver') {
          this.pdfDefinition.content[0].text = null
          this.pdfDefinition.content[1].text = null
          this.pdfDefinition.content[2].text = null
          this.pdfDefinition.content[3].table.body.splice(0, this.pdfDefinition.content[3].table.body.length)
          this.pdfDefinition.content[3].table.widths = widthes
          this.pdfDefinition.content[4].text = null
        } else {
          this.pdfDefinitionDriver.content[0].text = null
          // this.pdfDefinitionDriver.content[1].text = null
          this.pdfDefinitionDriver.content[1].text = null
          this.pdfDefinitionDriver.content[2].table.body.splice(0, this.pdfDefinitionDriver.content[2].table.body.length)
          this.pdfDefinitionDriver.content[3].text = null
        }
        let userName = req.userName
        let timeStample = date.format('yyyy-MM-dd hh:mm')
        let expr = null
        if (req.exprList.length !== 0) {
          for (let i = 0; i < req.exprList.length; i++) {
            if (i === 0) {
              expr = req.exprList[i].label + ' '
            } else {
              expr += req.exprList[i].logicLabel + ' ' + req.exprList[i].label + ' '
            }
          }
        } else {
          expr = '所有记录'
        }

        /*
        console.log('Ready to soar')
        console.log('Fuel tanks are filled: ' + this.pdfDefinition.content[0].text)
        console.log('Gotta clear view, sir ' + this.pdfDefinition.content[3].table.body[0])
        console.log('-----------------------------------')
        */
        isCreateFileOK = this.createPDF(reptTitle, filePath, fieldLabels, rows, userName, timeStample, expr)
        break
      default:
        console.warn('UNKNOWN file type.', fileType)
        break
    }

    if (isCreateFileOK) {
      message = {
        code: 0,
        msg: 'OK',
        data: {
          name: `${fileName}.${fileType}`,
          fileType: fileType,
          url: fileURI
          // rows: rows
        }
      }
      this.judgeFileExist(filePath, callback, message)
    } else {
      message = {
        code: -1,
        msg: '获取文件失败，请联系系统管理员。'
      }
      callback(message)
    }

    // callback(message)
  }

  judgeFileExist (filePath, callback, message) {
    fs.exists(filePath, function (exist) {
      callback(message)
    })
  }

  getExcelDatas (worktimeDept) {
    let keys = Array.from(worktimeDept.keys())
    let arr = []
    keys.forEach(key => {
      let msg = {}
      let rows = worktimeDept.get(key)
      msg['dept_id'] = this.metaStore.getNameByID('work_face_id', key)
      msg['znum'] = rows.get(1) ? rows.get(1).num : 0
      msg['zworktime'] = rows.get(1) ? rows.get(1).worktime : 0
      msg['enum'] = rows.get(2) ? rows.get(2).num : 0
      msg['eworktime'] = rows.get(2) ? rows.get(2).worktime : 0
      msg['fnum'] = rows.get(3) ? rows.get(3).num : 0
      msg['fworktime'] = rows.get(3) ? rows.get(3).worktime : 0
      msg['anum'] = msg['znum'] + msg['enum'] + msg['fnum']
      msg['aworktime'] = (msg['zworktime'] + msg['eworktime'] + msg['fworktime']).toFixed(2)
      arr.push(msg)
    })
    return arr
  }

  createExcel (filePath, labels, rows, reptTitle, name, data, time) {
    if (name === 'worktime_dept_shift') {
      let worktimeDept = new Map()
      rows.forEach(row => {
        let deptID = row.workface_id
        if (!worktimeDept.get(deptID)) {
          let ret = new Map()
          worktimeDept.set(deptID, ret)
        }
        let deptrow = worktimeDept.get(deptID)
        let shiftID = row.shift_id
        let msg = {
          num: row.num,
          worktime: row.worktime
        }
        deptrow.set(shiftID, msg)
      })
      let resultArr = this.getExcelDatas(worktimeDept)
      resultArr = [[{'tablename': reptTitle}], resultArr]
      let exlBuf = fs.readFileSync('../resource/worktime_dept_shift.xlsx')
      try {
        Excel.renderExcel(exlBuf, resultArr).then((exlBuf2) => {
          fs.writeFileSync(filePath, exlBuf2)
        })
      } catch (err) {
        return false
      }
      return true
    } else if (name === 'rept_efficiency_manage') {
      let resultArr = data
      for (let i = 0; i < resultArr.length; i++) {
        resultArr[i][0].sort(function (a, b) { return a.index - b.index })
      }
      let allLength = {
        'jBoot': data[0][0].length - 1,
        'cBoot': data[0][1].length - 1,
        'totalBoot': data[0][0].length + data[0][1].length,
        'jRugular': data[1][0].length - 1,
        'cRugular': data[1][1].length - 1,
        'totalRugular': data[1][0].length + data[1][1].length,
        'jWorktime': data[2][0].length - 1,
        'cWorktime': data[2][1].length - 1,
        'totalWorktime': data[2][0].length + data[2][1].length
      }
      resultArr = [[{'tablename': reptTitle, 'tabletime': time}], [allLength], resultArr]
      let exlBuf = fs.readFileSync('../resource/rept_efficiency_manage.xlsx')
      try {
        Excel.renderExcel(exlBuf, resultArr).then((exlBuf2) => {
          fs.writeFileSync(filePath, exlBuf2)
        })
      } catch (err) {
        return false
      }
      return true
    }
  }

  createCVS (filePath, labels, rows, reptTitle) {
    let csvStream = null

    let writableStream = null
    try {
      writableStream = fs.createWriteStream(filePath)
      // 在 csv 文件头写入 utf-8 BOM, 解决 excel 打开乱码的问题
      // writableStream.write(new Buffer('\xEF\xBB\xBF', 'binary'))
      writableStream.write(Buffer.from('\xEF\xBB\xBF', 'binary'))

      writableStream.on('finish', () => {
        console.log('Write csv file DONE! ', filePath)
      })
    } catch (err) {
      console.warn(`生成 CSV 文件 ${filePath} 失败！\n\t${err}`)
      return false
    }

    csvStream = csv.format({headers: true, quoteColumns: true, quoteHeaders: true})
    // csvStream = csv.format({headers: true})
    csvStream.pipe(writableStream)
        // .on('end', process.exit)
    let arrTitle = new Array(reptTitle)
    csvStream.write(arrTitle)
    csvStream.write(labels)
    // csvStream.write(rows)
    for (let j = 0; j < rows.length; j++) {
      let row = rows[j]
      let rowInArray = []
      for (let item in row) {
        rowInArray.push(row[item])
      }
      csvStream.write(rowInArray)
    }
    csvStream.end()

    return true
  }

  createPDF (reptTitle, filePath, labels, rows, userName, timeStample, expr) {
    let fonts = {
      Roboto: {
        normal: './fonts/Microsoft-YaHei.ttf',
        bold: './fonts/Microsoft-YaHei-Bold.ttf'
      }
    }

    let PDF = new PdfPrinter(fonts)
    // console.log('Ready to soar')
    // console.log('Fuel tanks are filled: ' + pdfDefinition.content[0].text)
   // console.log('Gotta clear view, sir ' + pdfDefinition.content[1].table.body)
   // console.log('-----------------------------------')

    if (reptTitle === '司机排班') {
      this.pdfDefinitionDriver.content[0].text = '高河矿' + reptTitle + '报表'
      // this.pdfDefinitionDriver.content[1].text = '（制表时间：' + timeStample + '）'
      this.pdfDefinitionDriver.content[1].text = '制表时间：' + timeStample
      this.pdfDefinitionDriver.content[2].table.body.push(labels)
      // console.log('>>>>>>>>>>>>>>>>>' + labels)
      this.pdfDefinitionDriver.content[3].text = '制表人: ' + '\u0020\u0020\u0020\u0020\u0020\u0020\u0020\u0020' + '审核人：'
    } else {
      this.pdfDefinition.content[0].text = '高河矿' + reptTitle + '报表'
      this.pdfDefinition.content[1].text = '（' + expr + '）'
      this.pdfDefinition.content[2].text = ' 制表时间：' + timeStample
      this.pdfDefinition.content[3].table.body.push(labels)
      this.pdfDefinition.content[4].text = '制表人: ' + '\u0020\u0020\u0020\u0020\u0020\u0020\u0020\u0020' + '审核人：'
    }

    /*
    console.log('Check out the view ' + pdfDefinition.content[0].text)
    console.log('I can go anywhere ' + pdfDefinition.content[1].table.body)
    console.log('-----------------------------------')
    */
    // console.log('Pushin\' away.')
    // console.log('md,wobubianle')
    for (let j = 0; j < rows.length; j++) {
      // pdfDefinition.content[1].table.body.push(rows[j])
      let row = rows[j]
      let rowData = []
      for (let item in row) {
       // console.log('the type of item is ' + typeof (row[item]))
        // console.log('the content of item is ' + row[item])
        let data = null
        if (row[item] === null || row[item] === '' || typeof (row[item]) === 'undefined') {
          data = ' '
        } else if (typeof (row[item]) !== 'string') {
          data = row[item].toString()
        } else {
          data = row[item]
        }
        // console.log('the type of data is ' + typeof (data))
        rowData.push(data)
      }
      if (reptTitle === '司机排班') {
        this.pdfDefinitionDriver.content[2].table.body.push(rowData)
        // console.log('<<<<<<<<<<<<<<<' + rowData)
      } else {
        this.pdfDefinition.content[3].table.body.push(rowData)
      }
    }
    // console.log(pdfDefinition.styles)
    /*
    console.log('after push: ')
    console.log(pdfDefinition.content[0].text)
    console.log(pdfDefinition.content[1].table.body)
    */
    // console.log('Data push done')
    try {
      if (reptTitle === '司机排班') {
        let pdfDoc = PDF.createPdfKitDocument(this.pdfDefinitionDriver)
        pdfDoc.pipe(fs.createWriteStream(filePath))
        pdfDoc.end()
        // console.log('Tesla suit ready!')
      } else {
        // console.log('Training...')
        let pdfDoc = PDF.createPdfKitDocument(this.pdfDefinition)
        // console.log('Training Complete.')
        // console.log('PDF Soldier: ' + pdfDoc)
        // console.log('-------------')
        // console.log('How \'bout some action?')
        pdfDoc.pipe(fs.createWriteStream(filePath))
        // console.log('For Mother Russia!')
        // console.log('-------------')
        pdfDoc.end()
        // console.log('Tesla suit ready!')
      }
    } catch (err) {
      console.log(err)
      console.warn(`生成 PDF 文件 ${filePath} 失败！\n\t${err}`)
      return false
    }
    return true
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

  getWorktimeResult (req) {
    let searchTime = req.data.searchTime
    let updateTime = this.sqlResultStore.updateTime
    let updateMonth = this.sqlResultStore.updateMonth
    let timeDistance = 60 * 60 * 1000
    let now = new Date().getTime()
    if (updateMonth === searchTime && (updateTime + timeDistance > now)) {
      return false
    }
    this.sqlResultStore.updateMonth = searchTime
    return true
  }

  async sendData (socket, req, callback) {
    // console.log('REPT : \n ', req)
    let message = null

    // init total
    let total = 0
    if (req.data.pageIndex === 0 && req.data.total < 0) {
      total = await this.getTotal(req.data.countSql)
    } else {
      total = req.data.total
    }

    // adjust sql
    let sql = req.data.sql  // 默认不分页
    if (req.data.pageSize > 0) {  // 如果 pageSize 值有效，则需要分页
      let start = req.data.pageIndex * req.data.pageSize
      // if (Number(req.data.start) === 0) {
      //   start = 0
      // }
      let count = req.data.pageSize

      sql = sql && sql.trim()
      if (sql.endsWith(';')) {  // 去掉末尾的 ';'
        sql = sql.slice(0, sql.length - 1)
      }
      sql = `${sql} limit ${start},${count};`
    }

    let name = req.data.name

    // do query
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
        callback(message)

        return
      }
    }

    // if (rows && rows.worktime) {
    //   if (typeof rows.worktime !== 'boolean') this.sqlResultStore.splitData(rows.worktime)
    //   if (rows.name === 'three-credentials') {
    //     let overworktime = ((this.sqlResultStore.overWorktime - this.sqlResultStore.overChecktime) / this.sqlResultStore.overWorktime).toFixed(2) * 100
    //     rows['overview-worktime'] = [{worktime: overworktime}]
    //     rows['dept_worktime'] = Array.from(this.sqlResultStore.deptresult.values())
    //   } else if (rows.name === 'efficiency_overview') {
    //     rows['overview-worktime'] = Array.from(this.sqlResultStore.wholeMine.values())
    //     rows['dept_worktime'] = Array.from(this.sqlResultStore.deptEverydayWorktime.values())
    //   }
    //   rows['worktime'] = null
    // }
    // if (rows && rows.name === 'efficiency-detail') {
    //   rows['wholeworktime'] = Array.from(this.sqlResultStore.deptEverydayWorktime.values())
    // }
    // if (rows && rows.name === 'efficiency_manage') {
    //   // TODO:按时间筛选出来
    //   rows['dept_worktime'] = Array.from(this.sqlResultStore.deptEverydayWorktime.values())
    // }
    // if (name === 'worktime_dept_shift') {
    //   rows && this.sqlResultStore.splitData(rows)
    //   let time = req.data.time
    //   rows = this.sqlResultStore.getShiftDeptDay(time)
    // }
    // answer client
    message = {
      code: 0,
      msg: 'OK',
      data: rows,
      total: total,
      pageIndex: req.data.pageIndex
    }

    if (req.data.name === 'TrackList' || req.data.name === 'TrackData') {
      this.dealTrackListData(message)
    } else if (/^[0-9]{1,13}card$/ig.test(req.data.name)) {
      this.dealCardData(message)
    } else if (/^[0-9]{1,13}point$/ig.test(req.data.name)) {
      this.dealCardPoint(message)
    } else if (req.data.name === 'updatePath') {
      this.dealUpdatePathData(message)
    }
    callback(message)
  }

  sendDataWork (socket, req, callback) {
    if (req.data.name === 'worktime-shift') {
      let deptID = req.data.deptID
      let time = req.data.time
      let datas = this.sqlResultStore.deptEverydayShift && this.sqlResultStore.deptEverydayShift.get(deptID)
      datas = datas && datas.get(time)
      datas = datas && Array.from(datas.values())
      let message = {
        code: 0,
        msg: 'OK',
        data: datas
      }
      callback(message)
    }
  }

  dealData (item, msg, datas, name, nextpoint) {
    let beginTime = item.begin_time
    let endTime = item.last_time
    beginTime = new Date(beginTime).getTime()
    endTime = new Date(endTime).getTime()
    let timeDistance = 1000
    let speed = item.speed
    let coordinate = item.begin_pt
    coordinate = coordinate.split(',')
    nextpoint = nextpoint && nextpoint.split(',')
    // let bx = 'begin'
    // let by = 'begin'
    // if (nextpoint) {
    //   if (Number(coordinate[0]) < Number(nextpoint[0])) bx = 'next'
    //   if (Number(coordinate[1]) < Number(nextpoint[1])) by = 'next'
    // }
    let direction = item.direction
    for (let i = beginTime; i < endTime; i += timeDistance) {
      let cmsg = JSON.parse(JSON.stringify(msg))
      cmsg['cur_time'] = new Date(i).format('yyyy-MM-dd hh:mm:ss')
      let n = (i - beginTime) / timeDistance
      let x = Number(coordinate[0]) + n * speed * Math.cos(direction)
      let y = Number(coordinate[1]) + n * speed * Math.sin(direction)
      // if (nextpoint) {
      //   if ((bx === 'next' && x > Number(nextpoint[0])) || (bx === 'begin' && x < Number(nextpoint[0]))) x = Number(nextpoint[0])
      //   if ((by === 'next' && y > Number(nextpoint[1])) || (by === 'begin' && y < Number(nextpoint[1]))) y = Number(nextpoint[1])
      // }
      cmsg['x'] = Number(x.toFixed(2))
      cmsg['y'] = Number(y.toFixed(2))
      if (name === 'trackList') {
        if (i === beginTime) {
          cmsg['critical_point'] = 1
        } else {
          cmsg['critical_point'] = 0
        }
      }
      datas.push(cmsg)
    }
  }

  dealTrackListData (message, callback) {
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

  dealPathData (item, msg, datas, name) {
    let coordinate = item.begin_pt
    coordinate = coordinate.split(',')
    let cmsg = JSON.parse(JSON.stringify(msg))
    let x = Number(coordinate[0])
    let y = Number(coordinate[1])
    cmsg['x'] = Number(x.toFixed(2))
    cmsg['y'] = Number(y.toFixed(2))
    datas.push(cmsg)
  }

  dealUpdatePathData (message) {
    let rows = message.data
    let datas = []
    rows.forEach((item) => {
      let msg = {
        id: item.id,
        card_id: item.card_id,
        map_id: 5,
        speed: item.speed,
        last_time: item.last_time,
        landmark_id: item.landmark_id || 0,
        direction_mapper_id: item.direction_mapper_id || 0,
        landmark_dist: item.landmark_dist || 0
      }
      this.dealPathData(item, msg, datas, 'trackList')
    })
    if (datas.length > 0) message['data'] = datas
  }

  dealCardData (message) {
    let rows = message.data
    let datas = []
    rows.forEach((item) => {
      let beginTime = item.begin_time
      if (!beginTime) return
      let msg = {
        card_id: item.card_id
      }
      this.dealData(item, msg, datas)
    })
    if (datas.length > 0) message['data'] = datas
  }

  dealCardPoint (message) {
    let rows = message.data
    let datas = []
    rows.forEach((item) => {
      let beginTime = item.begin_time
      if (!beginTime) return
      let msg = {
        card_id: item.card_id,
        staff_id: item.staff_id
      }
      this.dealData(item, msg, datas)
    })
    if (datas.length > 0) message['data'] = datas
  }

  async composeData (socket, req, callback) {
    let message = null

    let mapID = req.data.mapID
    let startTime = req.data.startTime
    let endTime = req.data.endTime

    // let sqlForCard = `SELECT a.cur_time, SUM(a.nrow) nrow FROM(SELECT vd.cur_time, COUNT(vd.card_id) nrow FROM(SELECT hlv.cur_time,hlv.card_id FROM dat_vehicle dv ,his_location_vehicle hlv WHERE dv.vehicle_id=hlv.vehicle_id and hlv.map_id = ${mapID} && hlv.cur_time >= "${startTime}" && hlv.cur_time <= "${endTime}"
    // GROUP BY hlv.card_id) vd UNION SELECT res.cur_time, COUNT(res.card_id) nrow FROM(SELECT cur_time,card_id FROM dat_staff ds,his_location_staff hls WHERE ds.staff_id = hls.staff_id and hls.map_id = ${mapID} && hls.cur_time >="${startTime}" && hls.cur_time <= "${endTime}" GROUP BY hls.card_id) res) a`
    let sqlFotCardInArea = `SELECT a.area_id, SUM(a.card_count) card_count FROM(SELECT area_id, COUNT(DISTINCT card_id) card_count FROM dat_vehicle dv,his_location_vehicle hlv WHERE dv.vehicle_id = hlv.vehicle_id and hlv.map_id =${mapID} && hlv.cur_time >= "${startTime}" && hlv.cur_time <= "${endTime}" GROUP BY hlv.area_id UNION
    SELECT area_id, COUNT(DISTINCT card_id) card_count FROM dat_staff ds,his_location_staff hls WHERE ds.staff_id =hls.staff_id and hls.map_id = ${mapID} && hls.cur_time >= "${startTime}" && hls.cur_time <= "${endTime}" GROUP BY hls.area_id) a GROUP BY area_id`
    // let sqlFotCardInState = `SELECT a.state, SUM(a.card_count) FROM(SELECT hlv.state, COUNT(DISTINCT hlv.card_id) card_count FROM  dat_vehicle dv,his_location_vehicle hlv WHERE dv.vehicle_id =hlv.vehicle_id and hlv.map_id = ${mapID} && hlv.cur_time >= "${startTime}" && hlv.cur_time <= "${endTime}"
    // GROUP BY hlv.state UNION SELECT hls.state, COUNT(DISTINCT hls.card_id) card_count FROM dat_staff ds,his_location_staff hls WHERE  ds.staff_id=hls.staff_id and hls.map_id = ${mapID} && hls.cur_time >= "${startTime}" && hls.cur_time <= "${endTime}" GROUP BY hls.state) a GROUP BY a.state`
    // console.log('sqlForCard', sqlForCard)
    // console.log('sqlFotCardInArea', sqlFotCardInArea)
    // console.log('sqlFotCardInState', sqlFotCardInState)
    // let cardCount = null
    let cardInArea = null
    // let cardInState = null
    try {
      // cardCount = await mysql.query(sqlForCard)
      cardInArea = await mysql.query(sqlFotCardInArea)
      // cardInState = await mysql.query(sqlFotCardInState)
    } catch (err) {
      console.warn('查询 REPT DB 失败！ \n\t', err)
      message = {
        code: -1,
        msg: '查询失败！'
      }
      callback(message)

      return
    }
    // let nrow = 0
    // let curTime = 0
    // if (cardCount.length === 0) {
    //   nrow = cardCount[0].nrow
    //   curTime = cardCount[0].cur_time
    // } else {
    //   for (let i = 0; i < cardCount.length; i++) {
    //     nrow = nrow + cardCount[i].nrow
    //     curTime = cardCount[i].cur_time
    //   }
    // }
    let dataCompose = {
      // cardCount: nrow,
      // cur_time: curTime,
      cardInArea: cardInArea
      // cardInState: cardInState
    }

    message = {
      code: 0,
      msg: 'ok',
      data: dataCompose,
      total: 0,
      pageIndex: 0
    }
    callback(message)
  }

  async listData (socket, req, callback) {
    let message = null

    let startTime = req.data.startTime
    let endTime = req.data.endTime
    let sqlForBaseInfo = `select card_id, map_id from his_location where cur_time >= "${startTime}" && cur_time <= "${endTime}" group by card_id`

    let info = null
    try {
      info = await mysql.query(sqlForBaseInfo)
      for (let i = 0; i < info.length; i++) {
        let cardID = info[i].card_id
        let mapID = info[i].map_id
        let sqlForRealTimePoint = `select cur_time from his_location where card_id = "${cardID}" && map_id = "${mapID}" && cur_time >= "${startTime}" && cur_time <= "${endTime}"`
        let timeList = await mysql.query(sqlForRealTimePoint)
        info[i].startTime = timeList[0].cur_time
        info[i].endTime = timeList[timeList.length - 1].cur_time
      }
    } catch (err) {
      console.warn('查询 REPT DB 失败！ \n\t', err)
      message = {
        code: -1,
        msg: '查询失败！'
      }
      callback(message)

      return
    }

    // let trackList = info

    message = {
      code: 0,
      msg: 'ok',
      data: info,
      total: 0,
      pageIndex: 0
    }
    callback(message)
  }

  async dataForHistory (socket, req, callback) {
    let segementSize = 300 * 1000 // 5min
    let message = null
    let mapID = req.mapID
    let type = req.type
    let cards = req.cards
    let startTime = new Date(req.startTime).getTime()
    let endTime = new Date(req.endTime).getTime()
    // let startTimeString = req.startTime
    let endTimeString = req.endTime
    let startSegementIndex = req.startSegementIndex
    let segementCount = req.segementCount
    let segementOffset = req.segementOffset

    let duration = new Date(endTime) - new Date(startTime)
    let timePoint0 = null
    let timePoint1 = null
    let timePoint2 = null
    let timeStamp1 = null
    let sql = []
    if (type === 'scene') {
      if (segementOffset === 0) {
        timePoint0 = new Date(startTime + (startSegementIndex / segementCount) * duration).format('yyyy-MM-dd hh:mm:ss')
        timeStamp1 = new Date(timePoint0).getTime() + segementSize
        timePoint1 = new Date(timeStamp1).getTime() > endTime ? endTimeString : new Date(timeStamp1).format('yyyy-MM-dd hh:mm:ss')
        sql[0] = `SELECT *, LPAD(card_id, 13,0) AS cid FROM(SELECT hlv.card_id,hlv.cur_time,hlv.x,hlv.y,hlv.map_id,hlv.speed,hlv.state,hlv.mileage,hlv.landmark_id,hlv.direction_mapper_id,hlv.landmark_dist FROM his_location_vehicle hlv, dat_vehicle dv WHERE dv.vehicle_id=hlv.vehicle_id and hlv.map_id = ${mapID} && hlv.cur_time >= "${timePoint0}" && hlv.cur_time <= "${timePoint1}" UNION SELECT hls.card_id,hls.cur_time,hls.x,hls.y,hls.map_id,hls.speed,hls.state,hls.mileage,hls.landmark_id,hls.direction_mapper_id,hls.landmark_dist FROM his_location_staff hls,dat_staff ds WHERE hls.staff_id=ds.staff_id and hls.map_id = ${mapID} && hls.cur_time >= "${timePoint0}" && hls.cur_time <= "${timePoint1}") a ORDER BY cur_time`
      } else {
        timePoint0 = new Date(startTime + (startSegementIndex / segementCount) * duration).format('yyyy-MM-dd hh:mm:ss')
        timeStamp1 = new Date(timePoint0).getTime() + segementSize
        timePoint1 = new Date(timeStamp1).getTime() > endTime ? endTimeString : new Date(timeStamp1).format('yyyy-MM-dd hh:mm:ss')
        let timeStamp2 = new Date(timePoint1).getTime() + segementSize
        timePoint2 = new Date(timeStamp2).getTime() > endTime ? endTimeString : new Date(timeStamp2).format('yyyy-MM-dd hh:mm:ss')

        // console.log(typeof (startTime))
        // console.log(typeof (timePoint0))
        // console.log(typeof (timePoint1))
        // console.log(typeof (timePoint2))
        sql[0] = `SELECT *, LPAD(card_id, 13,0) AS cid FROM(SELECT hlv.card_id,hlv.cur_time,hlv.x,hlv.y,hlv.map_id,hlv.speed,hlv.state,hlv.mileage,hlv.landmark_id,hlv.direction_mapper_id,hlv.landmark_dist FROM his_location_vehicle hlv, dat_vehicle dv WHERE dv.vehicle_id=hlv.vehicle_id and hlv.map_id = ${mapID} && hlv.cur_time >= "${timePoint0}" && hlv.cur_time <= "${timePoint1}" UNION SELECT hls.card_id,hls.cur_time,hls.x,hls.y,hls.map_id,hls.speed,hls.state,hls.mileage,hls.landmark_id,hls.direction_mapper_id,hls.landmark_dist FROM his_location_staff hls,dat_staff ds WHERE hls.staff_id=ds.staff_id and hls.map_id = ${mapID} && hls.cur_time >= "${timePoint0}" && hls.cur_time <= "${timePoint1}") a ORDER BY cur_time`
        sql[1] = `SELECT *FROM(SELECT hlv.card_id,hlv.cur_time,hlv.x,hlv.y,hlv.map_id,hlv.speed,hlv.state,hlv.mileage,hlv.landmark_id,hlv.direction_mapper_id,hlv.landmark_dist FROM his_location_vehicle hlv,dat_vehicle dv WHERE hlv.vehicle_id = dv.vehicle_id and hlv.map_id = ${mapID} && hlv.cur_time >= "${timePoint0}" && hlv.cur_time <= "${timePoint1}" UNION SELECT hls.card_id,hls.cur_time,hls.x,hls.y,hls.map_id,hls.speed,hls.state,hls.mileage,hls.landmark_id,hls.direction_mapper_id,hls.landmark_dist FROM his_location_staff hls,dat_staff ds WHERE hls.staff_id = ds.staff_id and hls.map_id = ${mapID} && hls.cur_time >= "${timePoint0}" && hls.cur_time <= "${timePoint1}")a ORDER BY cur_time`
      }
    }

    // else if (type === 'track') {
    //   let sqlSubContent = null
    //   for (let i = 0; i < cards.length; i++) {
    //     if (i === 0) {
    //       sqlSubContent = 'card_id = ' + cards[i]
    //     } else {
    //       sqlSubContent = sqlSubContent + ' || card_id = ' + cards[i]
    //     }
    //   }
    //   sqlSubContent = sqlSubContent + ' &&'
    //   if (segementOffset === 0) {
    //     timePoint0 = new Date(startTime + (startSegementIndex / segementCount) * duration).format('yyyy-MM-dd hh:mm:ss')
    //     timePoint1 = new Date(new Date(timePoint0).getTime() + segementSize).getTime() > endTime ? endTimeString : new Date(new Date(timePoint0).getTime() + segementSize).format('yyyy-MM-dd hh:mm:ss')
    //     sql[0] = `select * from his_location where ${sqlSubContent} map_id = "${mapID}" && cur_time >= "${timePoint0}" && cur_time <= "${timePoint1}" order by cur_time`
    //   } else {
    //     timePoint0 = new Date(startTime + (startSegementIndex / segementCount) * duration).format('yyyy-MM-dd hh:mm:ss')
    //     timePoint1 = new Date(new Date(timePoint0).getTime() + segementSize).getTime() > endTime ? endTimeString : new Date(new Date(timePoint0).getTime() + segementSize).format('yyyy-MM-dd hh:mm:ss')
    //     timePoint2 = new Date(new Date(timePoint1).getTime() + segementSize).getTime() > endTime ? endTimeString : new Date(new Date(timePoint1).getTime() + segementSize).format('yyyy-MM-dd hh:mm:ss')
    //     sql[0] = `select * from his_location where ${sqlSubContent} map_id = "${mapID}" && cur_time >= "${timePoint0}" && cur_time <= "${timePoint1}" order by cur_time`
    //     sql[1] = `select * from his_location where ${sqlSubContent} map_id = "${mapID}" && cur_time >= "${timePoint1}" && cur_time <= "${timePoint2}" order by cur_time`
    //   }
    // }
    else {
      console.log('historyData type error: ' + type)
      message = {
        code: -1,
        msg: '请求类型错误！type = ' + type
      }
      callback(message)

      return
    }

    let rows = []
    try {
      let row = null
      for (let i = 0, len = sql.length; i < len; i++) {
        // console.log(sql[i])
        row = await mysql.query(sql[i])
        rows.push(row)
      }
    } catch (err) {
      console.warn('查询 REPT DB 失败！ \n\t', err)
      message = {
        code: -1,
        msg: '查询失败！'
      }
      callback(message)

      return
    }

    message = {
      code: 0,
      msg: 'ok',
      cmd: req.cmd,
      data: rows
    }
    callback(message)
  }
  getLocaleTime (str) {
    let time = new Date(str)
    time = time.getTime() - 8 * 60 * 60 * 1000
    return new Date(time).format('yyyy-MM-dd hh:mm:ss')
  }
}
