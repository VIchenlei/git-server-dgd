import net from 'net'
import {READER_COMMAND_WORD, NumberTurnBuf, handleUnIPBuffer, handleBuffer, parsingData, getCrc} from './reader_command_word.js'
import Logger from './Logger.js'
import Pusher from './Pusher.js'

const fs = require('fs')
const path = require('path')
const config = require('./config/appconfig.js')
const binPath = config['FileDir']['bin']
const binFilePath = path.resolve(binPath)

const PORT = 6000
const FRAME_LENGTH = 1000
const uploadCmds = ['start_send_device_update_response', 'non_ip_start_send_device_update_response', 'wireless_start_send_device_update_response']

// const endUploadCmds = ['end_send_device_update', 'end_send_bigsmall_reader_update', 'wireless_end_send_bigsmall_reader_update']

const cardUploadType = [145, 146, 148, 149]

export default class Manager {
  constructor (metaStore) {
    this.name = null
    this.startTcpServer = false
    // this.cmdIPStore = {}
    this.tcpLink = new Map() // 存储tcp连接
    this.uploadFiles = new Map() // 存储升级文件
    this.fsData = null // 暂存升级文件的内容，注意用完之后要清空
    this.upLoadDown = false // 升级文件状态，注意用完之后要清空
    this.updateDeviceMsg = null // 升级文件信息
    // 通知设备启动连接的IP和端口
    this.port = null
    // 存储同一IP地址的设备
    this.distinctIPDevice = new Map()
    // 消息帧定时器
    this.timer = null
    // 记录重发次数
    this.count = 0
    // 升级程序帧时是否收到回复消息
    this.isReceiveMsg = false
    this.metaStore = metaStore
  }

  dispatch (socket, req) {
    let cmd = req.cmd
    let data = req.data
    this.socket = socket
    this.name = cmd
    switch (cmd) {
      case 'device_net_params':
      case 'device_params':
      case 'network_configuration':
      case 'device_configuration':
      case 'device_software_update':
      case 'start_send_device_update':
      case 'non_ip_start_send_device_update':
      case 'wireless_start_send_device_update':
      case 'power_limit':
        this.getConnect(data, cmd, socket)
        break
      case 'send_device_update_frame':
      case 'real_time_debug':
        this.startUpServer(data, cmd, socket)
        break
    }
  }

  clearTcpLink () {
    let tcpLinks = Array.from(this.tcpLink.values())
    if (tcpLinks.length > 0) {
      tcpLinks.forEach(tcpLink => tcpLink.end())
    }
    this.tcpLink.clear()
  }

  splitIP (data) {
    let {start, end} = data
    let ip = start.split('.')
    let commonIP = `${ip[0]}.${ip[1]}.${ip[2]}`
    let startIPAddress = ip[3], endIPAddress = end.split('.')[3]
    return {commonIP, startIPAddress, endIPAddress}
  }

  readFile ({value, file, fsdata, self}) {
    return new Promise((resolve, reject) => {
      fs.readFile(`${binPath}/${value}`, function (error, data) {
        if (error) {
          reject(error)
        } else {
          // 验证文件内容是否符合升级条件
          // resolve(self.dealFsData(data, fsdata, value))
          const { deviceType, programVersion } = fsdata
          // YA_DEVICE:KJ490,deviceType version,HIGH/LOW
          const yaDeviceIndex = data.indexOf('YA-DEVICE')
          const cutData = data.slice(yaDeviceIndex)
          const lineIndex = cutData.indexOf('\n')
          const basicDeviceMsg = data.slice(yaDeviceIndex, yaDeviceIndex + lineIndex)
          const commaIndex = basicDeviceMsg.indexOf(',')
          const basicDevice = basicDeviceMsg.slice(commaIndex + 1).toString()
          const basicDeviceArray = basicDevice.split(' ')
          const fileDeviceType = parseInt(basicDeviceArray[0])
          const fileProgramVersion = `${parseInt(`0x${basicDeviceArray[1].slice(2, 4)}`)}.${parseInt(`0x${basicDeviceArray[1].slice(4)}`).toString().padStart(3, 0)}`
          console.log('文件版本号：', fileProgramVersion)
          console.log('deviceType:', deviceType)
          console.log('fileDeviceType', fileDeviceType)
          const fileHL = parseInt(basicDeviceArray[2]) === 1 ? 'low' : 'high'
          if (cardUploadType.includes(deviceType)) {
            // 标识卡升级：只有设备类型和版本号都一致时，才不发送升级程序帧
            if (deviceType === fileDeviceType && programVersion == fileProgramVersion) {
              resolve({
                code: -1,
                msg: '该设备当前设备已是最新版本！',
                version: fileProgramVersion
              })
            } else {
              resolve({
                code: 0,
                file: `${fileHL}-${value}`,
                version: fileProgramVersion
              })
            }
          } else if (deviceType !== fileDeviceType || programVersion == fileProgramVersion) {
            resolve({
              code: -1,
              msg: deviceType !== fileDeviceType ? '升级文件与设备不符！' : '当前设备已是最新版本！',
              version: fileProgramVersion
            })
          } else {
            resolve({
              code: 0,
              file: `${fileHL}-${value}`,
              version: fileProgramVersion
            })
            // resolve(`${fileHL}-${value}`)
          }
        }
      })
    })
  }

  asyncReadFile (value, index) {
    if (this.upLoadDown && !value) {
      return {
        sendData: null
      }
    }
    if (!this.fsdata) {
      try {
        this.fsData = fs.readFileSync(`${binPath}/${value}`)
        this.upLoadDown = false
      } catch (error) {
        // console.warn(error)
      }
    }
    index = index || 1
    let end = index * FRAME_LENGTH
    let start = end - FRAME_LENGTH
    let fsDataNums = null
    if (index === 1) {
      fsDataNums = Math.ceil(this.fsData.length / FRAME_LENGTH)
    }
    let sliceData = this.fsData.slice(start, end)
    let sendData = Buffer.concat([Buffer.from([index]), sliceData])
    if (end >= this.fsData.length) {
      this.upLoadDown = true
    }
    return {sendData: sendData, index: index++, nums: fsDataNums}
  }

  sendDeviceUploadFS (data) {
    const {deviceAddress, deviceType, highorLow} = data
    const file = `${deviceAddress}-${deviceType}`
    // 1：低区，发送高区文件；2：高区，发送低区文件
    let fileHL = highorLow === 1 ? 'high' : 'low'
    const files = this.uploadFiles.get(file)
    for (let file in files) {
      const value = files[file]
      // 标识卡升级只有一个文件，不区分高低区
      if (value.includes(fileHL) || (value && cardUploadType.includes(deviceType))) {
        console.log('升级文件：', value)
        const ovalue = value.replace(`${fileHL}-`, '')
        let fsResult = this.asyncReadFile(ovalue)
        return {fileHL: fileHL === 'high' ? 1 : 2, fsResult, nums: fsResult.nums}
      }
    }
    fileHL = fileHL === 'high' ? 'low' : 'high'
    let value = Object.values(files)[0]
    if (!value) return null
    let ovalue = value.replace(`${fileHL}-`, '')
    let fsResult = this.asyncReadFile(ovalue)
    return {fileHL: fileHL === 'high' ? 1 : 2, fsResult, nums: fsResult.nums}
  }

  async asyncFsRsult (value, fsResult, file, data, self) {
    fsResult = await this.readFile({value, file, fsdata: data, self})
    // code === -1，说明校验错误
    let {code, file: parsingFile, version} = fsResult
    if (code === -1) {
      return {code, fsResult, version}
    }
    // files[file] = fsResult
    return {code, parsingFile, version}
  }

  async judgmentFile (data, self, socket) {
    const {deviceAddress, deviceType} = data
    const files = self.uploadFiles.get(`${deviceAddress}-${deviceType}`)

    let code = 0
    let fsResult = null
    let version = null
    if (cardUploadType.includes(deviceType)) {
      const value = files.firstFile
      const result = await this.asyncFsRsult(value, fsResult, 'firstFile', data, self, files)
      code = result.code
      fsResult = result.parsingFile
      version = result.version
      files.firstFile = fsResult
    } else {
      for (let file in files) {
        const value = files[file]
        const result = await this.asyncFsRsult(value, fsResult, file, data, self, files)
        code = result.code
        version = result.version
        fsResult = result.parsingFile
        if (code === -1) break
        files[file] = fsResult
      }
    }
    return {
      code: code,
      msg: fsResult,
      version
    }
  }

  dealParsingResult (parsingResult, cdata, cmd, io, self) {
    if (cmd === 'real_time_debug') {
      const {deviceAddress} = parsingResult.data
      const {reader_id} = cdata
      if (deviceAddress == reader_id) {
        self.responceClient(parsingResult, io)
      }
    }
  }

  responceClient (message, socket) {
    if (socket) {
      console.log('发送client数据：', message)
    }
    socket && socket.emit('MANAGER_RESPONCE', message)
  }

  clearTimer (self) {
    if (self && self.timer) {
      clearTimeout(self.timer)
    }
    self && (self.timer = null)
  }

  startTimer (self, socket, sendMsg, io) {
    if (self.isReceiveMsg) return self.clearTimer(self)

    if (self.count > 3) {
      self.count = 0
      return self.responceClient({
        cmd: 'device_upload_down',
        data: {
          code: -1,
          msg: '连接超时，升级失败！'
        }
      }, io)
    }

    self.clearTimer(self)

    self.timer = setTimeout(() => {
      socket.write(sendMsg)
      console.log('重发升级信息', sendMsg)
      self.count++
      self.startTimer(self, socket, sendMsg, io)
    }, 4000)
  }

  sendUpdateFrame (cmd, sendCmd, socket, self, fsResult, endSend, io) {
    if (cmd.includes('non_ip')) sendCmd = `non_ip_${sendCmd}`
    if (cmd.includes('wireless')) sendCmd = `wireless_${sendCmd}`

    let sendMsg = handleBuffer(READER_COMMAND_WORD[sendCmd], sendCmd, fsResult, endSend)
    socket.write(sendMsg)
    self.isReceiveMsg = false
    sendCmd !== 'wireless_end_send_bigsmall_reader_update' && self.startTimer(self, socket, sendMsg, io)
    return sendMsg
  }

  // 设备升级，发送第一帧数据
  sendDeviceUpdateFirstFrame (cmd, self, socket, io) {
    let {deviceAddress, deviceType, highorLow, version, code} = self.updateDeviceMsg
    if (code === -1) {
      return self.sendEndUpdateFrame(io, version, cmd, deviceAddress, deviceType, self, socket)
    }
    let fsResultData = self.sendDeviceUploadFS({deviceAddress, deviceType, highorLow})
    if (fsResultData) {
      highorLow = fsResultData.fileHL
      let fsResult = fsResultData.fsResult
      let sendCmd = 'send_device_update_frame'
      let sendMsg = self.sendUpdateFrame(cmd, sendCmd, socket, self, fsResult, null, io)

      self.responceClient({
        cmd: 'device_uploading',
        data: {
          cur_num: 1,
          total: fsResult.nums
        }
      }, io)
      console.log('发送数据', sendMsg)
      return highorLow
    }
  }

  getBasicEndSend (deviceAddress, deviceType, version) {
    const deviceAddressBuf = NumberTurnBuf(deviceAddress, 0, 4)
    const deviceTypeBuf = NumberTurnBuf(deviceType, 0, 1)
    const [parsingVersion1, parsingVersion2] = version.split('.')
    const versionBuf1 = NumberTurnBuf(parseInt(parsingVersion1), 0, 1)
    const versionBuf2 = NumberTurnBuf(parseInt(parsingVersion2), 0, 1)
    return Buffer.concat([deviceAddressBuf, deviceTypeBuf, versionBuf1, versionBuf2])
  }

  // 升级失败，直接发送结束帧
  sendEndUpdateFrame (io, version, cmd, deviceAddress, deviceType, self, socket) {
    let sendCmd = 'end_send_bigsmall_reader_update'
    const basicEndSend = self.getBasicEndSend(deviceAddress, deviceType, version)
    const endSend = Buffer.concat([basicEndSend, Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00])])
    let sendMsg = self.sendUpdateFrame(cmd, sendCmd, socket, self, null, endSend, io)
    console.log('升级失败：', sendMsg)
  }

  // 结束升级，需要发送目标设备id，目标设备类型，升级版本号、升级包字节数、升级文件CRC
  getEndSend (deviceAddress, deviceType, version, self) {
    const basicEndSend = self.getBasicEndSend(deviceAddress, deviceType, version)
    const codeLength = self.fsData.length
    const codeLengthBuf = NumberTurnBuf(codeLength, 0, 3)
    const crc = getCrc(self.fsData, codeLength)
    let crcData = [(crc >> 8) & 0xFF, crc & 0xFF]
    let crcBuffer = Buffer.from(crcData)
    return Buffer.concat([basicEndSend, codeLengthBuf, crcBuffer])
  }

  // 发送剩余程序帧直到发送完毕
  sendDeviceUpdateFrame (cmd, data, self, socket, io, newHighorLow) {
    let {deviceAddress, deviceType, version} = self.updateDeviceMsg || {}
    let fsResult = self.asyncReadFile(null, +data[0] + 1)
    let sendCmd = fsResult.sendData ? 'send_device_update_frame' : 'end_send_bigsmall_reader_update'
    let endSend = null
    if (sendCmd === 'end_send_bigsmall_reader_update') {
      endSend = self.getEndSend(deviceAddress, deviceType, version, self)
    }
    let sendMsg = self.sendUpdateFrame(cmd, sendCmd, socket, self, fsResult, endSend, io)

    console.log(`第${data[0] + 1}`, sendMsg)
    self.responceClient({
      cmd: 'device_uploading',
      data: {
        cur_num: data[0] + 1,
        total: fsResult.nums
      }
    }, io)
  }

  endSendUpload (parsingResult, socket, self) {
    const {deviceAddress, deviceType, programVersion, upload_state} = parsingResult.data
    const {version} = self.updateDeviceMsg || {}
    self.fsData = null
    self.responceClient({
      cmd: 'device_upload_down',
      data: {
        code: upload_state === 85 ? 0 : -1,
        deviceAddress,
        deviceType,
        programVersion,
        msg: programVersion == version ? '版本一致，升级失败' : ''
      }
    }, socket)
  }

  // 启动tcp服务端
  startUpServer (cmd, io) {
    let self = this
    if (this.startTcpServer) return

    // this.startTcpServer = true
    let server = net.createServer(function (socket) {
      var client = socket.remoteAddress + ':' + socket.remotePort
      console.log('Connected to ' + client)
      self.startTcpServer = true
      // 连接成功，发送第一帧数据，返回当前升级的高低位
      let highorLow = cmd === 'power_alarm' ? null : self.sendDeviceUpdateFirstFrame(cmd, self, socket, io)
      // 监听数据接收事件
      socket.on('data', async function (ondata) {
        let parsingResult = parsingData(ondata, self.name)
        console.log('服务端接收到数据', ondata)
        if (parsingResult) {
          let {cmd, data} = parsingResult
          if (cmd.includes('send_device_update_frame')) {
            self.sendDeviceUpdateFrame(cmd, data, self, socket, io, highorLow)
          } else if (cmd.includes('end_send') && self.upLoadDown) {
            self.clearTimer(self)
            self.count = 0
            self.isReceiveMsg = true
            self.endSendUpload(parsingResult, io, self)
          }
        }
      })

      // 监听连接断开事件
      socket.on('end', function () {
        console.log('Client disconnected.')
      })

      socket.on('error', function (error) {
        console.log(error)
      })
    })
    server.listen(9300, '0.0.0.0')

    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE') {
        this.startTcpServer = true
        console.warn(`The port 9300 is occupied, please change other port.`)
      }
    })
  }

  storeCmdIPMessage (parsingResult, connectip, self) {
    let {cmd, data} = parsingResult
    let store = self.cmdIPStore[cmd]
    if (!store) self.cmdIPStore[cmd] = new Map()
    store = self.cmdIPStore[cmd]
    let ret = store.get(connectip)
    if (!ret) {
      store.set(connectip, {})
      ret = store.get(connectip)
    }
    let {deviceType, ip, port, deviceAddress} = data
    if (deviceType === 1) {
      ret = {ip, port, deviceAddress, deviceType}
    }
    let {deviceAddressArray} = ret
    if (!deviceAddressArray) {
      deviceAddressArray = []
      ret['deviceAddressArray'] = deviceAddressArray
    }
    !deviceAddressArray.includes(`${deviceAddress}-${deviceType}`) && deviceAddressArray.push(`${deviceAddress}-${deviceType}`)
  }

  storeUploadFile (cdata) {
    const {deviceAddress, deviceType, firstFile, secondFile} = cdata
    if (!firstFile) return
    this.uploadFiles.set(`${deviceAddress}-${deviceType}`, {firstFile, secondFile})
  }

  // 存储不同IP对应的设备
  distinctDevice (parsingResult, ip, self) {
    let {cmd, data} = parsingResult
    let {deviceAddress, deviceType} = data
    // if (deviceType !== 1) return
    if (cmd && cmd.includes('non_ip')) return
    let ret = Array.from(self.distinctIPDevice.values())
    let index = ret.findIndex(item => item.deviceAddress === deviceAddress && item.deviceType === deviceType)
    if (index) {
      let keys = Array.from(self.distinctIPDevice.keys())
      let curKey = keys[index]
      self.distinctIPDevice.delete(curKey)
    }

    self.distinctIPDevice.set(ip, {ipDeviceAddress: deviceAddress, ipDeviceType: deviceType})
  }

  // 启动tcp客户端
  getConnect (cdata, cmd, socket) {
    let self = this
    let {nets, port} = cdata
    // 通知设备启动连接的IP和端口
    this.port = port
    // let {commonIP, startIPAddress, endIPAddress} = this.splitIP(data)
    let commandWord = READER_COMMAND_WORD[cmd]
    for (let i = 0; i < nets.length; i++) {
      let tcpClient = new net.Socket()
      let ip = nets[i]
      let originData = cdata
      if (this.tcpLink.get(ip)) {
        this.tcpLink.get(ip).end()
        this.tcpLink.delete(ip)
      }
      if (cdata.fixeds) {
        // let curData = cdata.fixeds.find(item => item.ip === ip)
        let curData = cdata.fixeds[i]
        curData['data'] = cdata.data
        originData = curData
      }
      cmd.includes('start_send_device_update') && this.storeUploadFile(cdata)

      try {
        // 创建客户端
        tcpClient.connect(PORT, ip, function () {
          let sendMsg = handleBuffer(commandWord, cmd, originData)
          let {sendMag, resultBuffer} = handleUnIPBuffer(commandWord, cmd, originData, sendMsg, self.distinctIPDevice, ip)
          sendMag && tcpClient.write(sendMag) // 带IP设备
          resultBuffer && tcpClient.write(resultBuffer) // 不带IP设备
          console.log('发出的消息', sendMag, resultBuffer)
          // sendMag && Logger.log4('发送的消息', sendMag)
          // resultBuffer && Logger.log4('发出的消息', resultBuffer)
          self.tcpLink.set(ip, tcpClient)
        })

        tcpClient.on('data', async (msg) => {
          console.log('接收到消息', msg)
          // Logger.log4(`接收到的消息`, msg)
          let parsingResult = parsingData(msg, self.name)
          if (!parsingResult) return
          self.distinctDevice(parsingResult, ip, self)
          let {cmd, data} = parsingResult
          // 开始升级文件命令
          if (parsingResult && uploadCmds.includes(parsingResult.cmd)) {
            if (cmd.includes('start_send_device_update_response')) {
              let result = await self.judgmentFile(data, self, socket)
              let { code, deviceType, version } = result
              self.updateDeviceMsg = data
              self.updateDeviceMsg['version'] = version
              self.updateDeviceMsg['code'] = code

              if (cmd.includes('non_ip') || cmd.includes('wireless')) { // 非IP设备，发送升级程序帧作为客户端发送
                self.sendDeviceUpdateFirstFrame(`${cmd.includes('non_ip') ? 'non_ip' : 'wireless'}_send_device_update_frame`, self, tcpClient, socket)
              } else { // IP设备发送程序帧，作为服务端发送
                self.startUpServer('send_device_update_frame', socket)
              }
            }
          } else if (parsingResult && parsingResult.cmd.includes('send_device_update_frame')) {
            // 非IP设备发送后续程序帧
            // 接收到消息后，先清空定时器，将count重置为0
            self.clearTimer(self)
            self.count = 0
            self.isReceiveMsg = true
            self.sendDeviceUpdateFrame(cmd, data, self, tcpClient, socket)
          } else if (parsingResult && parsingResult.cmd.includes('end_send')) {
            self.clearTimer(self)
            self.count = 0
            self.isReceiveMsg = true
            self.endSendUpload(parsingResult, socket, self)
          } else {
            parsingResult && (parsingResult.data['ip'] = ip)
            !cmd.includes('non_ip') && (parsingResult.data['isIP'] = true)
            self.responceClient(parsingResult, socket, self)
          }
          // tcpClient.end() // 返回client数据后，关闭连接
          // self.tcpLink.delete(ip)
        })

        tcpClient.on('error', error => {
          console.warn(`连接失败${ip}:${PORT}:${error}`)
          tcpClient.end()
          self.tcpLink.delete(ip)
          // self.responceClient({
          //   cmd: 'device_error',
          //   data: {
          //     code: -1,
          //     msg: '连接失败，请重试！'
          //   }
          // }, socket)
        })
      } catch (error) {
        console.warn(error)
      }
    }
  }
}
