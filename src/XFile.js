import Utils from './Utils.js'
import File from './FileWraper.js'
import mysql from './MysqlWraper.js'

let fs = require('fs')
let path = require('path')
let crypto = require('crypto')
let config = require('./config/appconfig.js')
let metaDefinition = require('./meta_definition.js')

export default class XFiler {
  constructor (meta) {
    this.meta = meta
    this.files = {}
  }

  dispatch (socket, req) {
    if (!socket || !req) return

    switch (req.cmd) {
      case 'download':
        this.download(socket, req)
        break
      case 'upload':
        this.upload(socket, req)
        break
      case 'delete-pic':
        this.deletePic(socket, req)
        break
      default:
        console.warn(`未知的 FILE 请求：${req.cmd}`)
        break
    }
  }

  async download (socket, req) {
    let resMsg = null

    let fileName = req.data.name
    let fileType = req.data.type // map, staff, vehicle
            // console.log('config.FileDir.root : ', config.FileDir.root)
            // let uri = `${config.FileDir[fileType]}/${fileName}`
    let uri = path.resolve(config.FileDir[fileType], `./${fileName}`)
    let fdata = null
    try {
      fdata = await File.readFile(uri)
    } catch (err) {
      console.warn(`获取文件${uri}失败！\n\t${err}`)

      resMsg = {
        cmd: 'download',
        code: -1,
        msg: '获取文件失败，请联系系统管理员。',
        data: {
          name: fileName
        }
      }
      socket.emit('FILE', resMsg)
      return false
    }

    let sdata = fdata.toString()
    resMsg = {
      cmd: 'download',
      code: 0,
      msg: 'OK',
      data: {
        name: fileName,
        data: sdata
      }
    }

    socket.emit('FILE', resMsg)
  }

    // ---- upload ----

  upload (socket, req) {
    let cmd = req.data.op
    let data = Utils.toJson(req.data)
    if (!data) {
      console.warn('Invalid request.', data)
      return
    }

    switch (cmd) {
      case 'start':
        this.startUpload(socket, data)
        break
      case 'data':
        this.uploadData(socket, data)
        break
      default:
        console.warn(`上传文件，未知指令：${cmd}`)
        break
    }
  }

  async startUpload (socket, data) {
    // console.log(`Got upload_start Event : \n\t${JSON.stringify(data)}`)

    let name = data['name']
    this.files[name] = { // Create a new Entry in The this.files Variable
      type: data['type'], // map, staff, vehicle
      size: data['size'],
      data: '',
      uploaded: 0
    }

    // let tempFileURI = `${config.FileDir.tmp}/${data['type']}/${name}`
    let tempFileURI = path.resolve(config.FileDir.tmp, `./${data['type']}/${name}`)
    try {
      let stat = fs.statSync(tempFileURI)
      if (stat.isFile()) {
        this.files[name]['uploaded'] = stat.size
      }
    } catch (err) { // It's a New File
            // console.log(`Error : ${err}`)
    }

    let fd = -1
    try {
      fd = await File.openFile(tempFileURI, 'a', parseInt('0755', 8))
    } catch (err) {
      console.warn(`打开文件 [${tempFileURI}] 失败！\n`, err)
    }

    this.files[name]['handler'] = fd // store the file handler so we can write to it later
    this.requestUploadMoreData(socket, name)
  }

  async uploadData (socket, data) {
        // console.log(`Got upload_data Event : ${data}`)
        // this.files = this.this.files

    let name = data['name']
    let type = this.files[name]['type']
    this.files[name]['uploaded'] += data['data'].length
    this.files[name]['data'] += data['data']
    if (this.files[name]['uploaded'] >= this.files[name]['size']) { // If File is Fully Uploaded
            // save the last part to temp file
      try {
        await File.writeFile(this.files[name]['handler'], this.files[name]['data'], null, 'Binary')
      } catch (err) {
        console.log('文件缓存存盘失败！\n', err)
        return
      }

            // copy temp file to resource dir & calculate the md5
            // let tempFileURI = `${config.FileDir.tmp}/${type}/${name}`
      let tempFileURI = path.resolve(config.FileDir.tmp, `./${type}/${name}`)
                // let fileURI = `${config.FileDir[type]}/${name}`
      let fileURI = path.resolve(config.FileDir[type], `./${name}`)

      let reader = fs.createReadStream(tempFileURI)
      let writer = fs.createWriteStream(fileURI)

      let hash = crypto.createHash('md5')
      reader.on('data', hash.update.bind(hash))

      reader.pipe(writer)
      reader.on('end', async() => {
        let md5 = hash.digest('hex')
        // console.log(`The file MD5 is : ${md5}`)

                // Deletes The Temporary File
        try {
          await File.deleteFile(tempFileURI)
        } catch (err) {
          console.warn('删除临时文件失败！请手工删除。\n', err)
        }

        socket.emit('FILE', {
          cmd: 'upload_done',
          data: {
            name: name,
            md5: md5
          }
        })
      })
    } else {
      if (this.files[name]['data'].length > config.FileBufferSize) { // If the data Buffer reaches 10MB
        try {
          await File.writeFile(this.files[name]['handler'], this.files[name]['data'], null, 'Binary')
        } catch (err) {
          console.log('文件缓存存盘失败！\n', err)
          return
        }
        this.files[name]['data'] = '' // Reset The Buffer
      }
      this.requestUploadMoreData(socket, name)
      this.uploadData(socket, data) // 数据大小大于一次上传的大小时，继续上传
    }
  }

  requestUploadMoreData (socket, name) {
    let place = this.files[name]['uploaded'] / config.ChunkSize // 已上传的chunk数，每个chunk的大小为 config.ChunkSize
    let percent = (this.files[name]['uploaded'] / this.files[name]['size']) * 100
    let message = {
      cmd: 'upload_more',
      data: {
        place: place,
        percent: percent
      }
    }
    socket.emit('FILE', message)
  }

  async deletePic (socket, req) {
    let data = req.data.pic
    let picpath = config['FileDir']['staff']
    fs.unlink(`${picpath}/${data}`, (err) => {
      if (err) console.log(err)
      console.log(`删除${data}成功`)
    })
    let keyValue = req.data.keyValue
    let sql = `UPDATE dat_staff SET pic='' WHERE staff_id=${keyValue}`
    let rows = null
    try {
      rows = await mysql.query(sql)
    } catch (err) {
      return
    }
    let resMsg = {
      code: 0,
      msg: sql,
      cmd: 'update',
      data: {
        op: 'DELETE',
        name: 'staff',
        id: req.data.keyValue
      },
      mdtdata: {
        timename: 'lastUpdate',
        time: new Date().format('yyyy-MM-dd hh:mm:ss')
      }
    }
    socket.emit('META', resMsg)

    this.meta.broadcastMetaData(socket, config.STANDBY, metaDefinition['staff'], 'delete')
  }
}
