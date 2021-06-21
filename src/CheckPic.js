import mysql from './MysqlWraper.js'
import Logger from './Logger.js'
let fs = require('fs')
let path = require('path')
let config = require('./config/appconfig.js')

export default class CheckPic {
  constructor () {
    this.picpath = config['FileDir']['staff']
    this.picFilePath = path.resolve(this.picpath)
  }

  async searchPic () {
    // let results = [this.picFilePath]
    let files = fs.readdirSync(this.picpath, 'utf-8')
    let sql = `SELECT staff_id, pic from dat_staff;`
    let rows = await mysql.query(sql)
    this.matchPic(files, rows)
  }

  matchPic (files, rows) {
    if (!rows || !files) return
    rows.forEach(async function (row) {
      let staffID = row.staff_id
      let pic = row.pic
      if (!files.includes(pic) || pic === 'undefined' || pic === 'null') {
        let filename = null
        let jpg = `${staffID}.jpg`
        let png = `${staffID}.png`
        let bmp = `${staffID}.bmp`
        if (files.includes(jpg)) {
          filename = jpg
        } else if (files.includes(png)) {
          filename = png
        } else if (files.includes(bmp)) {
          filename = bmp
        }
        if (filename) {
          let sql = `UPDATE dat_staff set pic = '${filename}' where staff_id = ${staffID};`
          let result = null
          try {
            result = await mysql.query(sql)
            // console.log(`更新员工${staffID}照片${filename}成功：${sql}`)
          } catch (err) {
            console.log(`更新照片失败：${err}`)
          }
        }
      }
    })
  }
}
