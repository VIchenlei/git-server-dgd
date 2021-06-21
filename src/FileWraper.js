// FileWraper : 将文件操作封装成 promise ，用于 async / await 操作
let fs = require('fs')

export default class FileWraper {
  constructor () {  // eslint-disable-line
  }

  static async dirExist (path) {
    return new Promise((resolve, reject) => {
      fs.stat(path, (exists) => {
        if (!exists) {
          // console.log(`Directory do NOT exist.  ${path}`)
          // reject(new Error('NOT exist'))
          resolve(false)
        } else {
          // console.log(`Directory already exist.  ${path}`)
          resolve(true)
        }
      })
    })
  }

  static async makeDir (path) {
    return new Promise((resolve, reject) => {
      fs.mkdir(path, (err) => {
        if (err) {
          // console.warn(`MakeDir FAILED: ${path}`)
          // reject(new Error(err))
          resolve(false)
        } else {
          console.log(`MakeDir OK: ${path}`)
          resolve(true)
        }
      })
    })
  }

  static async readFile (uri) {
    return new Promise((resolve, reject) => {
      fs.readFile(uri, (err, data) => {
        if (err) {
          reject(new Error(err))
        } else {
          resolve(data)
        }
      })
    })
  }

  static async writeFile (fd, data, position, encoding) {
    return new Promise((resolve, reject) => {
      fs.write(fd, data, position, encoding, (err, written) => {
        if (err) {
          reject(new Error(err))
        } else {
          resolve(written)
        }
      })
    })
  }

  static async writeStream (path) {
    return new Promise((resolve, reject) => {
      fs.createWriteStream(path, (err, written) => {
        if (err) {
          reject(new Error(err))
        } else {
          resolve(written)
        }
      })
    })
  }

  static async deleteFile (uri) {
    return new Promise((resolve, reject) => {
      fs.unlink(uri, (err) => { // ??? fs.unlink 的回调函数没有 err 参数！！！
        if (err) {
          reject(new Error(err))
        } else {
          resolve()
        }
      })
    })
  }

  static async openFile (uri, flags, mode) {
    return new Promise((resolve, reject) => {
      fs.open(uri, flags, mode, (err, fd) => {
        if (err) {
          reject(new Error(err))
        } else {
          resolve(fd)
        }
      })
    })
  }
}
