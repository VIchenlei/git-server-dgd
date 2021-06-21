let path = require('path')
var readline = require('readline')
var fs = require('fs')
// var timer = require('timers')
// var os = require('os')

export default class TestPusher {
  constructor (filename, duration, socket) {
    this.filename = path.resolve(__dirname, filename)
    console.log('The path is : ' + this.filename)

    this.duration = duration
    this.socket = socket

    this.rl = null
  }

  start () {
    // var inputFileName = './data/in.json'
    var readStream = fs.createReadStream(this.filename)

    // var outputFileName = './data/out.json'
    // var writeStream = fs.createWriteStream(outputFileName)

    this.rl = readline.createInterface({
      input: readStream

      // 这是另一种复制方式，这样on('line')里就不必再调用writeStream.write(line)，当只是纯粹复制文件时推荐使用
      // 但文件末尾会多算一次index计数
      //  output: writeStream,
      //  terminal: true
    })

    var index = 0
    // let tpreview = performance.now()
    let tpreview = process.hrtime()

    let self = this

    this.rl.on('line', (line) => {
      if (line && line.trim() !== '') {
        self.socket.to('MONITOR').emit('PUSH', line)
        console.log('line', line)
        // this.socket.emit('PUSH', line)

        // if(index > 3) {
        //   this.rl.close()
        // }

        if (index % 1000 === 0) {
          let diff = process.hrtime(tpreview)
          let t = diff[0] * 1e9 + diff[1]

          // console.log('---- line : ' + index + '  last : ' + t + ' ns')
          // console.log(line)

          tpreview = process.hrtime()
        }

        // var tmp = 'line' + index.toString() + ':' + line
        // writeStream.write(tmp + os.EOL) // 下一行
        // console.log(index, line)
        index++

        self.rl.pause()
        setTimeout(() => {
          self.rl.resume()
          // console.log(self.duration)
        }, self.duration)
      }
    })

    this.rl.on('close', () => {
      console.log('readline closed. total line: ' + index)
    })
  }

  stop () {
    this.rl.close()
  }

  /**
 * 函数节流方法
 * @param Function fn 延时调用函数
 * @param Number delay 延迟多长时间
 * @param Number atleast 至少多长时间触发一次
 * @return Function 延迟执行的方法
 */
  throttle (fn, delay, atleast) {
    var timer = null
    var previous = null

    return function () {
      var now = +new Date()

      if (!previous) previous = now

      if (now - previous > atleast) {
        fn()

        // 重置上一次开始时间为本次结束时间
        previous = now
      } else {
        clearTimeout(timer)
        timer = setTimeout(() => {
          fn()
        }, delay)
      }
    }
  }
}
