import { setInterval } from 'core-js/library/web/timers'
const cluster = require('cluster')
const numCPUs = require('os').cpus().length

if (cluster.isMaster) {
  console.log(`主进程 ${process.pid} 正在运行`)

    // 衍生工作进程。
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork()
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`工作进程 ${worker.process.pid} 已退出`)
  })
} else {
  setInterval(function () {
    // cardStore.handUpdatescards.clear()
    console.log(`工作进程 ${process.pid} 已启动`)
  }, 1000)

  // console.log(`工作进程 ${process.pid} 已启动`)
}
