// MysqlWraper : 将 mysql 操作封装成 Promise，用于支持 async / await 调用
//
// Try mysql2 ???
// https://github.com/sidorares/node-mysql2

let mysql = require('mysql')
let dbconfig = require('./config/dbconfig')

let config = dbconfig.dev

// print pool debug info
function printDebugInfo (pool) {
    // when a connection created
  pool.on('connection', (connection) => {
    // console.log(`DBPool : A connection been created. total: ${pool._allConnections.length}, free : ${pool._freeConnections.length}`)

      // attache the error handler to the connection
    connection.on('error', (err) => {
      if (err.code === 'PROTOCOL_CONNECTION_LOST') {
        console.log(`xxxx DBConnection has been closed by remote.  total: ${pool._allConnections.length}, free : ${pool._freeConnections.length}\n`, err)
      } else {
        console.log('xxxx DBConnection error : \n', err) // 'ER_BAD_DB_ERROR'
      }
    })
  })

    // when a callback has been queued to wait for an available connection.
  pool.on('enqueue', () => {
    // console.warn(`>>>> DBPool : No free connections are available in pool. total: ${pool._allConnections.length}, free : ${pool._freeConnections.length}`)
  })

  pool.on('acquire', function (connection) {
      // console.log('Connection %d acquired', connection.threadId)
    // console.log(`>> DBPool : A connection been acquired. total: ${pool._allConnections.length}, acquired: ${pool._acquiringConnections.length}, free : ${pool._freeConnections.length}`)
  })

  pool.on('release', (connection) => {
      // console.log('Connection %d released', connection.threadId)
    // console.log(`<< DBPool : A connection been released. total: ${pool._allConnections.length}, free : ${pool._freeConnections.length}`)
  })
}

class MysqlWraper {
  constructor () {
    this.pool = this.createPool(config)
  }

  createPool (config) {
    let pool = mysql.createPool(config)

    // for debug
    if (config === dbconfig.dev) {
      printDebugInfo(pool)
    }

    return pool
  }

  async query (sql, values) {
    return new Promise((resolve, reject) => {
      this.pool.query(sql, values, (err, results, fields) => {
        if (err) {
          // console.warn('Excute sql error. \n', sql)
          reject(new Error(err))
        } else {
          resolve(results)
        }
      })
    })
  }

  close () {
    this.pool.end((err) => {
      // all connections in the pool have ended
      if (!err) {
        console.log('DB pool closed.')
      } else {
        console.warn('Close connection pool error', err)
      }
    })
  }
}

let db = new MysqlWraper()

export default db
