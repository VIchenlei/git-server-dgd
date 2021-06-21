// the database config for pool
module.exports = {
  /*
    // for local db
    dev: {
      connectionLimit: 100, // important
      host: '127.0.0.1',
      port: '3306',
      user: 'root',
      password: 'Hello000',
      database: 'yaloc3',
      charset: 'utf8',
      debug: false
    }
  // */
/*
  // for remote db
  dev: {
    connectionLimit: 20, // important
    // host: '192.168.100.213',
    // host: '124.204.33.42:9702',
    host: '127.0.0.1',
    port: '3306',
    user: 'root',
    password: 'password',
    database: 'yaxt4',
    charset: 'utf8',
    debug: false
  },
  pro: {
    connectionLimit: 20, // important
    host: '127.0.0.1',
    port: '3306',
    user: 'root',
    password: 'password',
    database: 'yaxt4',
    charset: 'utf8',
    debug: false
  }
*/
  /*
  // 高河正式外网
    dev: {
      connectionLimit: 10, // important
    // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.
      // connectTimeout: 60 * 60 * 1000,
      // aquireTimeout: 60 * 60 * 1000,
      // timeout: 60 * 60 * 1000,
      host: '60.220.238.150',
      port: '16387',
      user: 'yadb',
      password: 'Yadb@2018#123',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    },
    pro: {
      connectionLimit: 10, // important
      host: '60.220.238.150',
      port: '16387',
      user: 'yadb',
      password: 'Yadb@2018#123',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    }
  */
  /*
  // 高河正式
    dev: {
      connectionLimit: 20, // important
      // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.
      // connectTimeout: 60 * 60 * 1000,
      // aquireTimeout: 60 * 60 * 1000,
      // timeout: 60 * 60 * 1000,
      host: '192.168.118.198',
      port: '3306',
      user: 'yadb',
      password: 'Yadb@2018#123',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    },
    pro: {
      connectionLimit: 20, // important
      host: '192.168.118.198',
      port: '3306',
      user: 'yadb',
      password: 'Yadb@2018#123',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    }
  */
  /*
  // 9000web
    dev: {
      connectionLimit: 10, // important
    // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.

      host: '192.168.118.199',
      port: '3306',
      user: 'root',
      password: 'password',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    },
    pro: {
      connectionLimit: 10, // important
      host: '192.168.118.199',
      port: '3306',
      user: 'root',
      password: 'password',
      database: 'yaxt',
      charset: 'utf8',
      debug: false
    }
  */
  // 测试
  /*
  dev: {
    connectionLimit: 20, // important
    // host: '192.168.100.213',
    // host: '124.204.33.42:9702',
    connectTimeout: 60 * 60 * 1000,
    aquireTimeout: 60 * 60 * 1000,
    timeout: 60 * 60 * 1000,
    host: '192.168.118.200',
    port: '3306',
    user: 'webapp',
    password: 'webgroup',
    database: 'yaxt',
    charset: 'utf8',
    debug: false
  },
  pro: {
    connectionLimit: 20, // important
    host: '192.168.118.200',
    port: '3306',
    user: 'webapp',
    password: 'webgroup',
    database: 'yaxt',
    charset: 'utf8',
    debug: false
  }
*/
/*
  dev: {
    connectionLimit: 20, // important
    // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.

    host: '60.220.238.150',
    port: '16387',
    user: 'yadb',
    password: 'Yadb@2018#123',
    database: 'yaxt4',
    charset: 'utf8',
    debug: false
  },
  pro: {
    connectionLimit: 20, // important
    host: '60.220.238.150',
    port: '16387',
    user: 'yadb',
    password: 'Yadb@2018#123',
    database: 'yaxt',
    charset: 'utf8',
    debug: false
  }
*/
  /*
  dev: {
    connectionLimit: 20, // important
  // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.

    host: '127.0.0.1',
    port: '23306',
    user: 'yadb',
    password: 'yadb@123',
    database: 'yaxt_yanshi',
    charset: 'utf8',
    debug: false
  },
  pro: {
    connectionLimit: 20, // important
    host: '127.0.0.1',
    port: '23306',
    user: 'yadb',
    password: 'yadb@123',
    database: 'yaxt_yanshi',
    charset: 'utf8',
    debug: false
  }
*/
  dev: {
    connectionLimit: 20, // important
    // waitForConnections: true, // If true, the pool will queue the connection request and call it when one becomes available.

    host: '192.168.0.240',
    port: '3306',
    user: 'yadb',
    password: 'yadb@123',
    database: 'yaxt_dgd',
    charset: 'utf8',
    debug: false
  },
  pro: {
    connectionLimit: 20, // important
    host: '192.168.0.240',
    port: '3306',
    user: 'yadb',
    password: 'yadb@123',
    database: 'yaxt_gaohe',
    charset: 'utf8',
    debug: false
  }
}

/*
In addition to passing these options as an object, you can also use a url string. For example:
var connection = mysql.createConnection('mysql://user:pass@host/db?debug=true&charset=BIG5_CHINESE_CI&timezone=-0700')
*/
