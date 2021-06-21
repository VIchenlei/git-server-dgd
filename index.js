// // for pro : built version, use node to start
// let config = require('./bin/config.js')
// config.FileDir.root = __dirname || '/'
//
// let server = require('./bin/main.js')
// server.start()

// for dev : srouce version, use babel-node to start
let config = require('./src/config/appconfig.js')
config.FileDir.root = __dirname || '/'

let server = require('./src/main.js')
server.start()
