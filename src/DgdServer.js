import net from 'net'
import { CARD } from './state.js'
import { trunByte } from './reader_command_word.js'

const PORT = 3535

const POWER = {
    0: 100,
    1: 50,
    2: 20
}

export default class DgdServer {
    constructor () {
        this.serverStarted = false
        this.dgdSocket = null
        this.startServer()
    }

    getTurnByteResult (msg) {
        let result = Buffer.from([])
        for (let key in msg) {
            const r = trunByte(key, msg[key])
            result = Buffer.concat([result, r])
        }
        return result
    }

    sendData (datas) {
        if (!datas) return
        const vDetail = datas.v.detail
        let buf = Buffer.from([])
        for (let i = 0; i < vDetail.length; i++) {
            const v = vDetail[i]
            const cardID = v[CARD.card_id]
            const x = v[CARD.x] * 2 * 100
            const y = -v[CARD.y] * 2 * 100
            const charge = POWER[v[CARD.state_card]]
            const msg = {cardID: Number(cardID.slice(3)), x, y, z: 0, charge, rssi: 100}
            const result = this.getTurnByteResult(msg)
            buf = Buffer.concat([buf, result])
        }
        const length = 2 + buf.length
        const lengthBuf = Buffer.from([(length >> 8) & 0xFF, length & 0xFF])
        const resultData = Buffer.concat([lengthBuf, Buffer.from([0xa0, 0xa0]), buf])
        try {
            this.dgdSocket.write(resultData)
            console.log('发送定位数据：', resultData)
        } catch (err) {
            console.warn(err)
        }
    }

    // 启动TCP服务
    startServer () {
        if (this.serverStarted) return

        const self = this
        const server = net.createServer((socket) => {
            const client = socket.remoteAddress + ':' + socket.remotePort
            console.log('Connected to ' + client)
            self.serverStarted = true

            self.dgdSocket = socket

            socket.on('data', (data) => {

            })

            // 监听连接断开事件
            socket.on('end', function () {
                console.log('Client disconnected.')
            })

            socket.on('error', function (error) {
                console.log(error)
            })
        })

        server.listen(PORT, '0.0.0.0')

        server.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                this.startTcpServer = true
                console.warn(`The port ${PORT} is occupied, please change other port.`)
            }
        })
    }
}