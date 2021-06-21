import crypto from 'crypto'
export default class Md5 {
  constructor () {
    this.md5 = crypto.createHash('md5')
    this.md5Store = new Map()
  }

  aesEncrypt (username, userpwd, key = 'password') {
    let data = `${username}+${userpwd}`
    const cipher = crypto.createCipher('aes192', key)
    var crypted = cipher.update(data, 'utf8', 'hex')
    crypted += cipher.final('hex')
    this.md5Store.set(crypted, true)
    return crypted
  }

  aesDecrypt (encrypted, key = 'password') {
    if (this.md5Store.get(encrypted)) {
      const decipher = crypto.createDecipher('aes192', key)
      var decrypted = decipher.update(encrypted, 'hex', 'utf8')
      decrypted += decipher.final('utf8')
      return decrypted
    }
  }
}
