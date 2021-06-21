let crypto = require('crypto')

export default class Utils {
  constructor () { // eslint-disable-line
  }

  static md5 (str) {
    return crypto.createHash('md5').update(str).digest('hex').toUpperCase()
  }

  static sha1 (str) {
    return crypto.createHash('sha1').update(str).digest('hex').toUpperCase()
  }

  /**
   * Convert a string to JSON
   *
   * @method toJson
   *
   * @param  {[type]} data [description]
   *
   * @return {[type]}      [description]
   */
  static toJson (data) {
    if (typeof data === 'object') {
      return data
    }

    let ret = null
    if (data && (typeof data === 'string')) {
      try {
        ret = JSON.parse(data)
      } catch (error) {
        console.warn('Can NOT parse the input data to be JSON : ', data)
      }
    } else {
      console.warn('The input data\'s type is NOT string : ', data)
    }

    return ret
  }
}

/**
 * usage:
 * let x = new Date()
 * x.format('yyyy-MM-dd hh:mm:ss')
 *
 * @method format
 *
 * @param  {[type]} format [description]
 *
 * @return {[type]}        [description]
 */
Date.prototype.format = function (format) { // eslint-disable-line
  let o = {
    'M+': this.getMonth() + 1, // month
    'd+': this.getDate(), // day
    'h+': this.getHours(), // hour
    'm+': this.getMinutes(), // minute
    's+': this.getSeconds(), // second
    'q+': Math.floor((this.getMonth() + 3) / 3), // quarter
    'S': this.getMilliseconds() // millisecond
  }
  if (/(y+)/.test(format)) {
    format = format.replace(RegExp.$1, (this.getFullYear() + '').substr(4 - RegExp.$1.length))
  }
  for (let k in o) {
    if (new RegExp('(' + k + ')').test(format)) {
      format = format.replace(RegExp.$1,
        RegExp.$1.length === 1 ? o[k] : ('00' + o[k]).substr(('' + o[k]).length))
    }
  }
  return format
}
