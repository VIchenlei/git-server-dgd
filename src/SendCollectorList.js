export default class SendCollectorList {
  constructor () {
    this.collectorList = new Map()
  }

  storeList (msg) {
    const { data } = msg
    const { name, id, op_type } = data
    this.collectorList.set(`${name}_${id}_${op_type}`, data)
  }

  deleteList (msg) {
    const { name, id, op_type } = msg
    this.collectorList.delete(`${name}_${id}_${op_type}`)
  }
}
