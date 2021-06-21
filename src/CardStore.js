import {
  CARD,
  CARDAREAINFO
} from './state'
import {
  setInterval
} from 'core-js/library/web/timers'
import {
  clearTimeout
} from 'timers'
import {
  OD,
  ST
} from './odef.js'

const NOSIGNAL = 1024
const STAFFDISTANCE = 25
const VEHICLEDISTANCE = 100
// const STAFFSPEED = 10 // 上猴车速度
const MONKEYID = 7 // 上猴车
export default class CardStore {
  constructor () {
    // 井下车辆状态：cardID -> []
    this.vcards = new Map()

    // 井下人员状态：cardID -> []
    this.scards = new Map()

    this.sdiscards = new Map()
    this.vdiscards = new Map()

    // 井下人员无信号卡
    this.nosignalscars = new Map()

    // 客户端手动升井卡
    this.handUpdatescards = new Map()

    this.time = null
  }

  getscards () {
    return Array.from(this.sdiscards.values()).length
  }

  processDetail (data) {
    if (!data) return
    data = Array.from(data.values())
    for (let i = 0, len = data.length; i < len; i++) {
      let card = data[i]
      //   let cardID = card[CARD.card_id]
      this.setNosignalscards(card)
    }
  }

  // 存储丢失信号卡
  setNosignalscards (card) {
    let cardBiz = card[CARD.state_biz]
    if (cardBiz === NOSIGNAL) {
      let cardID = card[CARD.card_id]
      this.nosignalscars.set(cardID, card)
    }
  }

  getCood (card) {
    return {
      x: card[CARD.x],
      y: card[CARD.y]
    }
  }

  /**
   *
   * @param {本次卡的json} cData
   * @param {上次卡的json} bData
   * @param {人/车} type
   */
  checkoutCards (cData, bData, type) {
    let maps = {
      xmap: new Map(),
      dismap: new Map()
    }
    for (let i = 0; i < cData.length; i++) {
      let card = cData[i]
      let cardID = card[CARD.card_id]
      let bcard = bData && bData.get(cardID)
      if (bcard) {
        let newCood = this.getCood(card)
        let bCood = this.getCood(bcard)
        let distance = Math.pow((newCood.x - bCood.x), 2) + Math.pow((newCood.y - bCood.y), 2)
        if (type === 'staff') {
          // let speed = card[CARD.speed]
          let isMonkey = Number(card[CARD.state_object])
          // 0: 动画；1：定位
          if (isMonkey === MONKEYID || distance < STAFFDISTANCE) {
            card.push(0)
          } else {
            card.push(1)
          }
        } else {
          distance > VEHICLEDISTANCE ? card.push(1) : card.push(0) // 0: 动画；1：定位
        }
      } else {
        card.push(1) // 定位
      }
      maps.dismap.set(cardID, card)
      // if (type === 'staff') {
      let areaInfoArray = card[CARD.area_info_array]
      if (areaInfoArray.length > 0) {
        for (let i = 0; i < areaInfoArray.length; i++) {
          let cardarea = areaInfoArray[i]
          let areaID = cardarea[CARDAREAINFO.area_id]
          let cardCopy = [...card]
          let cardMark = `${cardID}-${areaID}`
          cardCopy.splice(14, 1)
          // cardCopy.unshift(cardMark)
          cardCopy.splice(4, 0, cardarea[CARDAREAINFO.enter_area_time])
          cardCopy.splice(8, 0, areaID)
          cardCopy.splice(14, 0, cardarea[CARDAREAINFO.mark_id], cardarea[CARDAREAINFO.mark_direction], cardarea[CARDAREAINFO.mark_distance])
          maps.xmap.set(cardMark, cardCopy)
        }
      } else {
        let cardCopy = card
        let cardMark = `${cardID}-0`
        cardCopy.splice(14, 1)
        cardCopy.splice(4, 0, 0)
        cardCopy.splice(8, 0, 0)
        cardCopy.splice(14, 0, 0, 0, 0)
        maps.xmap.set(cardMark, cardCopy)
      }
    }
    return maps
  }

  setOrAnimate (data) {
    let currentScards = data.s.detail
    let currentVcards = data.v.detail
    let beforeScards = this.sdiscards
    let beforeVcards = this.vdiscards
    let scards = currentScards && currentScards.length > 0 && this.checkoutCards(currentScards, beforeScards, 'staff')
    this.scards = scards && scards.xmap
    this.sdiscards = scards ? scards.dismap : this.sdiscards
    let vcards = currentVcards && this.checkoutCards(currentVcards, beforeVcards, 'vehicle')
    this.vcards = vcards && vcards.xmap
    this.vdiscards = vcards && vcards.dismap
  }

  // 处理卡移动数据
  cardMove (data) {
    if (!data) {
      return
    }
    this.setOrAnimate(data)
    data.s.detail = this.scards ? Array.from(this.scards.values()) : []
    data.v.detail = Array.from(this.vcards.values())
    // this.scards = data.s.detail
    // this.vcards = data.v.detail
    this.processDetail(this.scards)
  }

  // 存入手动升井卡列表
  setHandupdatescards (req) {
    let datas = req.data
    if (!datas) return
    for (let i = 0, len = datas.length; i < len; i++) {
      let data = datas[i]
      let cardID = data.cardid
      // 存入手动升井卡列表 并且 从丢失信号卡列表删除
      this.handUpdatescards.set(cardID, true)
      this.nosignalscars.delete(cardID)
    }
    if (Array.from(this.handUpdatescards.values()).length > 0) {
      this.startTime()
    }
  }

  deleteNosignalCards (data) {
    if (!data) return
    for (let i = 0, len = data.length; i < len; i++) {
      let card = data[i]
      let cardID = card[CARD.card_id]
      this.nosignalscars.delete(cardID) // 从丢失信号卡列表中删除
      this.handUpdatescards.delete(cardID) // 从手动升井卡列表中删除
    }
    if (Array.from(this.handUpdatescards.values()).length <= 0) {
      clearTimeout(this.time)
    }
  }

  startTime () {
    let timing = 30 * 60 * 1000
    this.time = setInterval(() => {
      this.handUpdatescards.clear()
    }, timing)
  }

  /**
   * 根据卡类型（vehicle, staff）获得对应的 map
   * @param {*} type
   */
  getStatesMapByCardType (type) {
    let xmap = null
    switch (type) {
      case OD.VEHICLE:
      case OD.CMJ:
      case OD.JJJ:
        xmap = this.vcards
        break
      case OD.STAFF:
        xmap = this.scards
        break
      default:
        console.log('UNKNOWN type:', type)
        return null
    }

    return xmap
  }

  /**
   * 获取（vehicle, staff）按照（area, dept, level）分类的某个类别（id）的明细
   * @param {*} cardType 卡类别，（vehicle, staff）
   * @param {*} groupBy 分类（area, dept, level）
   * @param {*} groupID 分类 ID（id）
   */
  getDetail (cardType, groupBy, groupID) {
    let fieldIndex = 0
    switch (groupBy) {
      case ST.AREA:
        fieldIndex = CARD.area_id
        break
      case ST.DEPT:
        fieldIndex = CARD.dept_id
        break
      case ST.LEVEL:
        fieldIndex = CARD.occupation_level_id
        break
      case ST.SUM:
        fieldIndex = -1 // ALL
        break
      default:
        // console.log('UNKNOWN groupBy: ', groupBy)
        return
    }

    let xmap = this.getStatesMapByCardType(cardType)
    let allCards = xmap && Array.from(xmap.values())
    let arrtriFilterCards = fieldIndex < 0 ? allCards : allCards.filter(item => item[fieldIndex] === groupID)
    return arrtriFilterCards
  }
}
