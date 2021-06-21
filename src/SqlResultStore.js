import { throws } from 'assert'

export default class SqlResultStore {
  constructor () {
    // 修改时间
    this.updateTime = null
    // 修改的月份
    this.updateMonth = null
    // 全矿总工时
    this.overWorktime = 0
    // 全矿总考勤
    this.overChecktime = 0
    // 按部门算工时
    this.deptresult = new Map()
    // 全矿每天工时
    this.wholeMine = new Map()
    // 部门每天工时
    this.deptEverydayWorktime = new Map()
    // 部门每天shift
    this.deptEverydayShift = new Map()
  }

  reset () {
    this.updateTime = new Date().getTime()
    this.overWorktime = 0
    this.overChecktime = 0
    this.deptresult.clear()
    this.wholeMine.clear()
    this.deptEverydayWorktime.clear()
    this.deptEverydayShift.clear()
  }

  splitData (rows) {
    this.reset()
    rows.forEach((row) => {
      let worktime = Number(row.worktime.toFixed(2))
      let dtime = row.dtime ? Number(row.dtime.toFixed(2)) : 0
      if (worktime >= dtime) {
        this.overWorktime += worktime
        this.overChecktime += dtime
        let deptID = row.dept_id
        if (!this.deptresult.get(deptID)) {
          this.deptresult.set(deptID, {
            dept_id: deptID,
            overWorktime: 0,
            overChecktime: 0
          })
        }
        let deptworktime = this.deptresult.get(deptID)
        deptworktime['overWorktime'] += worktime
        deptworktime['overChecktime'] += dtime
        let datetime = new Date(new Date(row.start_time).getTime()).format('yyyy-MM-dd')
        let shiftime = new Date(`${datetime} 21:00:00`).getTime()
        let startime = new Date(row.start_time).getTime()
        if (startime > shiftime) {
          let y = new Date(row.start_time).getFullYear()
          let m = new Date(row.start_time).getMonth() + 1
          let d = new Date(row.start_time).getDate() + 1
          row['stime'] = `${y}-${m}-${d}`
        }
        let stime = new Date(new Date(row.stime).getTime()).format('yyyy-MM-dd')
        if (!this.wholeMine.get(stime)) {
          this.wholeMine.set(stime, {
            stime: stime,
            overWorktime: 0,
            overChecktime: 0
          })
        }
        let wholeMine = this.wholeMine.get(stime)
        wholeMine['overWorktime'] += worktime
        wholeMine['overChecktime'] += dtime
        if (!this.deptEverydayWorktime.get(`${deptID}-${stime}`)) {
          this.deptEverydayWorktime.set(`${deptID}-${stime}`, {
            dept_id: deptID,
            overWorktime: 0,
            overChecktime: 0,
            stime: stime,
            num: 0
          })
        }
        let deptEverydayWorktime = this.deptEverydayWorktime.get(`${deptID}-${stime}`)
        deptEverydayWorktime['overWorktime'] += worktime
        deptEverydayWorktime['overChecktime'] += dtime
        deptEverydayWorktime['num'] += 1
        if (!this.deptEverydayShift.get(deptID)) {
          let ret = new Map()
          this.deptEverydayShift.set(deptID, ret)
        }
        let deptshift = this.deptEverydayShift.get(deptID)
        if (!deptshift.get(stime)) {
          let ret = new Map()
          deptshift.set(stime, ret)
        }
        let staffID = row.staff_id
        deptshift.get(stime).set(staffID, row)
      }
    })
  }

  getShiftDeptDay (time) {
    let keys = this.deptEverydayShift && Array.from(this.deptEverydayShift.keys())
    let arr = []
    keys.forEach(key => {
      let deptData = this.deptEverydayShift.get(key)
      deptData = deptData && deptData.get(time) && Array.from(deptData.get(time).values())
      if (!deptData) return
      let datas = new Map()
      for (let i = 0; i < deptData.length; i++) {
        let data = deptData[i]
        let shift = data.workshift
        if (!datas.get(shift)) {
          let msg = {
            num: 0,
            worktime: 0,
            shift_id: shift,
            dept_id: key
          }
          datas.set(shift, msg)
        }
        datas.get(shift).num += 1
        datas.get(shift).worktime += data.worktime
      }
      let results = Array.from(datas.values())
      results.forEach(result => {
        arr.push(result)
      })
    })
    return arr
  }
}
