var card = {
  vehicle: {
    name: 'vehicle',
    label: '车辆',
    table: 'dat_vehicle_dync', // 动态 push 数据
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['card_id', 'card_type_id', 'number', 'x', 'y', 'rec_time', 'down_time', 'up_time', 'enter_area_time', 'map_id', 'area_id', 'dept_id', 'work_time', 'state'], // 字段
      types: ['NUMBER', 'NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'DATETIME', 'DATETIME', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER'], // 字段类型
      labels: ['卡号', '卡类型', '车牌号', 'X坐标', 'Y坐标', '接收时间', '下井时间', '升井时间', '进入区域时间', '地图', '区域', '部门', '工作时长', '状态']
    }
  },

  staff: {
    name: 'staff',
    label: '人员',
    table: 'dat_staff_dync', // 动态 push 数据
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['card_id', 'card_type_id', 'number', 'x', 'y', 'rec_time', 'down_time', 'up_time', 'enter_area_time', 'map_id', 'area_id', 'dept_id', 'work_time', 'state'], // 字段
      types: ['NUMBER', 'NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'DATETIME', 'DATETIME', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER'], // 字段类型
      labels: ['卡号', '卡类型', '身份证', 'X坐标', 'Y坐标', '接收时间', '下井时间', '升井时间', '进入区域时间', '地图', '区域', '部门', '工作时长', '状态']
    }
  },

  adhoc: {
    name: 'adhoc',
    label: '自组网',
    table: 'dat_adhoc_dync', // 动态 push 数据
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['card_id', 'card_type_id', 'number', 'x', 'y', 'rec_time', 'down_time', 'up_time', 'enter_area_time', 'map_id', 'area_id', 'dept_id', 'work_time', 'state'], // 字段
      types: ['NUMBER', 'NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'DATETIME', 'DATETIME', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER'], // 字段类型
      labels: ['卡号', '卡类型', '设备号', 'X坐标', 'Y坐标', '接收时间', '下井时间', '升井时间', '进入区域时间', '地图', '区域', '部门', '工作时长', '状态']
    }
  }
}

module.exports = card
