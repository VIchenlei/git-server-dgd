const datatable = {
  mdt_update: {
    name: 'mdt_update',
    label: '基础信息',
    table: 'dat_mdt_update',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['tableName', 'lastUpdate', 'lastDelete', 'remark'], // 字段, md5用于更新地图
      types: ['STRING', 'DATETIME', 'DATETIME', 'STRING'], // 字段类型
      labels: ['表名称', '表最后更新时间', '表最后删除时间', '备注']
    }
  }
}

module.exports = datatable
