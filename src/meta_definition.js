/*
字段类型：NUMBER, STRING, DATETIME, SELECT, IMAGE, MAP, COLOR, ICON, PATH
每种字段类型，在客户端都对应不同的录入/修改、查阅/显示方式：

* NUMBER : 显示：span，右对齐； 录入：<input type="number">
* STRING : 显示：span, 居中对齐； 录入：<input type="text">
* DATETIME : 显示： span, 居中对齐； 录入：日期选择器 <input type="date">
* SELECT :  显示：span, 居中对齐（value 值为 INT)；录入：select, value 值为 INT
* IMAGE : 显示：span，文件名（居中），鼠标移上去，显示 img 浮层；录入：input，文件名，鼠标点击，打开图片文件对话框。
* MAP：显示：span, 文件名（居中），鼠标点击，打开地图对话框；录入：input，文件名，鼠标点击，打开地图文件对话框。
* COLOR：显示：span，颜色值文字，背景为对应颜色； 录入：颜色选择器 <input type="color">
* ICON : 显示：span + svg；录入：icon选择器（自定义）
* PATH : 显示：链接，鼠标移上去，显示浮层； 录入：textarea，dialog
 */
const metadata = {
  map: {
    name: 'map',
    label: '地图',
    table: 'dat_map',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['map_id', 'name', 'scale', 'detail', 'state_id', 'path', 'md5'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'NUMBER', 'STRING', 'SELECT', 'FILE', 'STRING'], // 字段类型
      labels: ['地图编号', '地图名称', '伸缩比例', '详细描述', '是否有效', '地图文件', '文件摘要'],
      enableNull: [false, false, false, true, true, false, true]
    }
  },
  map_gis: {
    name: 'map_gis',
    label: '地图管理',
    table: 'dat_map_gis',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['map_id', 'name', 'url', 'inner_url', 'layers', 'map_type', 'scale', 'default_map', 'matrixId', 'judge_id', 'x', 'y', 'width', 'height', 'minX', 'minY', 'maxX', 'maxY'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'STRING', 'STRING', 'STRING', 'STRING', 'NUMBER', 'SELECT', 'STRING', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['地图编号', '地图名称', '地图网址', '地图内网地址', '图层', '类型', '地图比例尺', '是否默认地图', '图层名', '是否平铺', '中心x', '中心y', '地图宽', '地图高', '左边界', '上边界', '右边界', '下边界'],
      enableNull: [false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true]
    }
  },
  obj_type: {
    name: 'obj_type',
    label: '对象类型',
    table: 'dat_obj_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['obj_type_id', 'name', 'description'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['对象类型ID', '对象名称', '对象详细描述'],
      enableNull: [false, false, true]
    }
  },
  gis_layer: {
    name: 'gis_layer',
    label: '地图图层',
    table: 'dat_gis_layer',
    keyIndex: 0,
    fields: {
      names: ['gis_layer_id', 'name', 'map_id', 'url', 'request', 'typename', 'outputFormat', 'srsname'],
      types: ['NUMBER', 'STRING', 'SELECT', 'STRING', 'STRING', 'STRING', 'STRING', 'STRING'],
      labels: ['图层编号', '图层名称', '地图编号', '图层网址', '请求', '类别名', '输出格式', '坐标系名称'],
      enableNull: [false, false, true, true, true, true, true, true, true]
    }
  },
  geofault: {
    name: 'geofault',
    label: '地质断层',
    table: 'dat_geofault',
    keyIndex: 0,
    fields: {
      names: ['geofault_id', 'workface_id', 'workface_type', 'geofault_name', 'pt_data'],
      types: ['NUMBER', 'NUMBER', 'SELECT', 'STRING', 'STRING'],
      labels: ['断层ID', '工作面ID', '工作面类型', '断层名称', '断层点坐标'],
      enableNull: [false, false, false, false, false]
    }
  },
  judge: {
    name: 'judge',
    label: '判断',
    table: 'dat_judge',
    keyIndex: 0,
    fields: {
      names: ['judge_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['判断ID', '名称'],
      enableNull: [false, true]
    }
  },
  area: {
    name: 'area',
    label: '区域',
    table: 'dat_area',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['area_id', 'name', 'area_type_id', 'business_type', 'map_id', 'over_count_person', 'over_count_vehicle', 'over_time_person', 'over_speed_vehicle', 'path', 'angle', 'is_work_area', 'over_count_person_rp', 'need_display'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'NUMBER', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'STRING', 'NUMBER', 'SELECT', 'NUMBER', 'SELECT'], // 字段类型
      labels: ['区域编号', '区域名称', '区域类型', '区域业务', '所属地图', '人数上限', '车辆上限', '人停留时长上限(分钟)', '车速上限(Km/h)', '区域定义', '车辆角度', '是否是工作区域', '区域核对人数上限', '是否对外展示'],
      enableNull: [false, false, false, false, false, false, false, false, false, false, true, true, true, false]
    }
  },
  area_business: {
    name: 'area_business',
    label: '区域业务',
    table: 'dat_area_business',
    keyIndex: 0,
    fields: {
      names: ['area_business_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['业务编号', '业务名称'],
      enableNull: [false, true]
    }
  },
  area_ex: {
    name: 'area_ex',
    label: '特殊区域',
    table: 'dat_area_ex',
    keyIndex: 0,
    fields: {
      names: ['area_id', 'point'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['区域编号', '区域点'],
      enableNull: [false, true]
    }
  },
  coalface: {
    name: 'coalface',
    label: '综采面',
    table: 'dat_coalface',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['coalface_id', 'totalLength', 'start_time', 'base_line', 'knife_length', 'knife_ton', 'plan_time', 'limit_speed', 'plan_length', 'head_x', 'head_y', 'tail_x', 'tail_y', 'x_offset', 'y_offset', 'head_y_pos', 'ht_radius'], // 字段
      types: ['SELECT', 'NUMBER', 'DATE', 'NUMBER', 'NUMBER', 'NUMBER', 'DATE', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['综采面编号', '总长度', '开始时间', '基准线宽度', '米/刀', '吨/刀', '计划采完日期', '车速上限', '计划开采长度', '机头x坐标', '机头y坐标', '机尾x坐标', '机尾y坐标', 'x偏移量', 'y偏移量', '机头位置', '机头和机尾范围'],
      enableNull: [false, false, false, false, false, false, false, false, true, false, false, false, false, true, true, false, false]
    }
  },

  landmark: {
    name: 'landmark',
    label: '地标',
    table: 'dat_landmark',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['landmark_id', 'name', 'map_id', 'area_id', 'x', 'y', 'z'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['地标编号', '名称', '所属地图', '所属区域', '坐标x', '坐标y', '坐标z'],
      enableNull: [false, false, false, false, false, false, false]
    }
  },

  direction_mapper: {
    name: 'direction_mapper',
    label: '地图方向',
    table: 'dat_direction_mapper',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['direction_mapper_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['方向编号', '方向名称'],
      enableNull: [false, true]
    }
  },

  coalface_render: {
    name: 'coalface_render',
    label: '综采面分站维护',
    table: 'dat_coalface_render',
    keyIndex: 1, // table中key值在 field 中的位置
    fields: {
      names: ['coalface_id', 'reader_id', 'is_base', 'time', 'user_id'], // 字段
      types: ['SELECT', 'SELECT', 'NUMBER', 'DATETIME', 'STRING'], // 字段类型
      labels: ['综采面名称', '分站名称', '是否为基准分站(0为基准)', '设置时间', '设置人'],
      enableNull: [false, false, false, false, false]
    }
  },
  rules: {
    name: 'rules',
    label: '规则管理',
    table: 'dat_rules',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['rules_id', 'name', 'status', 'description'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'STRING'], // 字段类型
      labels: ['规则编号', '规则名称', '过滤是否生效', '备注'],
      enableNull: [false, false, false, true]
    }
  },
  coalface_vehicle: {
    name: 'coalface_vehicle',
    label: '采煤机与综采面绑定',
    table: 'dat_coalface_vehicle',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['coalface_id', 'vehicle_id', 'state', 'schedule_work_time', 'schedule_mine_times', 'driver_dist', 'driver_dist_time'], // 字段
      types: ['SELECT', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['综采面名称', '绑定车辆', '状态', '计划开机时长（单位：小时）', '计划刀数', '司机距掘进机距离阈值', '司机距掘进机距离时长阈值'],
      enableNull: [false, false, false, false, false, true, true]
    }
  },
  drivingface: {
    name: 'drivingface',
    label: '掘进面',
    table: 'dat_drivingface',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['drivingface_id', 'total_length', 'start_time', 'plan_time', 'plan_length', 'drifting_footage_unit'], // 字段
      types: ['SELECT', 'NUMBER', 'DATE', 'DATE', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['掘进面名称', '总长度', '开始时间', '计划完成日期', '计划开采长度', '掘进面排距(单位：米)'],
      enableNull: [false, false, false, false, false, false]
    }
  },
  camera: {
    name: 'camera',
    label: '视频监控管理',
    table: 'dat_camera',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['camera_id', 'ip', 'map_id', 'name', 'pos_desc', 'user', 'password', 'port', 'codec', 'channel', 'subtype', 'angle', 'x', 'y', 'z'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'STRING', 'STRING', 'STRING', 'STRING', 'NUMBER', 'STRING', 'STRING', 'STRING', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['摄像机编号', 'ip', '所属地图', '摄像头名称', '位置', '用户名', '密码', '端口', '视频编码', '通道号', '码流类型', '方向', 'x坐标', 'y坐标', 'z坐标'],
      enableNull: [false, false, false, false, false, false, false, false, false, false, false, false, false, false, false]

    }
  },
  // drivingface_render: {
  //   name: 'drivingface_render',
  //   label: '掘进面分站',
  //   table: 'dat_drivingface_render',
  //   keyIndex: 0, // table中key值在 field 中的位置
  //   fields: {
  //     names: ['drivingface_id', 'reader_id', 'time', 'user_id', 'lastUpdate'], // 字段
  //     types: ['SELECT', 'SELECT', 'DATETIME', 'STRING', 'DATETIME'], // 字段类型
  //     labels: ['掘进面', '分站名称', '设置时间', '设置人', '最后更新时间']
  //   }
  // },
  drivingface_warning_point: {
    name: 'drivingface_warning_point',
    label: '掘进面告警点',
    table: 'dat_drivingface_warning_point',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['warning_point_id', 'warning_point_name', 'drivingface_id', 'point_x', 'point_y', 'point_z', 'warning_threshold', 'isvalid'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'SELECT'], // 字段类型
      labels: ['告警点编号', '告警点名称', '绑定掘进面', '告警点x', '告警点y', '告警点z', '告警门限', '是否有效'],
      enableNull: [false, false, false, false, false, false, false, false]
    }
  },
  drivingface_vehicle: {
    name: 'drivingface_vehicle',
    label: '掘进面',
    table: 'dat_drivingface_vehicle',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['drivingface_id', 'vehicle_id', 'state', 'warning_threshold', 'over_count_num', 'schedule_work_time', 'schedule_tunnelling_times', 'small_reader_id', 'relay_small_reader_id', 'big_reader_id', 'shake_threshold', 'driver_dist', 'driver_dist_time'], // 字段
      types: ['SELECT', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['掘进面', '车牌号', '状态', '距离告警阈值', '人员限值', '计划开机时长(单位：小时)', '计划排数', '小分站ID', '中继分站ID', '大分站ID', '震动阈值', '司机距掘进机距离阈值', '司机距掘进机距离时长阈值'],
      enableNull: [true, false, true, false, false, false, false, true, true, false, false, true, true]
    }
  },

  drivingface_worktype_permission: {
    name: 'drivingface_worktype_permission',
    label: '掘进面工种描述',
    table: 'dat_drivingface_worktype_permission',
    keyIndex: 0,
    fields: {
      names: ['worktype_id', 'worktype_des'],
      types: ['SELECT', 'STRING'],
      labels: ['工种', '工种描述'],
      enableNull: [false, true]
    }
  },
  area_type: {
    name: 'area_type',
    label: '区域类型',
    table: 'dat_area_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['area_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['区域类型编号', '区域类型名称'],
      enableNull: [false, false]
    }
  },
  patrol_path_type: {
    name: 'patrol_path_type',
    label: '巡检路线类型',
    table: 'dat_patrol_path_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['巡检路线类型编号', '类型名称'],
      enableNull: [false, true]
    }
  },
  patrol_path: {
    name: 'patrol_path',
    label: '巡检路线',
    table: 'dat_patrol_path',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_path_id', 'name', 'patrol_type_id'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT'], // 字段类型
      labels: ['巡检路线编号', '巡检路线名称', '巡检路线类型'],
      enableNull: [false, false, true]
    }
  },

  patrol_path_detail: {
    name: 'patrol_path_detail',
    label: '巡检路线点信息管理',
    table: 'dat_patrol_path_detail',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['order_id', 'patrol_path_id', 'patrol_point_id', 'idx', 'duration_last', 'duration_stay_min', 'duration_stay_max', 'duration_ranging'], // 字段
      types: ['NUMBER', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['编号', '巡检路线名称', '巡检路线点名称', '巡检点顺序', '距离上点时间差(分)', '停留最短时间(分)', '停留最长时间(分)', '巡检点时间范围偏差（分）'],
      enableNull: [false, false, false, false, false, false, false, false]
    }
  },
  patrol_point: {
    name: 'patrol_point',
    label: '巡检点表',
    table: 'dat_patrol_point',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_point_id', 'name', 'patrol_point_type_id', 'x', 'y', 'z', 'map_id', 'ranging'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['点编号', '点名称', '点类型', '点x坐标', '点y坐标', '点z坐标', '点地图名', '点巡检范围'],
      enableNull: [false, false, false, false, false, false, false, false]
    }
  },
  patrol_point_type: {
    name: 'patrol_point_type',
    label: '巡检点类型表',
    table: 'dat_patrol_point_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_point_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['巡检点类型编号', '点类型名称'],
      enableNull: [false, true]
    }
  },
  patrol_task: {
    name: 'patrol_task',
    label: '巡检排班表',
    table: 'dat_patrol_task',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_task_id', 'staff_id', 'time', 'shift_id', 'start_time', 'end_time', 'patrol_path_id'], // 字段
      types: ['NUMBER', 'SELECT', 'DATE', 'SELECT', 'DATETIME', 'DATETIME', 'SELECT'], // 字段类型
      labels: ['巡检任务编号', '巡检人员', '日期', '班次', '巡检起始时间', '巡检结束时间', '巡检路径名'],
      enableNull: [false, false, false, false, false, false, false]
    }
  },
  patrol_state: {
    name: 'patrol_state',
    label: '巡检点状态定义表',
    table: 'dat_patrol_state',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_state_id', 'name', 'color'], // 字段
      types: ['NUMBER', 'STRING', 'COLOR'], // 字段类型
      labels: ['巡检状态编号', '巡检状态描述', '巡检状态颜色'],
      enableNull: [false, false, false]
    }
  },
  patrol_stay_state: {
    name: 'patrol_stay_state',
    label: '巡检停留状态表',
    table: 'dat_patrol_stay_state',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_stay_state_id', 'name', 'color'], // 字段
      types: ['NUMBER', 'STRING', 'COLOR'], // 字段类型
      labels: ['巡检停留状态编号', '巡检停留状态描述', '巡检停留状态颜色'],
      enableNull: [false, false, false]
    }
  },
  patrol_data: {
    name: 'patrol_data',
    label: '巡检记录表',
    table: 'his_patrol_data',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['patrol_task_id', 'patrol_path_id', 'patrol_point_id', 'idx', 'enter_time', 'leave_time', 'patrol_state_id', 'patrol_stay_state_id', 'card_id'], // 字段
      types: ['NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'SELECT', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['巡检任务编号', '巡检路径编号', '巡检点编号', '巡检点索引', '进入点集时间', '离开点集时间', '巡检状态', '巡检停留状态', '卡号'],
      enableNull: [false, false, false, false, false, false, false, false, false]
    }
  },

  dept: {
    name: 'dept',
    label: '部门',
    table: 'dat_dept',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['dept_id', 'hm_dept_id', 'name', 'abbr', 'address', 'phone', 'rank', 'dept_info'], // 字段
      types: ['NUMBER', 'NUMBER', 'STRING', 'STRING', 'STRING', 'STRING', 'NUMBER', 'STRING'], // 字段类型
      labels: ['部门编号', '虹膜部门编号', '部门名称', '部门缩写', '部门地址', '部门电话', '部门排序', '备注'],
      enableNull: [false, false, false, true, true, true, true, true]
    }
  },
  dept_ck: { // 虚拟部门表
    name: 'dept_ck',
    label: '部门',
    table: 'dat_dept_ck',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['dept_id', 'name', 'abbr', 'address', 'phone', 'rank', 'dept_info'], // 字段
      types: ['NUMBER', 'STRING', 'STRING', 'STRING', 'STRING', 'NUMBER', 'STRING'], // 字段类型
      labels: ['部门编号', '部门名称', '部门缩写', '部门地址', '部门电话', '部门排序', '备注'],
      enableNull: [false, false, true, true, true, true, true]
    }
  },

  duty: {
    name: 'duty',
    label: '在职',
    table: 'dat_duty',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['duty_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['是否在职', '名称'],
      enableNull: [false, false]
    }
  },
  group: {
    name: 'group',
    label: '班组',
    table: 'dat_group',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['group_id', 'name', 'detail', 'rank'], // 字段
      types: ['NUMBER', 'STRING', 'STRING', 'NUMBER'], // 字段类型
      labels: ['班组编号', '名称', '详细描述', '班组排序'],
      enableNull: [false, false, true, true]
    }
  },
  worktype: {
    name: 'worktype',
    label: '工种',
    table: 'dat_worktype',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['worktype_id', 'name', 'shortname', 'rank'], // 字段
      types: ['NUMBER', 'STRING', 'STRING', 'NUMBER'], // 字段类型
      labels: ['工种编号', '名称', '简称', '工种排序'],
      enableNull: [false, false, false, true]
    }
  },
  occupation: {
    name: 'occupation',
    label: '职务',
    table: 'dat_occupation',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['occupation_id', 'name', 'shortname', 'occupation_level_id', 'rank'], // 字段
      types: ['NUMBER', 'STRING', 'STRING', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['职务编号', '职务名称', '简称', '职务级别', '职务排序'],
      enableNull: [false, false, false, false, true]
    }
  },
  // leader_arrange: {
  //   name: 'leader_arrange',
  //   label: '领导排班',
  //   table: 'dat_leader_arrange',
  //   keyIndex: 0, // table中key值在 field 中的位置
  //   fields: {
  //     names: ['staff_id', 'shift_id', 'duty_date', 'user_id'], // 字段
  //     types: ['SELECT', 'SELECT', 'DATE', 'STRING'], // 字段类型
  //     labels: ['姓名', '班次', '日期', '操作人'],
  //     enableNull: [false, false, false, false]
  //   }
  // },
  driver_arrange: {
    name: 'driver_arrange',
    label: '司机排班',
    table: 'dat_driver_arrange',
    keyIndex: 0,
    fields: {
      names: ['name', 'staff_id', 'dept_id', 'driver_date', 'vehicle_id', 'vehicle_number', 'shift_type_id', 'shift_id'], // 字段
      types: ['NUMBER', 'SELECT', 'SELECT', 'DATE', 'SELECT', 'SELECT', 'SELECT'], // 字段类型
      labels: ['编号', '姓名', '部门', '日期', '车辆名称', '班制', '班次'],
      enableNull: [true, true, true, true, true, true, true]
    }
  },
  card: {
    name: 'card', // 标识卡
    label: '标识卡',
    table: 'dat_card',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['card_id', 'card_type_id', 'ident', 'state_id', 'comment'], // 字段，card_id = card_type_id + card_ident
      types: ['NUMBER', 'SELECT', 'NUMBER', 'SELECT', 'STRING'], // 字段类型
      labels: ['卡号', '卡类型', '卡标识', '使用状态', '备注'],
      enableNull: [false, false, false, false, true]
    }
  },
  state: {
    name: 'state', // 标识卡
    label: '卡状态',
    table: 'dat_state',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['state_id', 'name', 'description'], // 字段，card_id = card_type_id + card_ident
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['卡标识', '名称', '描述'],
      enableNull: [false, false, true]
    }
  },

  card_type: {
    name: 'card_type',
    label: '卡类型',
    table: 'dat_card_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['card_type_id', 'name', 'detail'],
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['卡类型编号', '卡类型名称', '详细描述'],
      enableNull: [false, false, false]
    }
  },
  state_card: {
    name: 'state_card',
    label: '标识卡状态',
    table: 'dat_state_card',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['state_card_id', 'name', 'label', 'color'],
      types: ['NUMBER', 'STRING', 'STRING', 'COLOR'], // 字段类型
      labels: ['标识卡状态编号', '状态名称', '状态描述', '显示颜色'],
      enableNull: [false, false, false, false]
    }
  },
  state_object: {
    name: 'state_object',
    label: '标识卡绑定对象状态',
    table: 'dat_state_object',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['state_object_id', 'name', 'label', 'color'],
      types: ['NUMBER', 'STRING', 'STRING', 'COLOR'], // 字段类型
      labels: ['对象状态编号', '状态名称', '状态描述', '显示颜色'],
      enableNull: [false, false, true, true]
    }
  },
  state_biz: {
    name: 'state_biz',
    label: '标识卡业务状态',
    table: 'dat_state_biz',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['state_biz_id', 'name', 'label', 'color'],
      types: ['NUMBER', 'STRING', 'STRING', 'COLOR'], // 字段类型
      labels: ['业务状态编号', '状态名称', '状态描述', '显示颜色'],
      enableNull: [false, false, true, true]
    }
  },
  sex: {
    name: 'sex',
    label: '性别',
    table: 'dat_sex',
    keyIndex: 0,
    fields: {
      names: ['sex_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['性别编号', '名称'],
      enableNull: [true, true]
    }
  },
  staff: {
    name: 'staff',
    label: '员工',
    table: 'dat_staff',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['staff_id', 'name', 'rank', 'sex_id', 'certification', 'birthday', 'pic', 'blood', 'height', 'weight', 'marry_id', 'education_id', 'medicine', 'telephone', 'relative_telephone', 'address'], // 字段
      types: ['NUMBER', 'STRING', 'NUMBER', 'SELECT', 'STRING', 'DATE', 'FILE', 'STRING', 'NUMBER', 'NUMBER', 'SELECT', 'SELECT', 'STRING', 'STRING', 'STRING', 'STRING'], // 字段类型
      labels: ['员工编号', '姓名', '序号', '员工性别', '身份证', '出生日期', '照片', '血型', '身高(cm)', '体重(kg)', '婚姻状况', '学历', '药物过敏史', '联系方式', '亲属电话', '地址'],
      enableNull: [false, false, true, false, false, true, true, true, true, true, true, true, true, true, true, true]
    }
  },

  marry: {
    name: 'marry',
    label: '婚姻状况',
    table: 'dat_marry',
    keyIndex: 0,
    fields: {
      names: ['marry_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['婚姻编号', '名称'],
      enableNull: [false, false]
    }
  },

  education: {
    name: 'education',
    label: '学历状况',
    table: 'dat_education',
    keyIndex: 0,
    fields: {
      names: ['education_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['学历编号', '名称'],
      enableNull: [false, true]
    }
  },
  staff_extend: {
    name: 'staff_extend',
    label: '人员业务表',
    table: 'dat_staff_extend',
    keyIndex: 0,
    fields: {
      names: ['staff_id', 'dept_id', 'dept_id_ck', 'card_id', 'lampNo', 'occupation_id', 'worktype_id', 'shift_type_id', 'min_work_time', 'need_display'], // 字段
      types: ['NUMBER', 'SELECT', 'SELECT', 'NUMBER', 'STRING', 'SELECT', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['员工', '部门', '虚拟部门', '卡号', '矿灯号', '职务', '工种', '班制', '最小下井时长', '是否显示(0:不显示;1:显示)'],
      enableNull: [false, true, true, false, true, true, true, true, true, true, true, true, true, true]
    }
  },
  staff_extend_ck: { // 虚拟人员业务表
    name: 'staff_extend_ck',
    label: '人员业务表',
    table: 'dat_staff_extend_ck',
    keyIndex: 0,
    fields: {
      names: ['staff_id', 'dept_id', 'card_id', 'lampNo', 'occupation_id', 'worktype_id', 'shift_type_id', 'min_work_time', 'need_display'], // 字段
      types: ['NUMBER', 'SELECT', 'NUMBER', 'STRING', 'SELECT', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['员工', '部门', '卡号', '矿灯号', '职务', '工种', '班制', '最小下井时长', '是否显示(0:不显示;1:显示)'],
      enableNull: [false, true, false, true, true, true, true, true, true, true, true, true, true]
    }
  },
  his_maintenance: {
    name: 'his_maintenance',
    label: '车辆保养管理',
    table: 'his_maintenance',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['maintenance_id', 'vehicle_id', 'maintenance_date', 'driver', 'maintainer', 'maintain_leader', 'remark'], // 字段
      types: ['NUMBER', 'SELECT', 'DATETIME', 'STRING', 'STRING', 'STRING', 'STRING'], // 字段类型
      labels: ['保养记录编号', '车辆名称', '保养日期', '司机', '检修人', '检修负责人', '备注'],
      enableNull: [false, false, false, false, false, false, true]
    }
  },

  parts_record: {
    name: 'parts_record',
    label: '配件消耗记录',
    table: 'his_parts_use_record',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['parts_record_id', 'cur_time', 'into_parts', 'into_version', 'into_number', 'out_parts', 'out_version', 'out_number', 'surplus', 'send_user', 'description'], // 字段
      types: ['NUMBER', 'DATETIME', 'STRING', 'STRING', 'NUMBER', 'STRING', 'STRING', 'NUMBER', 'NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['配件记录编号', '日期时间', '进库配件', '进库型号', '进库数量', '出库配件', '出库型号', '出库数量', '余量', '发放人', '备注'],
      enableNull: [false, true, true, true, true, true, true]
    }
  },

  his_checkparts_data: {
    name: 'his_checkparts_data',
    label: '保养记录检查项',
    table: 'his_checkparts_data',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['maintenance_id', 'checkpartsitem_id', 'stat'],
      types: ['NUMBER', 'SELECT', 'STRING'],
      labels: ['保养记录编号', '检查项目', '检查项目状态'],
      enableNull: [false, false, false]
    }
  },
  checkparts: {
    name: 'checkparts',
    label: '检查部位',
    table: 'dat_checkparts',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['checkparts_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['检查部位编号', '检查部位名称'],
      enableNull: [false, false]
    }
  },
  checkpartsitem: {
    name: 'checkpartsitem',
    label: '检查项',
    table: 'dat_checkpartsitem',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['checkpartsitem_id', 'checkparts_id', 'name'],
      types: ['NUMBER', 'SELECT', 'STRING'],
      labels: ['检查项目编号', '检查部位', '检查项目名称'],
      enableNull: [false, false, true]
    }
  },

  parts_type: {
    name: 'parts_type',
    label: '配件类型',
    table: 'dat_parts_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['parts_type_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['配件类型编号', '配件类型名称'],
      enableNull: [false, false]
    }
  },
  parts: {
    name: 'parts',
    label: '配件类型',
    table: 'dat_parts',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['parts_id', 'name', 'parts_type_id'],
      types: ['NUMBER', 'STRING', 'SELECT'],
      labels: ['配件类型编号', '配件类型名称', '配件类型'],
      enableNull: [false, false, false]
    }
  },
  intersection_point: {
    name: 'intersection_point',
    label: '相交点',
    table: 'dat_intersection_point',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['intersection_point_id', 'point'],
      types: ['NUMBER', 'STRING'],
      labels: ['相交点id', '交点'],
      enableNull: [false, false]
    }
  },

  /*
  level: {
    name: 'level',
      names: ['staff_id', 'name', 'sex', 'certification', 'birthday', 'duty_id', 'dept_id', 'group_id', 'occupation_id', 'area_id', 'card_id', 'pic', 'level_id'], // 字段
      types: ['NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER', 'SELECT', 'SELECT', 'SELECT', 'STRING', 'STRING', 'FILE', 'SELECT'], // 字段类型
      labels: ['员工编号', '姓名', '员工性别', '身份证', '出生日期', '是否在职', '部门', '班组', '职务', '区域', '卡号', '照片', '职务级别']
    }
  },
*/
  occupation_level: {
    name: 'occupation_level',
    label: '职务级别',
    table: 'dat_occupation_level',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['occupation_level_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['职务级别编号', '级别名称'],
      enableNull: [false, false]
    }
  },

  // adhoc: {
  //   name: 'adhoc',
  //   label: '自组网',
  //   table: 'dat_adhoc',
  //   keyIndex: 0, // table中key值在 field 中的位置
  //   fields: {
  //     names: ['adhoc_id', 'card_id', 'dept_id', 'state'], // 字段
  //     types: ['NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
  //     labels: ['自组网编号', '卡号', '所属部门', '状态']
  //   }
  // },

  vehicle: {
    name: 'vehicle',
    label: '车辆',
    table: 'dat_vehicle',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['vehicle_id', 'name', 'rank', 'vehicle_type_id', 'pic', 'bigger_car_flag'], // 字段
      types: ['NUMBER', 'STRING', 'NUMBER', 'SELECT', 'FILE', 'NUMBER'], // 字段类型
      labels: ['车辆编号', '车辆名称', '排序', '车辆类型', '照片', '大车(1)/小车(0)'],
      enableNull: [false, false, true, false, true, true]
    }
  },

  vehicle_extend: {
    name: 'vehicle_extend',
    label: '车辆业务表',
    table: 'dat_vehicle_extend',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['vehicle_id', 'dept_id', 'group_id', 'card_id', 'shift_type_id', 'need_display', 'power_alarm'],
      types: ['NUMBER', 'SELECT', 'SELECT', 'NUMBER', 'SELECT', 'NUMBER', 'SELECT'],
      labels: ['车辆编号', '部门', '班组', '卡号', '班制', '是否显示(0:不显示;1:显示)', '是否推送电量低'],
      enableNull: [false, true, false, false, true, false, true]
    }
  },
  vehicle_type: {
    name: 'vehicle_type',
    label: '车辆类型',
    table: 'dat_vehicle_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['vehicle_type_id', 'name', 'rank', 'capacity', 'vehicle_category_id', 'vehicle_level_id'], // 字段
      types: ['NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'SELECT', 'SELECT'], // 字段类型
      labels: ['车辆类型编号', '车辆类型名称', '车辆类型排序', '车辆载荷能力(吨)', '车辆类型', '车辆等级'],
      enableNull: [false, false, false, false, false, false]
    }
  },
  vehicle_category: {
    name: 'vehicle_category',
    label: '车辆类型颜色',
    table: 'dat_vehicle_category',
    keyIndex: 0,
    fields: {
      names: ['vehicle_category_id', 'name', 'description', 'color', 'over_speed'],
      types: ['SELECT', 'STRING', 'STRING', 'STRING', 'NUMBER'],
      labels: ['车辆类型', '类型名称', '类型描述', '类型颜色', '超速速度'],
      enableNull: [false, false, false, false, true]
    }
  },
  vehicle_level: {
    name: 'vehicle_level',
    label: '车辆等级',
    table: 'dat_vehicle_level',
    keyIndex: 0,
    fields: {
      names: ['vehicle_level_id', 'name', 'label'],
      types: ['SELECT', 'STRING', 'STRING'],
      labels: ['车辆等级序号', '车辆等级名称', '车辆等级描述'],
      enableNull: [false, false, true]
    }
  },
  /*
  vehicle_card: {
    name: 'vehicle_card',
    label: '车辆绑定关系',
    table: 'dat_vehicle_card',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['vehicle_id', 'card_id'], // 字段
      types: ['NUMBER', 'NUMBER'], // 字段类型
      labels: ['车辆编号', '卡号']
    }
  },
  */
  device_type: {
    name: 'device_type',
    label: '设备类型',
    table: 'dat_device_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['device_type_id', 'name', 'detail'],
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['设备类型编号', '设备类型名称', '详细描述'],
      enableNull: [false, false, false]
    }
  },

  device_state: {
    name: 'device_state',
    label: '设备状态',
    table: 'dat_device_state',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['device_state_id', 'name', 'label', 'color'],
      types: ['NUMBER', 'STRING', 'STRING', 'COLOR'], // 字段类型
      labels: ['设备状态编号', '状态名称', '状态描述', '显示颜色'],
      enableNull: [false, false, false, true]
    }
  },

  reader: {
    name: 'reader',
    label: '分站',
    table: 'dat_reader',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['reader_id', 'reader_type_id', 'name', 'brief_name', 'map_id', 'area_id', 'x', 'y', 'z', 'angle', 'state', 'ip', 'device_type_id', 'dimension', 'enable_simulate_card', 'need_power_alarm', 'fade_value', 'vehicle_id'], // 字段
      types: ['NUMBER', 'SELECT', 'STRING', 'STRING', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'SELECT', 'STRING', 'SELECT', 'SELECT', 'SELECT', 'SELECT', 'NUMBER', 'SELECT'], // 字段类型
      labels: ['分站编号', '分站类型', '分站名称', '分站简称', '所属地图', '所属区域', '坐标X', '坐标Y', '坐标Z', '分站仰角', '分站状态', 'IP', '设备类型', '定位维度', '是否可以模拟成定位卡', '掉电告警', '盲区阈值', '绑定车辆'],
      enableNull: [false, false, false, true, false, false, false, false, true, false, false, true, false, false, false, false, false, true]
      // names: [ 'reader_id', 'reader_type_id', 'name', 'map_id', 'area_id', 'x', 'y', 'z', 'angle', 'state', 'ip', 'device_type_id' ], ,'isParent','parent_reader_id'// 字段
      // types: [ 'NUMBER', 'NUMBER', 'STRING', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'SELECT' ,'NUMBER','NUMBER'], // 字段类型
      // labels: [ '分站编号', '分站类型', '分站名称', '所属地图', '所属区域', '坐标X', '坐标Y', '坐标Z', '分站仰角', '分站状态', 'IP', '设备类型','主分站(1:是0:否)','主分站编号' ]
    }
  },
  reader_type: {
    name: 'reader_type',
    label: '分站类型',
    table: 'dat_reader_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['reader_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['分站类型', '类型名称'],
      enableNull: [false, false]
    }
  },
  antenna: {
    name: 'antenna',
    label: '天线',
    table: 'dat_antenna',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['antenna_id', 'reader_id', 'name', 'idx', 'x', 'y', 'z', 'angle'], // 字段
      types: ['NUMBER', 'SELECT', 'STRING', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['天线编号', '分站名称', '天线名称', '天线索引', '坐标X', '坐标Y', '坐标z', '天线角度'],
      enableNull: [false, false, true, false, false, false, true, true]
    }
  },
  /*
    power_box: {
      name: 'power_box',
      label: '电源箱',
      table: 'dat_power_box',
      keyIndex: 0, // table中key值在 field 中的位置
      fields: {
        names: ['power_box_id', 'map_id', 'area_id', 'render_id', 'name', 'x', 'y','z','state','port','device_type_id','lastUpdate'], // 字段
        types: ['NUMBER', 'SELECT', 'STRING', 'SELECT', 'STRING', 'NUMBER','NUMBER','NUMBER','STRING','STRING','NUMBER'], // 字段类型
        labels: ['电源箱编号', '所属地图', '所属区域', '供电分站号', '名称', 'x坐标','y坐标','z坐标','状态','端口','设备类型','最后更新时间']
        // names: [ 'antenna_id', 'reader_id', 'reader_idx', 'name', 'x', 'y', 'z', 'angle' ], // 字段
        // types: [ 'NUMBER', 'SELECT', 'NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER' ], // 字段类型
        // labels: [ '天线编号', '分站编号', '分站天线序号', '天线名称', '坐标X', '坐标Y', '坐标Z', '天线角度' ]
      }
    }
    */

  light: {
    name: 'light',
    label: '红绿灯',
    table: 'dat_light',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['light_id', 'lights_group_id', 'x', 'y', 'z', 'name', 'physics_light_id', 'physics_light_direction', 'special_flag', 'reader_id', 'state'], // 字段
      types: ['NUMBER', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER', 'SELECT', 'SELECT', 'SELECT', 'SELECT'], // 字段类型
      labels: ['红绿灯编号', '灯组', '坐标x', '坐标y', '坐标z', '名称', '物理灯号', '物理灯朝向', '特殊红绿灯', '分站id', '使用状态'],
      enableNull: [false, false, true, false, false, false, true, false, false, false, false, true]
    }
  },
  lights_group: {
    name: 'lights_group',
    label: '红绿灯组',
    table: 'dat_lights_group',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['lights_group_id', 'name', 'state', 'x', 'y', 'z', 'map_id', 'area_id', 'light_auto_interval'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'SELECT', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['红绿灯组编号', '灯组名称', '使用状态', '中心点x', '中心点y', '中心点z', '地图', '区域', '断网时自动控制间隔(s)'],
      enableNull: [false, true, true, true, false, false, true, false, true, false]
    }
  },
  // lights_binding: {
  //   name: 'lights_binding',
  //   label: '红绿灯组绑定',
  //   table: 'dat_lights_binding',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['lights_binding_id', 'light_id', 'lights_group_id'],
  //     types: ['NUMBER', 'SELECT', 'SELECT'],
  //     labels: ['绑定编号', '红绿灯', '红绿灯组'],
  //     enableNull: [false, false, false]
  //   }
  // },

  shift_type: {
    name: 'shift_type', // 班次类别 / 班制
    label: '班制类别',
    table: 'dat_shift_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['shift_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['班制类别编号', '班制类别名称'],
      enableNull: [false, true]
    }
  },
  shift: {
    name: 'shift', // 班次
    label: '班次',
    table: 'dat_shift',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['shift_id', 'name', 'shift_type_id', 'start_time', 'end_time', 'offset', 'short_name', 'min_minutes'], // 字段
      types: ['NUMBER', 'STRING', 'SELECT', 'TIME', 'TIME', 'NUMBER', 'STRING', 'NUMBER'], // 字段类型
      labels: ['班次编号', '班次名称', '班次类别', '开始时间', '结束时间', '班次偏移天数', '短名称', '班次最小时长（小时）'],
      enableNull: [false, true, false, true, true, false, true, false]
    }
  },

  user: { // 密码只能在server端读写，不会push到客户端
    name: 'user',
    label: '账户',
    table: 'dat_user',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['user_id', 'pwd', 'name', 'role_id', 'is_check', 'access_id'], // 字段
      types: ['STRING', 'STRING', 'STRING', 'SELECT', 'SELECT', 'STRING'], // 字段类型
      labels: ['账号', '密码', '用户名称', '用户角色', '用户类别', '管理范围'],
      enableNull: [false, false, false, false, false, false, true]
    }
  },
  role: {
    name: 'role',
    label: '角色',
    table: 'dat_role',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['role_id', 'name', 'menus'], // 字段
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['角色编号', '角色名称', '管理菜单'],
      enableNull: [false, true, false]
    }
  },
  // role_rank: {
  //   name: 'role_rank',
  //   label: '角色等级',
  //   table: 'dat_role_rank',
  //   keyIndex: 0, // table中key值在 field 中的位置
  //   fields: {
  //     names: ['role_rank_id', 'name'], // 字段
  //     types: ['NUMBER', 'STRING'], // 字段类型
  //     labels: ['角色编号', '角色级别名称'],
  //     enableNull: [false, false]
  //   }
  // },
  menu: {
    name: 'menu',
    label: '菜单定义',
    table: 'dat_menu',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['menu_id', 'name', 'level_id'], // 字段
      types: ['STRING', 'STRING', 'NUMBER'], // 字段类型
      labels: ['菜单编号', '菜单名称', '菜单等级'],
      enableNull: [false, true, false]
    }
  },
  access: {
    name: 'access',
    label: '菜单权限',
    table: 'dat_access',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['access_id', 'depts'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['权限组编号', '管理部门'],
      enableNull: [false, true]
    }
  },
  /*
    alarm_type: {
      name: 'alarm_type',
      label: '告警类型',
      table: 'dat_alarm_type',
      keyIndex: 0, // table中key值在 field 中的位置
      fields: {
        names: ['alarm_type_id', 'name'], // 字段
        types: ['NUMBER', 'STRING'], // 字段类型
        labels: ['告警类型编号', '告警类型名称']
      }
    },
  */
  op_type: {
    name: 'op_type',
    label: '操作类型',
    table: 'dat_op_type',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['op_type_id', 'name'], // 字段
      types: ['NUMBER', 'STRING'], // 字段类型
      labels: ['操作类型编号', '操作名称'],
      enableNull: [false, true]
    }
  },

  setting: {
    name: 'setting', // 系统设置
    label: '系统设置',
    table: 'dat_setting',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['setting_id', 'name', 'value', 'description'], // 字段，card_id = card_type_id + card_ident
      types: ['NUMBER', 'STRING', 'STRING', 'STRING'], // 字段类型
      labels: ['设置编号', '设置名称', '值', '描述'],
      enableNull: [false, false, false, true]
    }
  },

  battery_vehicle: {
    name: 'battery_vehicle',
    label: '电池安装记录',
    table: 'dat_battery_vehicle',
    keyIndex: 0,
    fields: {
      names: ['battery_id', 'vehicle_id', 'staff_id', 'use_date', 'remark'],
      types: ['SELECT', 'SELECT', 'SELECT', 'DATETIME', 'STRING'],
      labels: ['电池编号', '车辆名称', '安装人', '安装日期', '备注'],
      enableNull: [false, false, false, false, true]
    }
  },

  battery: {
    name: 'battery',
    label: '电池充电记录',
    table: 'dat_battery',
    keyIndex: 0,
    fields: {
      names: ['battery_id', 'name', 'staff_id', 'charge_date', 'remark'],
      types: ['SELECT', 'STRING', 'SELECT', 'DATETIME', 'STRING'],
      labels: ['电池编号', '电池编号名称', '充电人', '充电日期', '备注'],
      enableNull: [false, false, false, false, true]
    }
  },

  // transport_plan: {
  //   name: 'transport_plan',
  //   label: '运输计划',
  //   table: 'dat_transport_plan',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['trans_id', 'dept_id', 'commodity', 'plan_num', 'site', 'truck_num', 'staff_id', 'date_time', 'shift_id'],
  //     types: ['NUMBER', 'SELECT', 'STRING', 'NUMBER', 'STRING', 'NUMBER', 'INPUT', 'DATETIME', 'SELECT'],
  //     labels: ['计划号', '部门', '运料种类', '计划装车量(车)', '卸料地点', '实际装车量(车)', '调度员', '调度时间', '班次'],
  //     enableNull: [false, false, false, false, true, true, false, false, false]
  //   }
  // },

  // transport_back: {
  //   name: 'transport_back',
  //   label: '运输任务反馈',
  //   table: 'dat_transport_back',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['tran_back_id', 'dept_id', 'feedstock_commodity', 'feedstock_num', 'reshipment_dept_id', 'reshipment_commodity', 'reshipment_plan', 'reshipment_finish', 'residues_dept_id', 'residues_commodity', 'residues_num', 'enter_time', 'captain', 'monitor', 'date_time', 'shift_id'],
  //     types: ['NUMBER', 'SELECT', 'STRING', 'NUMBER', 'STRING', 'STRING', 'NUMBER', 'NUMBER', 'STRING', 'STRING', 'NUMBER', 'DATETIME', 'NUMBER', 'NUMBER', 'DATETIME', 'SELECT'],
  //     labels: ['任务单号', '进料单位', '进料品名', '进料数量(车)', '倒运单位', '倒运品名', '倒运计划量(车)', '倒运完成情况量(车)', '剩料单位', '剩料品名', '剩料数量(车)', '剩料入井时间', '跟班队干', '班组长', '反馈时间', '班次'],
  //     enableNull: [false, false, false, false, true, true, true, true, false, false, false, true, false, false, false, true, false]
  //   }
  // },

  state_vehicle: {
    name: 'state_vehicle',
    label: '车辆维修状态',
    table: 'dat_state_vehicle',
    keyIndex: 0,
    fields: {
      names: ['state_vehicle_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['状态编号', '状态名称'],
      enableNull: [false, true]
    }
  },

  dat_vehicle_state: {
    name: 'dat_vehicle_state',
    label: '车辆状态登记',
    table: 'dat_vehicle_state',
    keyIndex: 0,
    fields: {
      names: ['vehicle_id', 'start_time', 'end_time', 'shift_id', 'state_vehicle_id'],
      types: ['SELECT', 'DATETIME', 'DATETIME', 'SELECT', 'SELECT'],
      labels: ['车辆编号', '开始时间', '结束时间', '班次', '车辆状态'],
      enableNull: [true, , false, false, true, true]
    }
  },

  dat_vehicle_drive: {
    name: 'dat_vehicle_drive',
    label: '车辆出/回车登记',
    table: 'dat_vehicle_drive',
    keyIndex: 0,
    fields: {
      names: ['vehicle_id', 'leave_time', 'enter_time', 'shift_id', 'staff_id'],
      types: ['SELECT', 'DATETIME', 'DATETIME', 'SELECT', 'SELECT'],
      labels: ['车辆编号', '出车时间', '回车时间', '班次', '司机'],
      enableNull: [false, true, false, false, false]
    }
  },

  // materiel_bar: {
  //   name: 'materiel_bar',
  //   label: '物料倒运计划表',
  //   table: 'his_materiel_bar_plan',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['work_property', 'product_name', 'vehcile_number', 'bar_place', 'discharge_place', 'complate_situation', 'date_time', 'shift_id'],
  //     types: ['NUMBER', 'SELECT', 'NUMBER', 'STRING', 'STRING', 'NUMBER', 'DATETIME', 'SELECT'],
  //     labels: ['工作性质', '品名', '使用车数', '倒运物料地点', '卸料物料地点', '完成情况', '日期', '班次']
  //   }
  // },

  // materiel_submitted: {
  //   name: 'materiel_submitted',
  //   label: '物料报送审批表',
  //   table: 'his_materiel_submitted_plan',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['id', 'company_name', 'product_name', 'vehicle_number', 'rubber_tire_number', 'discharge_place', 'vehicle_describe', 'date_time', 'shift_id'],
  //     types: ['SELECT', 'STRING', 'NUMBER', 'NUMBER', 'STRING', 'STRING', 'DATETIME', 'SELECT'],
  //     labels: ['编号', '单位', '品名', '使用车数', '胶轮车数', '卸料物料地点', '装车情况', '日期', '班次']
  //   }
  // },

  // special_vehicle_transport: {
  //   name: 'special_vehicle_transport',
  //   label: '特种车运输计划',
  //   table: 'his_special_vehicle_transport',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['id', 'model', 'vehicle_number', 'product_name', 'transport_way', 'entrucking_place', 'carray_place', 'shift', 'complate_situation', 'date_time'],
  //     types: ['INT', 'SELECT', 'STRING', 'NUMBER', 'STRING', 'STRING', 'STRING', 'STRING', 'SELECT', 'NUMBER', 'DATETIME'],
  //     labels: ['编号', '单位', '型号', '使用车数', '物料名称', '运输方式', '装车地点', '运送地点', '班次', '完成情况', '日期']
  //   }
  // },

  // special_vehicle_bar: {
  //   name: 'special_vehicle_bar',
  //   label: '特种车倒运计划表',
  //   table: 'his_special_vehicle_bar',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['id', 'group_name', 'product_name', 'vehicle_number', 'bar_place', 'transport_place', 'vehicle', 'shift', 'complate_situation', 'date_time'],
  //     types: ['INT', 'SELECT', 'STRING', 'NUMBER', 'STRING', 'STRING', 'SELECT', 'SELECT', 'NUMBER', 'DATETIME'],
  //     labels: ['编号', '队组', '物料名称', '使用车数', '倒运地点', '运往地点', '使用车辆', '班次', '完成情况', '日期']
  //   }
  // },

  event_level: {
    name: 'event_level',
    label: '告警级别',
    table: 'dat_event_level',
    keyIndex: 0,
    fields: {
      names: ['event_level_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['事件级别', '事件名称'],
      enableNull: [false, false]
    }
  },

  event_type: {
    name: 'event_type',
    label: '告警类型',
    table: 'dat_event_type',
    keyIndex: 0,
    fields: {
      names: ['event_type_id', 'name', 'event_level_id', 'is_show', 'description'],
      types: ['NUMBER', 'STRING', 'SELECT', 'SELECT', 'STRING'],
      labels: ['事件类型', '事件名称', '事件级别', '事件是否显示', '描述'],
      enableNull: [false, true, false, true, true]
    }
  },

  warn: {
    name: 'warn',
    label: '是否告警',
    table: 'dat_warn',
    keyIndex: 0,
    fields: {
      names: ['warn_id', 'name'],
      types: ['NUMBER', 'STRING'],
      labels: ['告警编号', '名称'],
      enableNull: [false, true]
    }
  },

  credentials: {
    name: 'credentials',
    label: '资格证类型管理',
    table: 'dat_credentials',
    keyIndex: 0,
    fields: {
      names: ['credentials_id', 'name', 'description', 'remind_time'],
      types: ['NUMBER', 'STRING', 'STRING', 'NUMBER'],
      labels: ['资格证编号', '资格证名称', '描述', '到期提醒周期(天)'],
      enableNull: [false, true, true, true]
    }
  },

  credentials_staff: {
    name: 'credentials_staff',
    label: '资格证信息管理',
    table: 'dat_credentials_staff',
    keyIndex: 0,
    fields: {
      names: ['credentials_staff_id', 'staff_id', 'credentials_id', 'credentials_number', 'get_credentials_time', 'expire_time', 'warn_id'], // 字段
      types: ['NUMBER', 'NUMBER', 'SELECT', 'STRING', 'DATE', 'DATE', 'SELECT'], // 字段类型
      labels: ['编号', '员工编号', '证件类型', '证件编号', '取证时间', '到期时间', '是否提醒'],
      enableNull: [false, false, false, false, true, true, true, true]
    }
  },

  sensor_type: {
    name: 'sensor_type',
    label: '传感器类型',
    table: 'dat_sensor_type',
    keyIndex: 0,
    fields: {
      names: ['sensor_type_id', 'name', 'unit'],
      types: ['NUMBER', 'STRING', 'STRING'],
      labels: ['传感器编号', '名称', '单位'],
      enableNull: [false, true, true]
    }
  },

  sensor: {
    name: 'sensor',
    label: '传感器表',
    table: 'dat_sensor',
    keyIndex: 0,
    fields: {
      names: ['sensor_id', 'sensor_type_id', 'data_source', 'work_face_id', 'driver_alarm_threshold', 'alarm_threshold', 'x', 'y', 'z', 'sensor_desc'],
      types: ['NUMBER', 'SELECT', 'NUMBER', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING'],
      labels: ['编号', '传感器类型', '数据源', '绑定工作面', '通知司机告警阈值', '撤离告警阈值', '坐标x', '坐标y', '坐标z', '备注'],
      enableNull: [false, false, true, false, true, true, true, true, true, true]
    }
  },

  sensor_reader_map: {
    name: 'sensor_reader_map',
    label: '传感器分站绑定表',
    table: 'dat_sensor_reader_map',
    keyIndex: 0,
    fields: {
      names: ['sensor_reader_map_id', 'sensor_id', 'reader_id'],
      types: ['NUMBER', 'SELECT', 'SELECT'],
      labels: ['传感器分站id', '传感器id', '分站id'],
      enableNull: [false, false, false]
    }
  },

  sensor_driver_map: {
    name: 'sensor_driver_map',
    label: '传感器司机绑定表',
    table: 'dat_sensor_driver_map',
    keyIndex: 0,
    fields: {
      names: ['sensor_driver_map_id', 'sensor_id', 'staff_id'],
      types: ['NUMBER', 'SELECT', 'SELECT'],
      labels: ['传感器司机id', '传感器id', '分站id'],
      enableNull: [false, false, false]
    }
  },
  reader_path_tof_n: {
    name: 'reader_path_tof_n',
    label: '分站覆盖范围',
    table: 'dat_reader_path_tof_n',
    keyIndex: 1,
    keyOtherIndex: 1,
    fields: {
      names: ['reader_id', 'id', 'tof_flag', 'b_x', 'b_y', 'b_z', 'e_x', 'e_y', 'e_z', 'spacing_ratio'],
      types: ['SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'],
      labels: ['分站名称', '路径编号', '路径标志', '起始点x', '起始点y', '起始点z', '结束点x', '结束点y', '结束点z', '间距比率'],
      enableNull: [false, false, false, false, false, false, false, false, false, false]
    }
  },

  ip_address: {
    name: 'ip_address',
    label: 'IP地址',
    table: 'dat_ip_address',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['ip_address_id', 'ip_begin', 'ip_end'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['IP地址编号', 'IP开始范围', 'IP结束范围'],
      enableNull: [false, false, false, false]
    }
  },
  sanlv_standart: {
    name: 'sanlv_standart',
    label: '三率标准表',
    table: 'dat_sanlv_standart',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['sanlv_standart_id', 'Excellent_Standart', 'Bad_Standart'], // 字段, md5用于更新地图
      types: ['SELECT', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['三率类型', '优秀标准', '差标准'],
      enableNull: [false, false, false]
    }
  },

  work_face: {
    name: 'work_face',
    label: '工作面表',
    table: 'dat_work_face',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['work_face_id', 'work_face_type', 'name', 'map_id', 'area_id', 'need_display', 'rank'], // 字段, md5用于更新地图
      types: ['NUMBER', 'SELECT', 'STRING', 'SELECT', 'SELECT', 'SELECT', 'NUMBER'], // 字段类型
      labels: ['工作面id', '工作面类型', '工作面名称', '地图', '区域', '界面是否显示', '排序序号'],
      enableNull: [false, false, false, false, false, true, false]
    }
  },

  month: {
    name: 'month',
    label: '月报日期设置表',
    table: 'dat_month',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['id', 'start_time', 'end_time'], // 字段, md5用于更新地图
      types: ['NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['月报id', '月报开始日期', '月报结束日期'],
      enableNull: [false, false, false]
    }
  },

  virtual_path: {
    name: 'virtual_path',
    label: '虚拟路径表',
    table: 'dat_virtual_path',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['virtual_path_id', 'name', 'type', 'obj_id', 'start_time', 'end_time'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'DATETIME', 'DATETIME'], // 字段类型
      labels: ['路径id', '路径名称', '类型(0:历史;1:手工)', '历史路径编号', '开始时间', '结束时间'],
      enableNull: [false, false, false, true, true, true]
    }
  },

  leader_arrange: {
    name: 'leader_arrange',
    label: '领导排班数据表',
    table: 'his_leader_arrange',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['staff_id', 'shift_id', 'duty_date', 'is_virtual', 'virtual_path_id', 'begin_time'], // 字段, md5用于更新地图
      types: ['NUMBER', 'NUMBER', 'DATE', 'NUMBER', 'NUMBER', 'DATETIME'], // 字段类型
      labels: ['员工id', '班次', '代班日期', '是否虚拟路径，0不是，1是', '虚拟路径id', '开始时间'],
      enableNull: [false, false, false, false, false, false, false]
    }
  },

  area_persons_dynamic_thre: {
    name: 'area_persons_dynamic_thre',
    label: '各时间段区域人数上限',
    table: 'dat_area_persons_dynamic_thre',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['apdt_id', 'area_id', 'start_time', 'end_time', 'thre_value'], // 字段, md5用于更新地图
      types: ['NUMBER', 'SELECT', 'TIME', 'TIME', 'NUMBER'], // 字段类型
      labels: ['编号', '区域名称', '开始时间', '结束时间', '人数上限'],
      enableNull: [false, false, false, false, false]
    }
  },

  // rt_person_forbid_down_mine: {
  //   name: 'rt_person_forbid_down_mine',
  //   label: '禁止下井人员表',
  //   table: 'rt_person_forbid_down_mine',
  //   keyIndex: 0, // table中key值在 field 中的位置
  //   fields: {
  //     names: ['id', 'staff_id', 'start_time', 'end_time', 'status', 'oper_time', 'oper_user'], // 字段, md5用于更新地图
  //     types: ['NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'SELECT', 'DATETIME', 'STRING'], // 字段类型
  //     labels: ['记录保存数', '员工号', '禁止下井起始时间', '禁止下井结束时间', '本记录是否启用', '新增操作时间', '新增操作员'],
  //     enableNull: [false, false, false, false, false, false, false]
  //   }
  // },

  // his_location_staff_: {
  //   name: 'his_location_staff_',
  //   label: '历史轨迹管理',
  //   table: 'his_location_staff_',
  //   keyIndex: 0,
  //   fields: {
  //     names: ['id', 'card_id', 'begin_time', 'last_time', 'map_id', 'area_id', 'speed', 'begin_pt', 'direction'], // 字段, md5用于更新地图
  //     types: ['NUMBER', 'NUMBER', 'DATETIME', 'DATETIME', 'NUMBER', 'NUMBER', 'NUMBER', 'STRING', 'NUMBER'], // 字段类型
  //     labels: ['轨迹ID', '卡号', '开始时间', '结束时间', '地图ID', '区域ID', '速度', '坐标', '方向'],
  //     enableNull: [false, false, false, false, false, false, false, true, false]
  //   }
  // },

  mdt_update: {
    name: 'mdt_update',
    label: '基础信息',
    table: 'dat_mdt_update',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['tableName', 'lastUpdate', 'lastDelete', 'remark'], // 字段, md5用于更新地图
      types: ['STRING', 'DATETIME', 'DATETIME', 'STRING'], // 字段类型
      labels: ['表名称', '表最后更新时间', '表最后删除时间', '备注'],
      enableNull: [false, true, true]
    }
  },
  drivingface_ref_point: {
    name: 'drivingface_ref_point',
    label: '掘进面基准点',
    table: 'dat_drivingface_ref_point',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['drp_id', 'drivingface_id', 'idx', 'base_x', 'base_y', 'base_z', 'offset'], // 字段, md5用于更新地图
      types: ['NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'], // 字段类型
      labels: ['基准点编号', '掘进面名称', '基准点索引', '基准点x', '基准点y', '基准点z', '偏移量'],
      enableNull: [false, false, false, false, false, true, true]
    }
  },
  reader_path_combine: {
    name: 'reader_path_combine',
    label: '路径集',
    table: 'dat_reader_path_combine',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['reader_path_combine_id', 'start_point', 'end_point'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'STRING'], // 字段类型
      labels: ['路径集编号', '起始点', '结束点'],
      enableNull: [false, false, false, false, false, true, true]
    }
  },
  rt_person_forbid_down_mine: {
    name: 'rt_person_forbid_down_mine',
    label: '禁止人员下井',
    table: 'rt_person_forbid_down_mine',
    keyIndex: 0, // table中key值在 field 中的位置
    fields: {
      names: ['staff_id', 'name', 'dept_id', 'start_time', 'oper_time', 'oper_user'], // 字段, md5用于更新地图
      types: ['NUMBER', 'STRING', 'SELECT', 'DATETIME', 'DATETIME', 'STRING'], // 字段类型
      labels: ['员工编号', '姓名', '部门', '禁止下井开始时间', '新增操作时间', '新增操作员'],
      enableNull: [false, false, true, false, true, true]
    }
  },
  goaf: {
    name: 'goaf',
    label: '采空区',
    table: 'dat_goaf',
    keyIndex: 0,
    fields: {
      names: ['goaf_id', 'name', 'path', 'need_display'],
      types: ['NUMBER', 'STRING', 'STRING', 'SELECT'],
      labels: ['编号', '名称', '路径', '检查人员是否可见'],
      enableNull: [false, false, false, false]
    }
  },
  road_segment: {
    name: 'road_segment',
    label: '路径',
    table: 'dat_road_segment',
    keyIndex: 0,
    fields: {
      names: ['road_segment_id', 'bpoint', 'epoint'],
      types: ['NUMBER', 'STRING', 'STRING'],
      labels: ['编号', '开始点', '结束点'],
      enableNull: [false, false, false]
    }
  },
  tt_inspection_route_planning: {
    name: 'tt_inspection_route_planning',
    label: '路径',
    table: 'tt_inspection_route_planning',
    keyIndex: 0,
    fields: {
      names: ['staff_id', 'name', 'status', 'route_planning', 'reader_planning'],
      types: ['NUMBER', 'STRING', 'SELECT', 'STRING', 'STRING'],
      labels: ['员工编号', '姓名', '状态', '路径信息', '分站计划'],
      enableNull: [false, false, false, false, true]
    }
  },
  sosalarm: {
    name: 'sosalarm',
    label: '声光告警',
    table: 'dat_sosalarm',
    keyIndex: 0,
    fields: {
      names: ['sosalarm_id', 'event_type_id', 'event_level_id', 'is_showSoundLight', 'light', 'sound'],
      types: ['NUMBER', 'SELECT', 'SELECT', 'SELECT', 'SELECT', 'SELECT'],
      labels: ['编号', '告警类型', '告警级别', '声光显示', '声音强度 ', '灯光亮度'],
      enableNull: [false, false, false, false, false, false]
    }
  },
  reader_rssi: {
    name: 'reader_rssi',
    label: '分站盲区间距',
    table: 'dat_reader_rssi',
    keyIndex: 0,
    fields: {
      names: ['reader_rssi_id', 'rssi_distance'],
      types: ['NUMBER', 'NUMBER'],
      labels: ['编号', '距离间隔'],
      enableNull: [false, false]
    }
  },
  device_power: {
    name: 'device_power',
    label: '设备电源',
    table: 'dat_device_power',
    keyIndex: 0,
    fields: {
      names: ['device_power_id', 'power_model', 'power_number', 'reted_supply_voltage', 'device_id', 'device_type_id', 'ip', 'install_date', 'power_limit', 'temperature_limit', 'map_id', 'area_id', 'x', 'y', 'z'],
      types: ['NUMBER', 'STRING', 'NUMBER', 'NUMBER', 'SELECT', 'SELECT', 'STRING', 'DATE', 'SELECT', 'SELECT', 'SELECT', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER'],
      labels: ['电源编号', '电源型号', '电源序号', '额定供电电压', '设备名称', '设备类型', 'IP地址', '安装日期', '电量下限(%)', '温度上限(℃)', '所属地图', '所属区域', '坐标x', '坐标y', '坐标z'],
      enableNull: [false, false, false, false, false, false, false, false, false, false, false, false, false, false, true]
    }
  },
  power_levels: {
    name: 'power_levels',
    label: '电源对应的多路电源信息',
    table: 'dat_power_levels',
    keyIndex: 0,
    fields: {
      names: ['id', 'power_levels_id', 'device_power_id', 'battery_number', 'battery_model', 'battery_type', 'rated_voltage', 'discharge_voltage', 'capacity', 'discharge_voltage_cycle', 'discharge_voltage_time'],
      types: ['NUMBER', 'NUMBER', 'SELECT', 'NUMBER', 'STRING', 'SELECT', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER', 'NUMBER'],
      labels: ['编号', '电路编号', '电源编号', '电池编号', '电池型号', '电池类型', '额定电压', '放电截止电压', '标称容量(mAH)', '强制放电周期', '标准放电时间'],
      enableNull: [false, false, false, false, false, false, false, false, false, false, false]
    }
  },
  leave: {
    name: 'leave',
    label: '区域早退',
    table: 'dat_leave',
    keyIndex: 0,
    fields: {
      names: ['area_id','shift_id', 'leave_time'],
      types: ['SELECT', 'SELECT', 'TIME'],
      labels: ['区域', '班次', '离开时间'],
      enableNull: [false, false, false]
    }
  }
}

module.exports = metadata
