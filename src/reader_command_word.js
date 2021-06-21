function getNetIP () {
  const interfaces = require('os').networkInterfaces()
  let IPAdress = []
  for (let devName in interfaces) {
    let iface = interfaces[devName]
    for (let i = 0; i < iface.length; i++) {
      let alias = iface[i]
      if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal) {
        !IPAdress.includes(alias.address) && IPAdress.push({
          ip: alias.address,
          netmask: alias.netmask
        })
        // IPAdress = alias.address
      }
    }
  }
  return IPAdress
}

// 命令字
const READER_COMMAND_WORD = {
  device_net_params: [0x93, 0xa8], // 请求/应答设备网络配置
  network_configuration: [0x3c, 0x74], // 设置/应答设备网络配置
  device_params: [0x6b, 0x1d], // 请求/应答设备参数
  device_software_update: [0x6b, 0x1d], // 软件升级前，需要先请求设备参数，获取当前软件版本信息
  device_power: [0x6b, 0x1d], // 电源信息
  device_configuration: [0x87, 0xa4], // 设置/应答设备参数
  power_discharge: [0x87, 0xa4], // 设置/应答电源放电
  power_limit: [0x87, 0xa4], // 设置电源电量下限/温度上限
  power_alarm: [0xa2, 0x00], // 应答/结束电源设备报警信息
  start_send_device_update: [0x66, 0xa4], // 开始发送设备更新程序通知
  send_device_update_frame: [0x67, 0x8a], // 发送/应答设备更新程序帧数据
  end_send_device_update: [0x68, 0x8a], // 应答/结束发送分站更新程序通知
  end_send_bigsmall_reader_update: [0x68, 0x8a], // 应答/结束发送大小分站更新程序通知
  request_device_all_parameters: [0x65, 0x8a], // 请求/应答设备所有参数信息
  clear_history_data: [0x69, 0x8a], // 清空历史数据/分站清空历史数据成功
  virtual_data_push: [0x69, 0x9a], // 虚拟数据推送通知
  reader_finish_sending_history_data: [0x83, 0x3b], // 分站历史数据发送完毕
  reader_send_time_request: [0x78, 0x3b], // 分站发送校时请求
  answer_start_send_device_update: [0x66, 0x8a], // 应答开始发送读卡分站更新程序通知
  // answer_send_bigsmall_reader_update: [0x66, 0xaa], // 应答开始发送大小分站更新程序通知
  extension_tof_realtime_position_data: [0x84, 0x3b], // 扩展tof实时定位数据
  non_ip_device: [0xa1, 0x0b], // 非IP设备向IP设备转发命令字
  non_ip_start_send_device_update: [0xa1, 0x0c], // 开始发送非IP设备的程序更新通知
  non_ip_send_device_update_frame: [0xa1, 0x0d], // 发送非IP设备的更新程序帧数据
  non_ip_end_send_bigsmall_reader_update: [0xa1, 0x0e], // 结束发送非IP设备更新程序通知
  wireless_start_send_device_update: [0xa3, 0x03], // 无线设备开始发送设备的程序更新通知
  wireless_send_device_update_frame: [0xa3, 0x04], // 无线设备发送更新程序帧数据
  wireless_end_send_bigsmall_reader_update: [0xa3, 0x05] // 无线设备结束发送设备更新程序通知
}

const PARSING_DATA_MEAN = {
  device_net_params: { // 网络参数信息
    lebels: ['设备地址', '设备类型', '控制码1', '设备类型', '控制码2', '本机IP', '控制码3', '子网掩码', '控制码4', '默认网关', '控制码5', '目标IP1', '目标端口1', 'TDOA端口1', '是否使用', '控制码6', '目标IP2', '目标端口2', 'TDOA端口2', '是否使用', '控制码7', '目标IP3', '目标端口3', 'TDOA端口3', '是否使用', '控制码8', 'MAC'],
    nums: [4, 1, 1, 1, 1, 4, 1, 4, 1, 4, 1, 4, 2, 2, 1, 1, 4, 2, 2, 1, 1, 4, 2, 2, 1, 1, 6],
    names: ['deviceAddress', 'deviceType', 'cc160', 'deviceType', 'cc161', 'ip', 'cc162', 'subnetMask', 'cc163', 'defaultGateway', 'cc164', 'aimsIP1', 'aimsPort1', 'tdoaPort1', 'enable1', 'cc165', 'aimsIP2', 'aimsPort2', 'tdoaPort2', 'enable2', 'cc166', 'aimsIP3', 'aimsPort3', 'tdoaPort3', 'enable3', 'cc167', 'mac']
  },
  device_params: { // 设备参数信息
    lebels: ['设备地址', '设备类型', '控制码1', '上传间隔', '控制码2', '重连间隔', '控制码3', '接收频点(第一路DW1000)', '控制码4', '接收频点(第二路DW1000)', '控制码5', '子设备地址', '控制码6', 'CANID', '控制码7', '组内数量', '控制码8', '程序版本', '控制码9', '天线1延迟值', '控制码10', '天线2延迟值', '控制码11', 'TDOA时间节点', '控制码12', '红绿灯是否显示背面面板', '控制码13', '分站时间同步上级分站', '控制码14', '分站区域编号', '控制码15', '红绿灯正面显示形状', '控制码16', '红绿灯反面显示形状', '控制码17', '离线正面显示时长', '控制码18', '离线背面显示时长', '控制码19', '是否上传心跳', '控制码20', '第一路DW1000发射功率', '控制码21', '第一路DW1000通信速率', '控制码22', '第一路DW1000脉冲重复频率', '控制码23', '第一路DW1000前导码', '控制码24', '第一路DW1000前导码长度', '控制码25', '第二路DW1000发射功率', '控制码26', '第二路DW1000通信速率', '控制码27', '第二路DW1000脉冲重复频率', '控制码28', '第二路DW1000前导码', '控制码29', '第二路DW1000前导码长度', '控制码30', '第二路DW1000PAC', '控制码31', '第一路DW1000的PAC', '控制码32', '广播时长', '控制码33', '设置Blink时长', '控制码34', '设置侦听Response时长', '控制码35', '设置侦听Final时长', '控制码36', '设置定位完成后休眠时长', '控制码37', '设置冲突时休眠时长', '控制码38', '设置侦听ACK时长', '控制码39', '设置Checking时长', '控制码40', '设置Ranging时长', '控制码41', '休眠状态', '控制码42', '侦听信号时长', '控制码48', '定位模式'],
    nums: [4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 2, 1, 8, 1, 8, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 1, 1, 4, 1, 1],
    names: ['deviceAddress', 'deviceType', 'cc1', 'uploadInterval', 'cc2', 'reconnectInterval', 'cc3', 'receivingFrequencyPoint1', 'cc4', 'receivingFrequencyPoint2', 'cc5', 'childDeviceAddress', 'cc6', 'canid', 'cc7', 'trafficLightNums', 'cc8', 'programVersion', 'cc9', 'antennaDelay1', 'cc10', 'antennaDelay2', 'cc11', 'tdoaTimeFrame', 'cc12', 'isShowBackside', 'cc13', 'timeSynchronization', 'cc14', 'readerAreaID', 'cc15', 'trafficLightsFontShap', 'cc16', 'trafficLightsReverseShap', 'cc17', 'trafficLightsFontColor', 'cc18', 'trafficLightsReverseColor', 'cc19', 'uploadHartbeat', 'cc20', 'transmitPower1', 'cc21', 'communicationRate1', 'cc22', 'pulseReptFrequency1', 'cc23', 'preambleCode1', 'cc24', 'preambleCodeLength1', 'cc25', 'transmitPower2', 'cc26', 'communicationRate2', 'cc27', 'pulseReptFrequency2', 'cc28', 'preambleCode2', 'cc29', 'preambleCodeLength2', 'cc30', 'PAC2', 'cc31', 'PAC1', 'cc32', 'broadcastDuration', 'cc33', 'BlinkDuration', 'cc34', 'responseDuration', 'cc35', 'FinalDuration', 'cc36', 'afterPositionDormat', 'cc37', 'confictDormat', 'cc38', 'ACKDuration', 'cc39', 'checkingDuration', 'cc40', 'rangingDuration', 'cc41', 'dormancyStatus', 'cc42', 'signalDuration', 'cc48', 'positionPattern']
  },
  device_software_update: {
    lebels: ['设备ip地址', '设备端口', '设备地址', '设备类型', '子设备类型', '子设备中继数', '控制码1', '上传间隔', '控制码2', '重连间隔', '控制码3', '接收频点(第一路DW1000)', '控制码4', '接收频点(第二路DW1000)', '控制码5', '子设备地址', '控制码6', 'CANID', '控制码7', '温度', '控制码8', '程序版本', '控制码9', '天线1延迟值', '控制码10', '天线2延迟值', '控制码11', 'TDOA时间节点', '控制码12', '分站发射功率', '控制码13', '分站时间同步上级分站', '控制码14', '分站区域编号', '控制码15', '红绿灯正面显示形状', '控制码16', '红绿灯反面显示形状', '控制码17', '红绿灯正面显示颜色', '控制码18', '红绿灯反面显示颜色', '控制码19', '通信分站是否上传心跳'],
    nums: [4, 2, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 1, 2, 1, 2, 1, 8, 1, 8, 1, 1, 1, 1, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    names: ['ip', 'port', 'deviceAddress', 'deviceType', 'childDeviceType', 'childDeviceRelaysNumber', 'cc1', 'uploadInterval', 'cc2', 'reconnectInterval', 'cc3', 'receivingFrequencyPoint1', 'cc4', 'receivingFrequencyPoint2', 'cc5', 'childDeviceAddress', 'cc6', 'canid', 'cc7', 'temperature', 'cc8', 'programVersion', 'cc9', 'antennaDelay1', 'cc10', 'antennaDelay2', 'cc11', 'tdoaTimeFrame', 'cc12', 'readerTransmitPower', 'cc13', 'timeSynchronization', 'cc14', 'readerAreaID', 'cc15', 'trafficLightsFontShap', 'cc16', 'trafficLightsReverseShap', 'cc17', 'trafficLightsFontColor', 'cc18', 'trafficLightsReverseColor', 'cc19', 'uploadHartbeat']
  },
  extension_tof_realtime_position_data: {
    labels: ['设备地址', '时间戳', '分站时间', '大小分站所属关系', '状态'],
    nums: [4, 2, 7, 1, 2],
    names: ['deviceAddress', 'timestamp', 'reader_time', 'reader_ownership', 'status']
  },
  vehicle_card: {
    labels: ['卡类型', '定位器ID', '报文时间戳', '报文类型', '电池信息', '角速度', '加速度', '定位时间戳', '天线号', '信号强度', '接收信号功率'],
    nums: [1, 4, 2, 0.5, 0.5, 1, 1, 5, 1, 2, 2],
    names: ['card_type', 'card_id', 'message_time_stamp', 'message_type', 'battery_information', 'angular_velocity', 'acceleration', 'position_stamp', 'antenna_num', 'signal_strength', 'received_signal_power']
  },
  staff_card: {
    labels: ['卡类型', '定位器ID', '报文时间戳', '报文类型', '电池信息', '呼叫状态', '加速度', '定位时间戳', '天线号', '信号强度', '接收信号功率'],
    nums: [1, 4, 2, 0.5, 0.5, 1, 1, 5, 1, 2, 2],
    names: ['card_type', 'card_id', 'message_time_stamp', 'message_type', 'battery_information', 'call_state', 'acceleration', 'position_stamp', 'antenna_num', 'signal_strength', 'received_signal_power']
  },
  start_send_device_update: {
    labels: ['设备地址', '设备类型', '版本号', '高/低区'],
    nums: [4, 1, 2, 1],
    names: ['deviceAddress', 'deviceType', 'programVersion', 'highorLow']
  },
  non_ip_start_send_device_update: {
    labels: ['设备地址', '设备类型', '版本号', '高/低区'],
    nums: [4, 1, 2, 1],
    names: ['deviceAddress', 'deviceType', 'programVersion', 'highorLow']
  },
  wireless_start_send_device_update: {
    labels: ['设备地址', '设备类型', '升级的设备类型', '升级包版本号'],
    nums: [4, 1, 1, 2],
    names: ['deviceAddress', 'deviceType', 'uploadDeviceType', 'programVersion']
  },
  device_power: {
    labels: ['设备地址', '设备类型', '控制码49', '几路电池', '控制码50', '交流供电状态', '控制码51', '交流供电电压', '控制码52', '电源箱内温度', '控制码64', '第一路电池当前电压', '控制码65', '第一路电池当前电流', '控制码66', '第一路电池当前电量', '控制码67', '第一路电池当前温度', '控制码68', '第一路电池累计充放电次数', '控制码69', '第一路电池累计充放电时间', '控制码70', '第一路功率器件当前温度', '控制码71', '第一路直流输出电压', '控制码72', '第2路电池当前电压', '控制码73', '第2路电池当前电流', '控制码74', '第2路电池当前电量', '控制码75', '第2路电池当前温度', '控制码76', '第2路电池累计充放电次数', '控制码77', '第2路电池累计充放电时间', '控制码78', '第2路功率器件当前温度', '控制码79', '第2路直流输出电压', '控制码80', '第3路电池当前电压', '控制码81', '第3路电池当前电流', '控制码82', '第3路电池当前电量', '控制码83', '第3路电池当前温度', '控制码84', '第3路电池累计充放电次数', '控制码85', '第3路电池累计充放电时间', '控制码86', '第3路功率器件当前温度', '控制码87', '第3路直流输出电压', '控制码88', '第4路电池当前电压', '控制码89', '第4路电池当前电流', '控制码90', '第4路电池当前电量', '控制码91', '第4路电池当前温度', '控制码92', '第4路电池累计充放电次数', '控制码93', '第4路电池累计充放电时间', '控制码94', '第4路功率器件当前温度', '控制码95', '第4路直流输出电压', '控制码96', '第5路电池当前电压', '控制码97', '第5路电池当前电流', '控制码98', '第5路电池当前电量', '控制码99', '第5路电池当前温度', '控制码100', '第5路电池累计充放电次数', '控制码101', '第5路电池累计充放电时间', '控制码102', '第5路功率器件当前温度', '控制码103', '第5路直流输出电压', '控制码104', '第6路电池当前电压', '控制码105', '第6路电池当前电流', '控制码106', '第6路电池当前电量', '控制码107', '第6路电池当前温度', '控制码108', '第6路电池累计充放电次数', '控制码109', '第6路电池累计充放电时间', '控制码110', '第6路功率器件当前温度', '控制码111', '第6路直流输出电压', '控制码112', '第7路电池当前电压', '控制码113', '第7路电池当前电流', '控制码114', '第7路电池当前电量', '控制码115', '第7路电池当前温度', '控制码116', '第7路电池累计充放电次数', '控制码117', '第7路电池累计充放电时间', '控制码118', '第7路功率器件当前温度', '控制码119', '第7路直流输出电压', '控制码120', '第8路电池当前电压', '控制码121', '第8路电池当前电流', '控制码122', '第8路电池当前电量', '控制码123', '第8路电池当前温度', '控制码124', '第8路电池累计充放电次数', '控制码125', '第8路电池累计充放电时间', '控制码126', '第8路功率器件当前温度', '控制码127', '第8路直流输出电压'],
    nums: [4, 1, 1, 1, 1, 1, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4],
    names: ['deviceAddress', 'deviceType', 'cc49', 'power_rode', 'cc50', 'excharge_state', 'cc51', 'supply_voltage', 'cc52', 'temperature', 'cc64', 'voltage1', 'cc65', 'electricity1', 'cc66', 'power1', 'cc67', 'temperature1', 'cc68', 'charge_discharge_num1', 'cc69', 'charge_discharge_time1', 'cc70', 'power_temperature1', 'cc71', 'dc_voltage1', 'cc72', 'voltage2', 'cc73', 'electricity2', 'cc74', 'power2', 'cc75', 'temperature2', 'cc76', 'charge_discharge_num2', 'cc77', 'charge_discharge_time2', 'cc78', 'power_temperature2', 'cc79', 'dc_voltage2', 'cc80', 'voltage3', 'cc81', 'electricity3', 'cc82', 'power3', 'cc93', 'temperature3', 'cc84', 'charge_discharge_num3', 'cc85', 'charge_discharge_time3', 'cc86', 'power_temperature3', 'cc87', 'dc_voltage3', 'cc88', 'voltage4', 'cc89', 'electricity4', 'cc90', 'power4', 'cc91', 'temperature4', 'cc92', 'charge_discharge_num4', 'cc93', 'charge_discharge_time4', 'cc94', 'power_temperature4', 'cc95', 'dc_voltage4', 'cc96', 'voltage5', 'cc97', 'electricity5', 'cc98', 'power5', 'cc99', 'temperature5', 'cc100', 'charge_discharge_num5', 'cc101', 'charge_discharge_time5', 'cc102', 'power_temperature5', 'cc103', 'dc_voltage5', 'cc104', 'voltage6', 'cc105', 'electricity6', 'cc106', 'power6', 'cc107', 'temperature6', 'cc108', 'charge_discharge_num6', 'cc109', 'charge_discharge_time6', 'cc110', 'power_temperature6', 'cc111', 'dc_voltage6', 'cc112', 'voltage7', 'cc113', 'electricity7', 'cc114', 'power7', 'cc115', 'temperature7', 'cc116', 'charge_discharge_num7', 'cc117', 'charge_discharge_time7', 'cc118', 'power_temperature7', 'cc119', 'dc_voltage7', 'cc120', 'voltage8', 'cc121', 'electricity8', 'cc122', 'power8', 'cc123', 'temperature8', 'cc124', 'charge_discharge_num8', 'cc125', 'charge_discharge_time8', 'cc126', 'power_temperature8', 'cc127', 'dc_voltage8']
  },
  end_send_device_update: {
    labels: ['设备地址', '设备类型', '升级版本', '状态标识'],
    nums: [4, 1, 2, 1],
    names: ['deviceAddress', 'deviceType', 'programVersion', 'upload_state']
  },
  non_ip_end_send_bigsmall_reader_update: {
    labels: ['设备地址', '设备类型', '升级版本', '状态标识'],
    nums: [4, 1, 2, 1],
    names: ['deviceAddress', 'deviceType', 'programVersion', 'upload_state']
  },
  wireless_end_send_bigsmall_reader_update: {
    labels: ['设备地址', '设备类型', '升级版本', '状态标识'],
    nums: [4, 1, 2, 1],
    names: ['deviceAddress', 'deviceType', 'programVersion', 'upload_state']
  }
}

const NETWORK = {
  160: 'deviceType',
  161: 'ip',
  162: 'subnetMask',
  163: 'defaultGateway',
  167: 'mac'
}

const DEVICE = {
  1: 'uploadInterval',
  2: 'reconnectInterval',
  3: 'receivingFrequencyPoint1',
  4: 'receivingFrequencyPoint2',
  5: 'childDeviceAddress',
  6: 'canid',
  7: 'trafficLightNums',
  8: 'programVersion',
  9: 'antennaDelay1',
  10: 'antennaDelay2',
  11: 'tdoaTimeFrame',
  12: 'isShowBackside',
  13: 'timeSynchronization',
  14: 'readerAreaID',
  15: 'trafficLightsFontShap',
  16: 'trafficLightsReverseShap',
  17: 'trafficLightsFontColor',
  18: 'trafficLightsReverseColor',
  19: 'uploadHartbeat',
  20: 'transmitPower1',
  21: 'communicationRate1',
  22: 'pulseReptFrequency1',
  23: 'preambleCode1',
  24: 'preambleCodeLength1',
  25: 'transmitPower2',
  26: 'communicationRate2',
  27: 'pulseReptFrequency2',
  28: 'preambleCode2',
  29: 'preambleCodeLength2',
  30: 'PAC1',
  31: 'PAC2',
  32: 'broadcastDuration',
  33: 'BlinkDuration',
  34: 'responseDuration',
  35: 'FinalDuration',
  36: 'afterPositionDormat',
  37: 'confictDormat',
  38: 'ACKDuration',
  39: 'checkingDuration',
  40: 'rangingDuration',
  41: 'dormancyStatus',
  42: 'signalDuration',
  48: 'positionPattern',
  49: 'power_rode',
  50: 'excharge_state',
  51: 'supply_voltage',
  52: 'temperature',
  64: 'voltage1',
  65: 'electricity1',
  66: 'power1',
  67: 'temperature1',
  68: 'charge_discharge_num1',
  69: 'charge_discharge_time1',
  70: 'power_temperature1',
  71: 'dc_voltage1',
  72: 'voltage2',
  73: 'electricity2',
  74: 'power2',
  75: 'temperature2',
  76: 'charge_discharge_num2',
  77: 'charge_discharge_time2',
  78: 'power_temperature2',
  79: 'dc_voltage2',
  80: 'voltage3',
  81: 'electricity3',
  82: 'power3',
  83: 'temperature3',
  84: 'charge_discharge_num3',
  85: 'charge_discharge_time3',
  86: 'power_temperature3',
  87: 'dc_voltage3',
  88: 'voltage4',
  89: 'electricity4',
  90: 'power4',
  91: 'temperature4',
  92: 'charge_discharge_num4',
  93: 'charge_discharge_time4',
  94: 'power_temperature4',
  95: 'dc_voltage4',
  96: 'voltage5',
  97: 'electricity5',
  98: 'power5',
  99: 'temperature5',
  100: 'charge_discharge_num5',
  101: 'charge_discharge_time5',
  102: 'power_temperature5',
  103: 'dc_voltage5',
  104: 'voltage6',
  105: 'electricity6',
  106: 'power6',
  107: 'temperature6',
  108: 'charge_discharge_num6',
  109: 'charge_discharge_time6',
  110: 'power_temperature6',
  111: 'dc_voltage6',
  112: 'voltage7',
  113: 'electricity7',
  114: 'power7',
  115: 'temperature7',
  116: 'charge_discharge_num7',
  117: 'charge_discharge_time7',
  118: 'power_temperature7',
  119: 'dc_voltage7',
  120: 'voltage8',
  121: 'electricity8',
  122: 'power8',
  123: 'temperature8',
  124: 'charge_discharge_num8',
  125: 'charge_discharge_time8',
  126: 'power_temperature8',
  127: 'dc_voltage8'
}

const POWER = {
  49: 'power_rode',
  50: 'excharge_state',
  51: 'supply_voltage',
  52: 'temperature',
  64: 'voltage1',
  65: 'electricity1',
  66: 'power1',
  67: 'temperature1',
  68: 'charge_discharge_num1',
  69: 'charge_discharge_time1',
  70: 'power_temperature1',
  71: 'dc_voltage1',
  72: 'voltage2',
  73: 'electricity2',
  74: 'power2',
  75: 'temperature2',
  76: 'charge_discharge_num2',
  77: 'charge_discharge_time2',
  78: 'power_temperature2',
  79: 'dc_voltage2',
  80: 'voltage3',
  81: 'electricity3',
  82: 'power3',
  83: 'temperature3',
  84: 'charge_discharge_num3',
  85: 'charge_discharge_time3',
  86: 'power_temperature3',
  87: 'dc_voltage3',
  88: 'voltage4',
  89: 'electricity4',
  90: 'power4',
  91: 'temperature4',
  92: 'charge_discharge_num4',
  93: 'charge_discharge_time4',
  94: 'power_temperature4',
  95: 'dc_voltage4',
  96: 'voltage5',
  97: 'electricity5',
  98: 'power5',
  99: 'temperature5',
  100: 'charge_discharge_num5',
  101: 'charge_discharge_time5',
  102: 'power_temperature5',
  103: 'dc_voltage5',
  104: 'voltage6',
  105: 'electricity6',
  106: 'power6',
  107: 'temperature6',
  108: 'charge_discharge_num6',
  109: 'charge_discharge_time6',
  110: 'power_temperature6',
  111: 'dc_voltage6',
  112: 'voltage7',
  113: 'electricity7',
  114: 'power7',
  115: 'temperature7',
  116: 'charge_discharge_num7',
  117: 'charge_discharge_time7',
  118: 'power_temperature7',
  119: 'dc_voltage7',
  120: 'voltage8',
  121: 'electricity8',
  122: 'power8',
  123: 'temperature8',
  124: 'charge_discharge_num8',
  125: 'charge_discharge_time8',
  126: 'power_temperature8',
  127: 'dc_voltage8'
}

const MAPPING = {
  network_configuration: 'device_net_params',
  device_configuration: 'device_params',
  power_discharge: 'device_power'
}

// 解析截取到的各个数据
function analyticalData (name, data) {
  data = Buffer.isBuffer(data) ? data : Buffer.from(data)
  const dataArray = Buffer.isBuffer(data) ? data.toJSON().data : data
  switch (name) {
    case 'ip':
    case 'deviceIPAddress':
    case 'subnetMask':
    case 'defaultGateway':
    case 'aimsIP1':
    case 'aimsIP2':
    case 'aimsIP3':
      return dataArray.join('.')
    case 'port':
    case 'aimsPort1':
    case 'aimsPort2':
    case 'aimsPort3':
    case 'tdoaPort1':
    case 'tdoaPort2':
    case 'tdoaPort3':
    case 'trafficLightTimeInterval':
      return data.readUInt16BE(0)
    case 'programVersion':
      return `${dataArray[0]}.${dataArray[1].toString().padStart(3, 0)}`
    case 'deviceAddress':
    case 'timeSynchronization':
    case 'childDeviceAddress':
    case 'broadcastDuration':
    case 'BlinkDuration':
    case 'responseDuration':
    case 'FinalDuration':
    case 'afterPositionDormat':
    case 'confictDormat':
    case 'ACKDuration':
    case 'checkingDuration':
    case 'rangingDuration':
    case 'signalDuration':
      return data.readUInt32BE(0)
    case 'mac':
      return parsingMac(dataArray)
    case 'antennaDelay1':
    case 'antennaDelay2':
      return parseFloat(data.readDoubleLE(0).toFixed(3), 10)
    case 'supply_voltage':
    case 'temperature':
    case 'voltage1':
    case 'voltage2':
    case 'voltage3':
    case 'voltage4':
    case 'voltage5':
    case 'voltage6':
    case 'voltage7':
    case 'voltage8':
    case 'temperature1':
    case 'temperature2':
    case 'temperature3':
    case 'temperature4':
    case 'temperature5':
    case 'temperature6':
    case 'temperature7':
    case 'temperature8':
      return data.readFloatBE(0)
    default:
      return dataArray[0]
  }
}

// 获取数组前n项和
function getNumArrayTotal (n, arr) {
  var total = arr.reduce(function (pre, cur, index, arr) {
    if (index > n - 1) {
      return pre + 0
    }
    return pre + cur
  })
  return total
}

const PARSING_RESPONSE = function (name, data, keyname) {
  let result = parsingResponse(data, name)
  if (name.includes('send_device_update_frame') || name.includes('end_send')) {
    keyname = name
  }
  return {
    cmd: `${keyname || name}_response`,
    data: result
  }
}

const CRCTABLE = [0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
  0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
  0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
  0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
  0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
  0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
  0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
  0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
  0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
  0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
  0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
  0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
  0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
  0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
  0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
  0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
  0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
  0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
  0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
  0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
  0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
  0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
  0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
  0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
  0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
  0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
  0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
  0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
  0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
  0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
  0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
  0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
]

function getCrc (data, len) {
  var crc = 0
  for (var i = 0; i < len; i++) {
    crc = (CRCTABLE[(crc >> 8) ^ data[i] & 0xff] ^ (crc << 8)) & 0xffff
  }
  return crc & 0xffff
}

/**
 * 验证收到的消息是否正确
 * @param {*} data 命令字
 * @param {*} length 长度
 * @param {*} crcRes 验证crc
 */
function judgeCrc (data, length, crcRes) {
  let verificationCrc = getCrc(data, length)
  let crcResToJson = crcRes.toJSON().data
  let crc = crcResToJson[0] << 8 | crcResToJson[1]
  if (verificationCrc === crc) {
    return true
  }
  return false
}

// 数字转字节
function NumberTurnBuf (value, offset, byteLength) {
  const buf = Buffer.allocUnsafe(byteLength)
  buf.writeUIntBE(value, offset, byteLength) // 大端写入
  return buf
}

// 将client传来的设置值转为字节表示
function trunByte (cmd, value) {
  let buf = null
  switch (cmd) {
    case 'ip':
    case 'aimsIP1':
    case 'aimsIP2':
    case 'aimsIP3':
    case 'defaultGateway':
    case 'subnetMask':
      return Buffer.from(value.split('.').map(item => Number(item)))
    case 'port':
    case 'aimsPort1':
    case 'aimsPort2':
    case 'aimsPort3':
    case 'tdoaPort1':
    case 'tdoaPort2':
    case 'tdoaPort3':
    case 'temperature':
    case 'programVersion':
    case 'trafficLightTimeInterval':
      return NumberTurnBuf(value, 0, 2)
    case 'deviceAddress':
    case 'childDeviceAddress':
    case 'timeSynchronization':
    case 'broadcastDuration':
    case 'BlinkDuration':
    case 'responseDuration':
    case 'FinalDuration':
    case 'afterPositionDormat':
    case 'confictDormat':
    case 'ACKDuration':
    case 'checkingDuration':
    case 'rangingDuration':
    case 'signalDuration':
    case 'cardID':
      return NumberTurnBuf(value, 0, 4)
    case 'antennaDelay1':
    case 'antennaDelay2':
      buf = Buffer.allocUnsafe(8)
      buf.writeDoubleLE(parseFloat(value, 10), 0)
      return buf
    case 'mac':
      return macTurnBuf(value)
    case 'x':
    case 'y':
    case 'z':
      buf = Buffer.allocUnsafe(4)
      buf.writeFloatBE(parseFloat(value, 10), 0)
      return buf
    default:
      return Buffer.from([value])
  }
}

function sortWholeData (data, cmd, fixNum) {
  if (!MAPPING[cmd]) return
  let names = PARSING_DATA_MEAN[MAPPING[cmd]].names.slice(0, fixNum)
  let bufResult = null
  for (let i = 0; i < names.length; i++) {
    let name = names[i]
    let result = data[name]
    if (result) {
      bufResult = Buffer.concat([bufResult || Buffer.from([]), result])
    }
  }
  return bufResult
}

function concatModifyData (data, cmd, fixNum) {
  if (!MAPPING[cmd]) return
  let names = PARSING_DATA_MEAN[MAPPING[cmd]].names.slice(fixNum)
  let cc = 0, oldcc = 0
  let bufResult = null
  for (let i = 0; i < names.length; i++) {
    let name = names[i]
    if (/cc[0-9]{1,}$/.test(name)) {
      cc = parseInt(name.slice(2), 10)
      continue
    }
    let result = data[name]
    if (result) {
      bufResult = cc !== oldcc ? Buffer.concat([bufResult || Buffer.from([]), Buffer.from([cc]), result])
                                : Buffer.concat([bufResult || Buffer.from([]), result])
    }
    oldcc = cc
  }
  return bufResult
}

// 处理拼接设置网络参数信息
function concatNetworkConfiguration (command, msg, fixNum) {
  let result = null
  let parentMsg = {}
  for (let key in msg) {
    if (key === 'nets' || key === 'ip' || key === 'isIP') continue
    let cmd = key
    let value = msg[key]
    if (key === 'data') {
      let childMsg = {}
      for (let childKey in msg.data) {
        cmd = childKey
        value = msg.data[childKey]
        childMsg[cmd] = trunByte(cmd, value)
      }
      result = result ? Buffer.concat([result, concatModifyData(childMsg, command, fixNum)]) : concatModifyData(childMsg, command, fixNum) // 拼接修改的数据
    } else {
      parentMsg[key] = trunByte(cmd, value)
    }
  }
  result = result ? Buffer.concat([sortWholeData(parentMsg, command, fixNum), result]) : sortWholeData(parentMsg, command, fixNum)
  return result
}

function getDeviceBasicBuf (data) {
  let {deviceAddress, deviceType} = data
  // 设备ID（4个字节）+设备类型(1字节)(全为0xFF表示请求所有设备参数信息)。
  let deviceAddressBuf = deviceAddress ? trunByte('deviceAddress', deviceAddress) : Buffer.from([0xff, 0xff, 0xff, 0xff])
  let deviceTypeBuf = Number(deviceType) === 597 ? Buffer.from([0xff]) : trunByte('deviceType', deviceType)
  return {deviceAddressBuf, deviceTypeBuf}
}

/**
 * 获取数据区
 * @param {*} cmd 请求信息
 * @param {*} data
 */
function obtainData (cmd, data) {
  let deviceBasicBuf = null
  switch (cmd) {
    case 'device_net_params': // 请求网络参数
    case 'device_params': // 请求设备参数
    case 'device_software_update':
    case 'device_power': // 设备电源参数
    case 'wireless_start_send_device_update': // 无线升级开始发送更新程序通知
      deviceBasicBuf = getDeviceBasicBuf(data)
      return Buffer.concat([deviceBasicBuf.deviceAddressBuf, deviceBasicBuf.deviceTypeBuf])
    case 'start_send_device_update': // 开始发送设备更新程序通知
    case 'non_ip_start_send_device_update': // 开始发送非IP设备更新程序通知
      deviceBasicBuf = getDeviceBasicBuf(data)
      let sendip = getNetIP()[0].ip
      let {port} = data
      let ipBuf = trunByte('ip', sendip)
      let portBuf = trunByte('port', port)
      return Buffer.concat([ipBuf, portBuf, deviceBasicBuf.deviceAddressBuf, deviceBasicBuf.deviceTypeBuf])
    case 'network_configuration':
    case 'device_configuration':
    case 'power_discharge':
      return concatNetworkConfiguration(cmd, data, 2) // cmd === 'network_configuration' ? 2 : 6
    case 'send_device_update_frame':
    case 'non_ip_send_device_update_frame':
    case 'wireless_send_device_update_frame':
      const {sendData} = data
      return sendData
    case 'end_send_bigsmall_reader_update':
    case 'non_ip_end_send_bigsmall_reader_update':
    case 'wireless_end_send_bigsmall_reader_update':
    case 'power_limit':
      return Buffer.from([])
  }
}

function dealNetworkConfigurationControlCodeData (controlCode, datas) {
  let name = NETWORK[controlCode[0]]
  let data = null
  let num = null
  switch (controlCode[0]) {
    case 160:
      name = NETWORK[controlCode]
      data = datas.slice(1, 2)
      num = 2
      break
    case 161:
    case 162:
    case 163:
      name = NETWORK[controlCode]
      data = datas.slice(1, 5)
      num = 5
      break
    case 167:
      name = NETWORK[controlCode]
      data = datas.slice(1, 7)
      num = 7
      break
    case 164:
    case 165:
    case 166:
      let cc = controlCode - 163
      name = [`aimsIP${cc}`, `aimsPort${cc}`, `tdoaPort${cc}`, `enable${cc}`]
      num = 10
      return {
        result: [
          analyticalData(`aimsIP${cc}`, datas.slice(1, 5)),
          analyticalData(`aimsPort${cc}`, datas.slice(5, 7)),
          analyticalData(`tdoaPort${cc}`, datas.slice(7, 9)),
          analyticalData(`enable${cc}`, datas.slice(9, 10))
        ],
        name,
        num
      }
    default:
      return {
        result: null,
        name: null,
        num: null
      }
  }
  return {
    result: analyticalData(name, data),
    name,
    num
  }
}

function dealDeviceConfigurationControlCodeData (controlCode, datas) {
  const codeEight = [9, 10]
  const codeTwo = [8]
  const codeFour = [5, 13, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42]
  let name = DEVICE[controlCode[0]]
  let data = null
  let num = null
  let code = controlCode[0]
  switch (true) {
    case codeTwo.includes(code):
      data = datas.slice(1, 3)
      num = 3
      break
    case codeEight.includes(code):
      data = datas.slice(1, 9)
      num = 9
      break
    case codeFour.includes(code):
      data = datas.slice(1, 5)
      num = 5
      break
    case code <= 50:
      data = datas.slice(1, 2)
      num = 2
      break
    case code <= 127:
      data = datas.slice(1, 5)
      num = 5
      break
    default:
      return {
        result: null,
        num: null,
        name: null
      }
  }
  return {
    result: analyticalData(name, data),
    num,
    name
  }
}

function dealPowerControlCodeDate (controlCode, datas) {
  let name = POWER[controlCode[0]]
  let data = null
  let num = null
  switch (controlCode[0]) {
    case 49:
    case 50:
      data = datas.slice(1, 2)
      num = 2
      break
    default:
      data = datas.slice(1, 5)
      num = 5
  }
  return {
    result: analyticalData(name, data),
    num,
    name
  }
}

function dealDifferentCmdname (cmdname, controlCode, dealData) {
  switch (cmdname) {
    case 'device_net_params':
    case 'network_configuration':
      return dealNetworkConfigurationControlCodeData(controlCode, dealData)
    case 'device_power':
      return dealPowerControlCodeDate(controlCode, dealData)
    default:
      return dealDeviceConfigurationControlCodeData(controlCode, dealData)
  }
}

function parsingConfigurationRes (datas, msg, cmdname) {
  let dealData = [...datas]
  let startnum = 0
  while (dealData.length > 0) {
    let controlCode = dealData.slice(startnum, startnum + 1)
    const {result, name, num} = dealDifferentCmdname(cmdname, controlCode, dealData)
    if (!result && result !== 0 && !isNaN(result)) break
    if (Array.isArray(name)) {
      name.forEach((item, index) => {
        msg[item] = result[index]
      })
    } else {
      msg[name] = result
    }
    dealData = dealData.slice(num)
  }
  return msg
}

function getCrcBuffer (commandWordBuffer, dataBuffer) {
  // 校验字节包括2个字节的命令字， N个字节的数据区
  let crc = getCrc(Buffer.concat([commandWordBuffer, dataBuffer]), commandWordBuffer.length + dataBuffer.length)
  // crc校验
  let crcData = [(crc >> 8) & 0xFF, crc & 0xFF]
  let crcBuffer = Buffer.from(crcData)
  return crcBuffer
}

// 获取非IP设备数据结构
function obtainUnIPDatas (cmd, data) {
  let {deviceAddress, deviceType, unIPDeviceAddress, unIPDeviceType} = data
  let deviceAddressBuf = deviceAddress ? trunByte('deviceAddress', deviceAddress) : Buffer.from([0xff, 0xff, 0xff, 0xff])
  let deviceTypeBuf = Number(deviceType) === 597 ? Buffer.from([0xff]) : trunByte('deviceType', deviceType)

  let unIPDeviceAddressBuf = unIPDeviceAddress ? trunByte('deviceAddress', unIPDeviceAddress) : Buffer.from([0xff, 0xff, 0xff, 0xff])
  let unIPDeviceTypeBuf = Number(unIPDeviceType) === 597 ? Buffer.from([0xff]) : trunByte('deviceType', unIPDeviceType)
  let unIPdeviceParams = handleBuffer([0x6b, 0x1d], cmd, {deviceAddress: unIPDeviceAddress, deviceType: unIPDeviceType}) // 请求非IP设备参数，数据长度+命令字+CRC
  let dataBuf = Buffer.concat([deviceAddressBuf, deviceTypeBuf, unIPDeviceAddressBuf, unIPDeviceTypeBuf, unIPdeviceParams])
  let resultBuffer = handleBuffer([0xa1, 0x0b], cmd, null, dataBuf)
  return resultBuffer
}

function handleUnIPBuffer (commandWord, cmd, data, sendMag, ipdatas, ip) {
  // deviceType === 1：带IP大分站，其余都为不带IP设备
  let {deviceType, deviceAddress, isIP} = data
  if (cmd === 'device_configuration' || cmd === 'power_discharge' || cmd === 'device_power') {
    // 修改非IP设备参数信息时，需要携带IP设备和非IP设备的设备id和设备类型
    // if (port !== 0) return sendMag
    if (isIP) return {sendMag}
    let {ipDeviceAddress, ipDeviceType} = ipdatas.get(ip) || {}
    let aimsIPDeviceAddress = trunByte('deviceAddress', ipDeviceAddress)
    let aimsIPDeviceType = trunByte('deviceType', ipDeviceType)

    let aimsDeviceAddress = trunByte('deviceAddress', deviceAddress)
    let aimsDeviceType = trunByte('deviceType', deviceType)

    let dataBuffer = Buffer.concat([aimsIPDeviceAddress, aimsIPDeviceType, aimsDeviceAddress, aimsDeviceType, sendMag])

    let resultBuffer = handleBuffer([0xa1, 0x0b], cmd, null, dataBuffer)
    return {resultBuffer}
  } else if (cmd === 'device_params' || cmd === 'device_software_update') {
    // 请求设备参数，升级文件前请求设备参数，需要判断非IP设备
    let resultBuffer = obtainUnIPDatas(cmd, data)
    return {sendMag, resultBuffer}
  } else if (cmd === 'non_ip_start_send_device_update') {
    let aimsIPDeviceAddress = trunByte('deviceAddress', deviceAddress)
    let aimsIPDeviceType = trunByte('deviceType', deviceType)
    let dataBuffer = Buffer.concat([aimsIPDeviceAddress, aimsIPDeviceType])
    let resultBuffer = handleBuffer([0xa1, 0x0c], cmd, null, dataBuffer)
    return {resultBuffer}
  } else if (cmd === 'power_limit') {
    let limitDef = {
      'power_limit': '0x32',
      'temperature_limit': '0x33'
    }
    let bufferData = []
    for (let j = 0; j < Object.keys(data.limit).length; j++) {
      let key = Object.keys(data.limit)[j]
      let value = '0x' + data.limit[key]
      bufferData.push(limitDef[key], value)
    }
    bufferData = Buffer.from(bufferData)
    let aimsDeviceAddress = trunByte('deviceAddress', deviceAddress)
    // let aimsDeviceType = trunByte('deviceType', deviceType)
    let aimsDeviceType = Buffer.from([0x0c])
    let dataBuffer = Buffer.concat([aimsDeviceAddress, aimsDeviceType, bufferData])
    let powerBuffer = handleBuffer(commandWord, cmd, null, dataBuffer)
    let warpBuffer = Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff])
    let dealBuffer = Buffer.concat([warpBuffer, aimsDeviceAddress, aimsDeviceType, powerBuffer])
    // sendMag = handleBuffer([0xa1, 0x0b], 'non_ip_device', null, dealBuffer)
    sendMag = Buffer.from([0x00, 0x1d, 0xa1, 0x0b, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x1e, 0x0c, 0x00, 0x0d, 0x87, 0xa4, 0x00, 0x00, 0x00, 0x1e, 0x0c, 0x32, 0x05, 0x33, 0x95, 0x68, 0xbc, 0x73, 0xa9])
    // sendMag = Buffer.from([0x00, 0x1b, 0xa1, 0x0b,0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x1e, 0x0c, 0x00, 0x0b, 0x87, 0xa4, 0x00, 0x00, 0x00, 0x1e, 0x0c, 0x32, 0x20, 0x0c,0x94, 0xb2, 0x89])
    return {sendMag}
  }
  return {sendMag}
}

function handleBuffer (commandWord, cmd, data, dataBuf) {
  // 命令字
  let commandWordBuffer = Buffer.from(commandWord)
  // 数据区
  // let dataBuffer = data ? Buffer.from(data) : Buffer.from([])
  let dataBuffer = dataBuf || obtainData(cmd, data)
  let crcBuffer = getCrcBuffer(commandWordBuffer, dataBuffer)
  let dataTotalLength = commandWord.length + dataBuffer.length + 2
  let dataTotalLengthBuffer = Buffer.from([(dataTotalLength >> 8) & 0xFF, dataTotalLength & 0xFF])
  // 2个字节长度+2个字节命令字+n个字节数据区+2个字节校验码
  return Buffer.concat([dataTotalLengthBuffer, commandWordBuffer, dataBuffer, crcBuffer])
}

function parsingMac (data) {
  let str = ''
  data.forEach(item => {
    str += `.${item.toString(16).toLocaleUpperCase()}`
  })
  str = str.replace('.', '')
  return str
}

function macTurnBuf (value) {
  let data = value.split('.')
  let buf = Buffer.from([])
  data.forEach(item => {
    buf = Buffer.concat([buf, NumberTurnBuf(parseInt(`0x${item}`), 0, 1)])
  })
  return buf
}

function dealSliceData (nums, i, name, dataArray) {
  let num = nums[i]
  let sumNum = i === 0 ? 0 : getNumArrayTotal(i, nums)
  let sliceData = dataArray.slice(sumNum, sumNum + num)
  return analyticalData(name, sliceData)
}

// 不按顺序解析设备发来的回复消息
function parsingModifyResponse (dataArray, name, isDefName) {
  // 回复的固定开头数据
  const fixedData = {
    names: ['deviceAddress', 'deviceType'],
    nums: [4, 1]
  }
  let names = fixedData.names
  let nums = fixedData.nums
  let sumNum = fixedData.nums.reduce((pre, cur) => pre + cur)
  let msg = {}
  for (let i = 0; i < names.length; i++) {
    let name = names[i]
    msg[name] = dealSliceData(nums, i, name, dataArray)
  }
  // 存放修改后的数据
  msg.data = {}
  // 解析包含控制码的数据
  let specialDatas = dataArray.slice(sumNum)
  parsingConfigurationRes(specialDatas, isDefName ? msg : msg.data, name)
  return msg
}

// 解析设备发来的数据
function parsingDeviceResponse (def, dataArray, name) {
  let {names, nums} = def
  let msg = {}
  for (let i = 0; i < names.length; i++) {
    let name = names[i]
    if (/^cc[0-9]{1,}/.test(name)) continue
    msg[name] = dealSliceData(nums, i, name, dataArray)
  }
  return msg
}

// 解析实时调试数据
function parsingRealTimeDebugData (def, dataArray, name) {
  let {nums} = def
  let length = dataArray.length
  let sum = nums.reduce((pre, cur) => pre + cur)
  let readerMsg = parsingDeviceResponse(def, dataArray, name)
  // 只有分站数据
  if (length === sum) return readerMsg
}

// 解析设备升级回复消息
function parsingSendDeviceUpdateFrame (dataArray) {
  return dataArray
}

// 解析设备参数
function parsingResponse (data, name) {
  const defName = ['device_net_params', 'device_params']

  let dataArray = data
  let def = PARSING_DATA_MEAN[name]
  if (name.includes('send_device_update_frame')) return parsingSendDeviceUpdateFrame(dataArray)
  if (name === 'extension_tof_realtime_position_data') return parsingRealTimeDebugData(def, dataArray, name)

  if (def && !defName.includes(name)) return parsingDeviceResponse(def, dataArray, name)

  // 修改为不按配置文件读取数据，按照控制码读取数据
  return parsingModifyResponse(dataArray, name, defName.includes(name))
}

function parsingNonIP (data, name) {
  let lenCommonwordLength = 14
  let originIPDeviceAddressBuf = data.slice(4, 8)
  // let originIPDeviceAddress = analyticalData('deviceAddress', originIPDeviceAddressBuf.toJSON().data)
  let originIPDeviceAddress = analyticalData('deviceAddress', originIPDeviceAddressBuf)
  let originIPDeviceTypeBuf = data.slice(8, 9)
  // let originIPDeviceType = analyticalData('deviceType', originIPDeviceTypeBuf.toJSON().data)
  let originIPDeviceType = analyticalData('deviceType', originIPDeviceTypeBuf)
  let nIpName = `non_ip_${name}`
  let oresultMsg = {originIPDeviceAddress, originIPDeviceType}
  let dataArea = data.slice(lenCommonwordLength, data.length - 2)
  return {nIpName, oresultMsg, dataArea}
}

 // 解析接收到的分站数据
function parsingData (data, name, resultMsg) {
  let dataLength = data.length - 4

  let commandWord = data.slice(2, 4) // 命令字
  let command = data.slice(2, data.length - 2) // 校验数据
  // 返回的crc
  let crcResponce = data.slice(data.length - 2, data.length)
  // crc校验
  let isCorrect = judgeCrc(command, dataLength, crcResponce)
  // 用的假数据，暂时隐藏验证crc
  if (!isCorrect) return

  let parsingResult = null

  for (let key in READER_COMMAND_WORD) {
    if (commandWord.equals(Buffer.from(READER_COMMAND_WORD[key]))) {
      let lenCommonwordLength = 4
      if (key === 'non_ip_device') {
        let {nIpName, oresultMsg, dataArea} = parsingNonIP(data, name)
        name = nIpName
        resultMsg = oresultMsg
        if (dataArea.length <= 0) return
        return parsingData(dataArea, name, resultMsg)
      }
      parsingResult = PARSING_RESPONSE(key, data.slice(lenCommonwordLength, data.length - 2), name)
      if (resultMsg) {
        parsingResult.data['originIPDeviceAddress'] = resultMsg.originIPDeviceAddress
        parsingResult.data['originIPDeviceType'] = resultMsg.originIPDeviceType
      }
      break
    }
  }
  return parsingResult
}

export {READER_COMMAND_WORD, PARSING_RESPONSE, getCrcBuffer, judgeCrc, handleUnIPBuffer, handleBuffer, analyticalData, getNetIP, parsingData, NumberTurnBuf, getCrc, trunByte}
