let path = require('path')

const APP_ROOT = path.resolve(__dirname, '../../..') // the app installed root: /loc/
const SERVER_ROOT = path.resolve(APP_ROOT, `./gis-server-dgd`)

const TMP_ROOT = path.resolve(SERVER_ROOT, `./tmp`)
const CLIENT_ROOT = path.resolve(APP_ROOT, `./gis-client-dgd`)

const CLIENT_STATIC_ROOT = path.resolve(CLIENT_ROOT, `./dist`) // 静态资源目录

const RESOURCE_ROOT = path.resolve(CLIENT_STATIC_ROOT, `./resource`)
const DATAFILE_ROOT = path.resolve(CLIENT_STATIC_ROOT, `./datafiles`)

let config = {
  // ip: '192.168.0.8',// 链接远程服务器进行调试，2016.10.29
  port: 8086,
  secret: 'Hello',
  routes: {
    login: '/account/login',
    logout: '/account/logout'
  },
  SESSION_TIMEOUT: 21600, // 默认的 session 超时时间：半个小时 ＝ 30*60

  COLLECTOR: 'COLLECTOR', // 采集服务器
  MONITOR: 'MONITOR', // 监视客户端
  STANDBY: 'STANDBY', // 离开监视界面，不接收实时位置 PUSH

  SPECIAL: 'SPECIAL', // hxtx特殊客户端
  CHECKRANGE: 'CHECKRANGE', // 检查用户&只看白名单用户

  // CLIENT_STATIC_DIR: `${CLIENT_ROOT}/dist`,
  CLIENT_STATIC_DIR: CLIENT_STATIC_ROOT, // 静态资源目录
  // used for upload / download
  FileDir: {
    tmp: TMP_ROOT,
    tmap: `${TMP_ROOT}/map`,
    tstaff: `${TMP_ROOT}/staff`,
    tvehicle: `${TMP_ROOT}/vehicle`,
    tbin: `${TMP_ROOT}/bin`,

    resource: RESOURCE_ROOT,
    map: `${RESOURCE_ROOT}/map`,
    staff: `${RESOURCE_ROOT}/staff`,
    vehicle: `${RESOURCE_ROOT}/vehicle`,
    bin: `${RESOURCE_ROOT}/bin`,

    datafiles: DATAFILE_ROOT,
    csv: `${DATAFILE_ROOT}/csv`,
    pdf: `${DATAFILE_ROOT}/pdf`
  },
  ChunkSize: 1024 * 1024, // 单次上传下载的文件块大小：1 MB
  FileBufferSize: 10 * 1024 * 1024, // 临时缓存的文件大小：10 MB

  // if the listed meta data updated, need to inform the collector.
  NeedInformCollectorList: ['setting', 'card', 'map', 'area', 'reader', 'antenna', 'path', 'drivingface', 'drivingface_render', 'dat_drivingface_warning_point', 'rules', 'vehicle_extend', 'staff_extend', 'staff_extend_ck', 'drivingface_vehicle', 'dat_handup_vehicle', 'light', 'lights_binding', 'lights_group', 'geofault', 'reader_path_tof_n', 'map_gis', 'landmark', 'work_face', 'coalface', 'coalface_vehicle', 'area_reader', 'area_persons_dynamic_thre', 'drivingface_ref_point', 'tt_inspection_route_planning']
}

module.exports = config
