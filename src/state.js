const CARD = {
  card_id: 0,
  x: 1,
  y: 2,
  down_time: 3,
  // enter_area_time: 4,
  rec_time: 4,
  work_time: 5,
  map_id: 6,
  // area_id: 8,
  dept_id: 7,
  state_card: 8,
  state_object: 9,
  state_biz: 10,
  speed: 11,
  occupation_level_id: 12,
  td_vehicle: 13, // 当天是否出车
  area_info_array: 14 // 卡的区域列表信息，数组格式
  // mark_id: 14,
  // mark_direction: 15,
  // mark_distance: 16,
  // occupation_level_id: 13
}

const VCARD = {
  card_id: 0,
  x: 1,
  y: 2,
  down_time: 3,
  enter_area_time: 4,
  rec_time: 5,
  work_time: 6,
  map_id: 7,
  area_id: 8,
  dept_id: 9,
  state_card: 10,
  state_object: 11,
  state_biz: 12,
  speed: 13,
  mark_id: 14,
  mark_direction: 15,
  mark_distance: 16,
  occupation_level_id: 17
}

const CARDAREAINFO = {
  area_id: 0,
  mark_id: 1,
  mark_direction: 2,
  mark_distance: 3,
  enter_area_time: 4
}

export { CARD, VCARD, CARDAREAINFO }
