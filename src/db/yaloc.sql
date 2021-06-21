-- MySQL dump 10.16  Distrib 10.1.7-MariaDB, for osx10.11 (x86_64)
--
-- Host: localhost    Database: yaloc
-- ------------------------------------------------------
-- Server version	10.1.7-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dat_access`
--

DROP TABLE IF EXISTS `dat_access`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_access` (
  `access_id` int(11) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`access_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限组，与菜单权限关联';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_access`
--

LOCK TABLES `dat_access` WRITE;
/*!40000 ALTER TABLE `dat_access` DISABLE KEYS */;
INSERT INTO `dat_access` VALUES (2,'333'),(4,'asdas'),(5,'ass');
/*!40000 ALTER TABLE `dat_access` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_alarm_type`
--

DROP TABLE IF EXISTS `dat_alarm_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_alarm_type` (
  `alarm_type_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  PRIMARY KEY (`alarm_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='报警信息类型';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_alarm_type`
--

LOCK TABLES `dat_alarm_type` WRITE;
/*!40000 ALTER TABLE `dat_alarm_type` DISABLE KEYS */;
INSERT INTO `dat_alarm_type` VALUES (1,'井下超员'),(2,'井下超时'),(3,'区域超员'),(4,'区域超时'),(5,'进入限制区域'),(6,'进入禁止区域'),(7,'分站工作异常'),(8,'车辆超速'),(9,'车辆闯红灯'),(10,'标识卡电量低');
/*!40000 ALTER TABLE `dat_alarm_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_antenna`
--

DROP TABLE IF EXISTS `dat_antenna`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_antenna` (
  `antenna_id` int(11) unsigned NOT NULL,
  `reader_id` int(11) unsigned NOT NULL,
  `reader_idx` int(11) unsigned NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `x` double NOT NULL,
  `y` double NOT NULL,
  `z` double NOT NULL,
  `angle` double unsigned NOT NULL,
  PRIMARY KEY (`antenna_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='天线';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_antenna`
--

LOCK TABLES `dat_antenna` WRITE;
/*!40000 ALTER TABLE `dat_antenna` DISABLE KEYS */;
INSERT INTO `dat_antenna` VALUES (1,1,1,'1-1',400,290,0,135),(2,1,2,'1-2',400,290,0,135),(3,1,3,'1-3',400,290,0,135),(4,2,1,'2-1',490,360,0,0),(5,2,2,'2-2',490,360,0,0),(6,2,3,'2-3',490,360,0,0),(7,3,1,'3-1',475,160,0,95),(8,3,2,'3-2',475,160,0,95),(9,3,3,'3-3',475,160,0,95),(10,4,1,'4-1',700,900,0,0),(11,4,2,'4-2',700,80,0,0),(12,4,3,'4-3',200,300,0,0);
/*!40000 ALTER TABLE `dat_antenna` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_area`
--

DROP TABLE IF EXISTS `dat_area`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_area` (
  `area_id` int(11) unsigned NOT NULL,
  `name` varchar(50) NOT NULL,
  `map_id` int(11) unsigned NOT NULL,
  `area_type_id` int(11) unsigned NOT NULL,
  `over_count_person` int(11) unsigned NOT NULL,
  `over_count_vehicle` int(11) unsigned NOT NULL,
  `over_time_person` int(11) unsigned NOT NULL,
  `over_time_vehicle` int(11) unsigned NOT NULL,
  `path` varchar(1024) NOT NULL,
  PRIMARY KEY (`area_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='区域';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_area`
--

LOCK TABLES `dat_area` WRITE;
/*!40000 ALTER TABLE `dat_area` DISABLE KEYS */;
INSERT INTO `dat_area` VALUES (1,'一号区域',3,2,50,0,120,0,''),(2,'二号区域',3,2,50,100,120,0,''),(3,'3号',2,2,35,35,35,35,'');
/*!40000 ALTER TABLE `dat_area` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_area_type`
--

DROP TABLE IF EXISTS `dat_area_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_area_type` (
  `area_type_id` int(11) unsigned NOT NULL,
  `name` varchar(50) NOT NULL,
  PRIMARY KEY (`area_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='区域类型';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_area_type`
--

LOCK TABLES `dat_area_type` WRITE;
/*!40000 ALTER TABLE `dat_area_type` DISABLE KEYS */;
INSERT INTO `dat_area_type` VALUES (1,'普通区域'),(2,'重点区域');
/*!40000 ALTER TABLE `dat_area_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_card`
--

DROP TABLE IF EXISTS `dat_card`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_card` (
  `card_id` bigint(13) unsigned zerofill NOT NULL,
  `card_type_id` tinyint(4) unsigned NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  `is_count` tinyint(3) unsigned NOT NULL,
  `ident` bigint(13) unsigned zerofill NOT NULL,
  PRIMARY KEY (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='标识卡';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_card`
--

LOCK TABLES `dat_card` WRITE;
/*!40000 ALTER TABLE `dat_card` DISABLE KEYS */;
INSERT INTO `dat_card` VALUES (0010000000001,1,1,0,0000000000001),(0010000000003,1,1,0,0000000000003),(0020000000002,2,1,0,0000000000002),(0020000000004,2,1,0,0000000000004),(0020012345678,2,1,1,0000000000005);
/*!40000 ALTER TABLE `dat_card` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_card_type`
--

DROP TABLE IF EXISTS `dat_card_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_card_type` (
  `card_type_id` int(11) NOT NULL,
  `name` varchar(50) COLLATE utf8_bin NOT NULL,
  `detail` varchar(50) COLLATE utf8_bin NOT NULL,
  `icon_normal` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `icon_error` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `icon_alarm` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`card_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='卡类型';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_card_type`
--

LOCK TABLES `dat_card_type` WRITE;
/*!40000 ALTER TABLE `dat_card_type` DISABLE KEYS */;
INSERT INTO `dat_card_type` VALUES (1,'staff','人员','icon-person','icon-assignment_ind','icon-person_outline'),(2,'vehicle','车辆','icon-directions_transit','icon-directions_transit','icon-directions_transit');
/*!40000 ALTER TABLE `dat_card_type` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `dat_card_state`
--

DROP TABLE IF EXISTS `dat_card_state`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_card_state` (
  `card_state_id` int(11) NOT NULL,
  `name` varchar(50) COLLATE utf8_bin NOT NULL,
  `label` varchar(50) COLLATE utf8_bin NOT NULL,
  `color` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`card_state_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='标识卡状态表，用来区分标识卡的状态，不同状态可以采用不同的颜色显示';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `dat_dept`
--

DROP TABLE IF EXISTS `dat_dept`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_dept` (
  `dept_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `spell` varchar(25) NOT NULL,
  `rank` int(10) NOT NULL,
  PRIMARY KEY (`dept_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='部门';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_dept`
--

LOCK TABLES `dat_dept` WRITE;
/*!40000 ALTER TABLE `dat_dept` DISABLE KEYS */;
INSERT INTO `dat_dept` VALUES (0,'总裁办','ZCB',0),(1,'运输部','YSB',0),(2,'部门二','BME',3);
/*!40000 ALTER TABLE `dat_dept` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `dat_device_state`
--

DROP TABLE IF EXISTS `dat_device_state`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_device_state` (
  `device_state_id` int(11) NOT NULL,
  `name` varchar(50) COLLATE utf8_bin NOT NULL,
  `label` varchar(50) COLLATE utf8_bin NOT NULL,
  `color` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`device_state_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='设备状态表，用来区分分站类型、道岔、红绿灯等的状态，不同状态可以采用不同的颜色显示';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `dat_device_type`
--

DROP TABLE IF EXISTS `dat_device_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_device_type` (
  `device_type_id` int(11) NOT NULL,
  `name` varchar(50) COLLATE utf8_bin NOT NULL,
  `detail` varchar(50) COLLATE utf8_bin NOT NULL,
  `icon_normal` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `icon_error` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `icon_alarm` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`device_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='设备类型表，用来区域分站类型、道岔、红绿灯等，不同设备采用不同的图标显示';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_device_type`
--

LOCK TABLES `dat_device_type` WRITE;
/*!40000 ALTER TABLE `dat_device_type` DISABLE KEYS */;
INSERT INTO `dat_device_type` VALUES (0,'reader','分站','icon-account_balance','icon-phonelink_erase','icon-phonelink_erase'),(1,'traffic','红绿灯','icon-traffic','icon-traffic','icon-traffic'),(2,'speaker','报站终端','icon-volume_up','icon-volume_up','icon-volume_up'),(3,'turnout','道岔','icon-shuffle','icon-shuffle','icon-shuffle');
/*!40000 ALTER TABLE `dat_device_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_group`
--

DROP TABLE IF EXISTS `dat_group`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_group` (
  `group_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `spell` varchar(25) NOT NULL,
  `detail` varchar(200) DEFAULT NULL,
  `rank` int(11) NOT NULL,
  PRIMARY KEY (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='组别';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_group`
--

LOCK TABLES `dat_group` WRITE;
/*!40000 ALTER TABLE `dat_group` DISABLE KEYS */;
INSERT INTO `dat_group` VALUES (1,'运输部','YSB','dept',8),(2,'工程部','GCB','OKsdfsdf',5);
/*!40000 ALTER TABLE `dat_group` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_level`
--

DROP TABLE IF EXISTS `dat_level`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_level` (
  `level_id` int(11) NOT NULL,
  `name` int(11) NOT NULL,
  `rank` int(11) NOT NULL,
  PRIMARY KEY (`level_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='级别';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_level`
--

LOCK TABLES `dat_level` WRITE;
/*!40000 ALTER TABLE `dat_level` DISABLE KEYS */;
/*!40000 ALTER TABLE `dat_level` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_map`
--

DROP TABLE IF EXISTS `dat_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_map` (
  `map_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `path` varchar(255) NOT NULL,
  `scale` float NOT NULL DEFAULT '1',
  `type` int(11) NOT NULL,
  `detail` varchar(128) DEFAULT NULL,
  `state` int(11) DEFAULT '1',
  `md5` char(16) DEFAULT NULL,
  PRIMARY KEY (`map_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='地图';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_map`
--

LOCK TABLES `dat_map` WRITE;
/*!40000 ALTER TABLE `dat_map` DISABLE KEYS */;
INSERT INTO `dat_map` VALUES (1,'一号地图','1.svg',1,1,'时空隧道',1,'8acd86cf0fd59c07'),(2,'测试地图','2.svg',1,0,'测试一下',1,'5b0977203cf52def'),(3,'神秘地图','3.svg',1,2,'what is this?',1,'204e15958eaebe3c'),(4,'DEMO','4.svg',1,1,'DEMO',1,'5b0977203cf52def'),(6,'国家宝藏','6.svg',1,1,'1',1,'9b4642f6ae15fcd9');
/*!40000 ALTER TABLE `dat_map` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_occupation`
--

DROP TABLE IF EXISTS `dat_occupation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_occupation` (
  `occupation_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `rank` int(11) DEFAULT NULL,
  PRIMARY KEY (`occupation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='职务';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_occupation`
--

LOCK TABLES `dat_occupation` WRITE;
/*!40000 ALTER TABLE `dat_occupation` DISABLE KEYS */;
INSERT INTO `dat_occupation` VALUES (1,'职务一',NULL),(2,'职务二',NULL),(7,'问问',8),(8,'hg',0),(9,'ggjhg',7),(11,'等待--',2);
/*!40000 ALTER TABLE `dat_occupation` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_op_type`
--

DROP TABLE IF EXISTS `dat_op_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_op_type` (
  `op_type_id` int(11) NOT NULL,
  `name` varchar(64) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`op_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='用户操作日志类型，增、删、改、呼叫';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_op_type`
--

LOCK TABLES `dat_op_type` WRITE;
/*!40000 ALTER TABLE `dat_op_type` DISABLE KEYS */;
INSERT INTO `dat_op_type` VALUES (0,'login'),(1,'logout'),(2,'update_meta'),(3,'call_card'),(4,'assd');
/*!40000 ALTER TABLE `dat_op_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_reader`
--

DROP TABLE IF EXISTS `dat_reader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_reader` (
  `reader_id` int(10) unsigned NOT NULL,
  `reader_type_id` int(11) NOT NULL,
  `map_id` int(11) NOT NULL,
  `area_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `x` double NOT NULL,
  `y` double NOT NULL,
  `z` double NOT NULL,
  `angle` double unsigned NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  `ip` varchar(20) DEFAULT NULL,
  `device_type_id` int(11) NOT NULL,
  PRIMARY KEY (`reader_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='分站';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_reader`
--

LOCK TABLES `dat_reader` WRITE;
/*!40000 ALTER TABLE `dat_reader` DISABLE KEYS */;
INSERT INTO `dat_reader` VALUES (1,3,1,1,'一号基站',400,290,0,0,1,'255.255.255.255',0),(2,3,1,2,'二号基站',100,0,0,0,0,'255.255.255.255',0),(3,3,1,1,'三号基站',475,160,0,0,0,'255.255.222.255',0),(4,3,1,1,'四号基站',600,0,0,0,0,'255.255.255.254',0);
/*!40000 ALTER TABLE `dat_reader` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_reader_type`
--

DROP TABLE IF EXISTS `dat_reader_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_reader_type` (
  `reader_type_id` int(11) NOT NULL,
  `name` varchar(20) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`reader_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='分站类型表，井上分站、井下分站、车场分站';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_reader_type`
--

LOCK TABLES `dat_reader_type` WRITE;
/*!40000 ALTER TABLE `dat_reader_type` DISABLE KEYS */;
/*!40000 ALTER TABLE `dat_reader_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_role`
--

DROP TABLE IF EXISTS `dat_role`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_role` (
  `role_id` int(11) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='角色组，与部门关联';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_role`
--

LOCK TABLES `dat_role` WRITE;
/*!40000 ALTER TABLE `dat_role` DISABLE KEYS */;
INSERT INTO `dat_role` VALUES (1,'系统管理员'),(2,'调度室'),(3,'运输部'),(4,'综采部');
/*!40000 ALTER TABLE `dat_role` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_setting`
--

DROP TABLE IF EXISTS `dat_setting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_setting` (
  `setting_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `type` varchar(64) NOT NULL,
  `value` varchar(64) NOT NULL,
  `description` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`setting_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='设置';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_setting`
--

LOCK TABLES `dat_setting` WRITE;
/*!40000 ALTER TABLE `dat_setting` DISABLE KEYS */;
INSERT INTO `dat_setting` VALUES (1,'over_count_person','int','200',NULL),(2,'over_count_vehicle','int','200',NULL),(3,'over_time_person','int','200',NULL),(4,'over_time_vehicle','int','2000',NULL);
/*!40000 ALTER TABLE `dat_setting` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_shift`
--

DROP TABLE IF EXISTS `dat_shift`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_shift` (
  `shift_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) DEFAULT NULL,
  `shift_type_id` int(11) DEFAULT NULL,
  `start_time` time DEFAULT NULL,
  `end_time` time DEFAULT NULL,
  `min_minutes` int(11) DEFAULT NULL,
  `offset` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`shift_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_shift`
--

LOCK TABLES `dat_shift` WRITE;
/*!40000 ALTER TABLE `dat_shift` DISABLE KEYS */;
INSERT INTO `dat_shift` VALUES (1,'早班',1,'06:30:00','14:30:00',5,3),(2,'中班',1,'14:30:00','22:30:00',5,0),(3,'晚班',1,'22:30:00','06:30:00',5,0),(4,'早班',2,'05:00:00','11:00:00',5,0),(5,'中班',3,'11:00:00','17:00:00',5,0),(6,'晚班',2,'17:00:00','23:00:00',5,0),(7,'夜班',2,'23:00:00','05:00:00',5,0);
/*!40000 ALTER TABLE `dat_shift` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_shift_type`
--

DROP TABLE IF EXISTS `dat_shift_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_shift_type` (
  `shift_type_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`shift_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=67 DEFAULT CHARSET=utf8 COMMENT='班制';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_shift_type`
--

LOCK TABLES `dat_shift_type` WRITE;
/*!40000 ALTER TABLE `dat_shift_type` DISABLE KEYS */;
INSERT INTO `dat_shift_type` VALUES (1,'三八制'),(2,'四六制'),(3,'不加班'),(5,'ehllo9'),(6,'hello---'),(12,'hello'),(13,'aaaaa'),(14,'sdfadsf'),(15,'how do you do ?'),(16,'asfasdf'),(23,'dfa'),(24,'sdfasdf'),(25,'undefined'),(26,'undefined'),(36,'hgjgh'),(66,'what your name');
/*!40000 ALTER TABLE `dat_shift_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_speaker`
--

DROP TABLE IF EXISTS `dat_speaker`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_speaker` (
  `speaker_id` int(10) unsigned NOT NULL,
  `map_id` int(11) NOT NULL,
  `area_id` int(11) NOT NULL,
  `reader_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `x` double NOT NULL,
  `y` double NOT NULL,
  `z` double NOT NULL,
  `angle` double unsigned NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  `ip` varchar(20) DEFAULT NULL,
  `port` varchar(10) NOT NULL,
  `device_type_id` int(11) NOT NULL,
  PRIMARY KEY (`speaker_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='报站终端，扩音器';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_speaker`
--

LOCK TABLES `dat_speaker` WRITE;
/*!40000 ALTER TABLE `dat_speaker` DISABLE KEYS */;
INSERT INTO `dat_speaker` VALUES (1,1,1,1,'不知道',300,300,1,0,1,'1','1',2),(2,2,1,1,'到宿舍',300,200,1,0,1,'1','1',2);
/*!40000 ALTER TABLE `dat_speaker` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_staff`
--

DROP TABLE IF EXISTS `dat_staff`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_staff` (
  `staff_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `spell` varchar(50) NOT NULL,
  `pic` varchar(50) NOT NULL,
  `com_id` int(11) NOT NULL,
  `dept_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  `occupation_id` int(11) NOT NULL,
  `card_id` bigint(13) unsigned zerofill NOT NULL,
  `sex` int(11) unsigned NOT NULL,
  `certification` int(11) NOT NULL,
  `state` int(11) unsigned NOT NULL,
  PRIMARY KEY (`staff_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='员工';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_staff`
--

LOCK TABLES `dat_staff` WRITE;
/*!40000 ALTER TABLE `dat_staff` DISABLE KEYS */;
INSERT INTO `dat_staff` VALUES (1,'张三','ZS','1.jpg',0,1,1,1,0010000000001,1,111,1),(2,'李四','LS','2.png',0,1,2,2,0020000000002,1,111,1),(3,'王五','WW','3.png',0,2,1,1,0010000000003,1,111,1),(4,'麻六','ML','4.jpg',0,2,2,2,0020000000004,1,111,1);
/*!40000 ALTER TABLE `dat_staff` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_staffer`
--

DROP TABLE IF EXISTS `dat_staffer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_staffer` (
  `staffer_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `spell` varchar(50) NOT NULL,
  `img` int(11) NOT NULL,
  `com_id` int(11) NOT NULL,
  `dept_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  `occupation_id` int(11) NOT NULL,
  `card_id` bigint(13) unsigned zerofill NOT NULL,
  `sex` int(11) unsigned NOT NULL,
  `certification` int(11) NOT NULL,
  `state` int(11) unsigned NOT NULL,
  PRIMARY KEY (`staffer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='员工';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_staffer`
--

LOCK TABLES `dat_staffer` WRITE;
/*!40000 ALTER TABLE `dat_staffer` DISABLE KEYS */;
INSERT INTO `dat_staffer` VALUES (1,'张三','',0,0,1,1,1,0010000000001,1,111,1),(2,'李四','',0,0,1,2,2,0020000000002,1,111,1),(3,'王五','',0,0,2,1,1,0010000000003,1,111,1),(4,'麻六','',0,0,2,2,2,0020000000004,1,111,1);
/*!40000 ALTER TABLE `dat_staffer` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_traffic`
--

DROP TABLE IF EXISTS `dat_traffic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_traffic` (
  `traffic_id` int(10) unsigned NOT NULL,
  `map_id` int(11) NOT NULL,
  `area_id` int(11) NOT NULL,
  `reader_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `x` double NOT NULL,
  `y` double NOT NULL,
  `z` double NOT NULL,
  `angle` double unsigned NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  `ip` varchar(20) DEFAULT NULL,
  `port` varchar(10) DEFAULT NULL,
  `device_type_id` int(11) NOT NULL,
  PRIMARY KEY (`traffic_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='红绿灯';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_traffic`
--

LOCK TABLES `dat_traffic` WRITE;
/*!40000 ALTER TABLE `dat_traffic` DISABLE KEYS */;
INSERT INTO `dat_traffic` VALUES (1,1,1,1,'ddd',100,100,0,0,0,'33','12',1),(2,1,1,1,'sss',0,0,0,0,1,'11','11',1);
/*!40000 ALTER TABLE `dat_traffic` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_turnout`
--

DROP TABLE IF EXISTS `dat_turnout`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_turnout` (
  `turnout_id` int(10) unsigned NOT NULL,
  `map_id` int(11) NOT NULL,
  `area_id` int(11) NOT NULL,
  `reader_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `x` double NOT NULL,
  `y` double NOT NULL,
  `z` double NOT NULL,
  `angle` double unsigned NOT NULL,
  `state` tinyint(3) unsigned NOT NULL,
  `ip` varchar(20) DEFAULT NULL,
  `port` varchar(20) DEFAULT NULL,
  `device_type_id` int(11) NOT NULL,
  PRIMARY KEY (`turnout_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='道岔';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_turnout`
--

LOCK TABLES `dat_turnout` WRITE;
/*!40000 ALTER TABLE `dat_turnout` DISABLE KEYS */;
INSERT INTO `dat_turnout` VALUES (1,1,1,1,'是是是',50,50,50,50,50,'50','50',3);
/*!40000 ALTER TABLE `dat_turnout` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_user`
--

DROP TABLE IF EXISTS `dat_user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_user` (
  `user_id` varchar(50) NOT NULL,
  `pwd` varchar(50) NOT NULL,
  `role_id` int(11) NOT NULL,
  `access_id` int(11) NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_user`
--

LOCK TABLES `dat_user` WRITE;
/*!40000 ALTER TABLE `dat_user` DISABLE KEYS */;
INSERT INTO `dat_user` VALUES ('COLLECTOR','666666',1,0),('guest','999',2,1),('HANK','88888888',1,0),('lihz','000',1,0);
/*!40000 ALTER TABLE `dat_user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_vehicle`
--

DROP TABLE IF EXISTS `dat_vehicle`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_vehicle` (
  `vehicle_id` int(11) NOT NULL,
  `number` varchar(50) DEFAULT NULL,
  `name` varchar(50) NOT NULL,
  `spell` varchar(50) NOT NULL,
  `pic` varchar(50) NOT NULL,
  `vehicle_type_id` tinyint(4) DEFAULT NULL,
  `card_id` bigint(13) unsigned zerofill NOT NULL,
  `dept_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`vehicle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='车辆表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_vehicle`
--

LOCK TABLES `dat_vehicle` WRITE;
/*!40000 ALTER TABLE `dat_vehicle` DISABLE KEYS */;
INSERT INTO `dat_vehicle` VALUES (1,'GC-GC1001','多功能铲运车','DGNCYC','1.JPG',5,0020012345678,1,1),(2,'GC-GC1002','二辆车','ELC','2.jpg',8,0020000000004,0,1),(3,'4567676','中国神车','ZGSC','3.jpg',15,0000000234234,1,1);
/*!40000 ALTER TABLE `dat_vehicle` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_vehicle_type`
--

DROP TABLE IF EXISTS `dat_vehicle_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_vehicle_type` (
  `vehicle_type_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `rank` int(11) DEFAULT NULL,
  `capacity` int(11) DEFAULT NULL,
  PRIMARY KEY (`vehicle_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8 COMMENT='车辆类型';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_vehicle_type`
--

LOCK TABLES `dat_vehicle_type` WRITE;
/*!40000 ALTER TABLE `dat_vehicle_type` DISABLE KEYS */;
INSERT INTO `dat_vehicle_type` VALUES (1,'多功能铲运车(10t)',NULL,10),(2,'防爆柴油铲运车(12t)',NULL,12),(3,'防爆指挥车(12人)',NULL,12),(4,'人员运输车(24人)',2,24),(5,'材料车(5t)',NULL,5),(6,'平推型自卸车(5t)',NULL,5),(7,'后翻型自卸车(5t)',NULL,5),(8,'后翻型自卸车(1.9t)',4,2),(9,'长材运输车(8t)',NULL,8),(10,'防爆装载机(3t)',NULL,3),(11,'吊臂运输车(5t)',NULL,5),(12,'加油运输车(5t)',NULL,5),(13,'支架搬运车(50t)',NULL,50),(14,'重型铲运车(55t)',NULL,55),(15,'蓄电池电机车(12t)',NULL,12);
/*!40000 ALTER TABLE `dat_vehicle_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dat_work_type`
--

DROP TABLE IF EXISTS `dat_work_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dat_work_type` (
  `work_type_id` int(11) NOT NULL,
  `name` varchar(50) NOT NULL,
  `rank` int(11) NOT NULL,
  PRIMARY KEY (`work_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='工种';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dat_work_type`
--

LOCK TABLES `dat_work_type` WRITE;
/*!40000 ALTER TABLE `dat_work_type` DISABLE KEYS */;
/*!40000 ALTER TABLE `dat_work_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `his_alarminfo`
--

DROP TABLE IF EXISTS `his_alarminfo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_alarminfo` (
  `alarm_type_id` int(11) NOT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime NOT NULL,
  `object_type` varchar(32) NOT NULL,
  `object_id` bigint(20) NOT NULL,
  `detail` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`alarm_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='历史报警记录';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `his_alarminfo`
--

LOCK TABLES `his_alarminfo` WRITE;
/*!40000 ALTER TABLE `his_alarminfo` DISABLE KEYS */;
/*!40000 ALTER TABLE `his_alarminfo` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `his_area_location`
--

DROP TABLE IF EXISTS `his_area_location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_area_location` (
  `card_id` bigint(20) NOT NULL,
  `area_id` int(11) NOT NULL,
  `map_id` int(11) NOT NULL,
  `enter_time` datetime NOT NULL,
  `leave_time` datetime NOT NULL,
  PRIMARY KEY (`card_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='区域定位数据';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `his_area_location`
--

LOCK TABLES `his_area_location` WRITE;
/*!40000 ALTER TABLE `his_area_location` DISABLE KEYS */;
/*!40000 ALTER TABLE `his_area_location` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `his_locatedata`
--

DROP TABLE IF EXISTS `his_locatedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_locatedata` (
  `card_id` bigint(20) DEFAULT NULL,
  `reader_id` int(11) DEFAULT NULL,
  `antenna_id` int(11) DEFAULT NULL,
  `reader_stamp` int(11) DEFAULT NULL,
  `card_stamp` int(11) DEFAULT NULL,
  `locatetime` bigint(20) DEFAULT NULL,
  `distance` double DEFAULT NULL,
  `stamp_type` int(11) DEFAULT NULL,
  `power_state` int(11) DEFAULT NULL,
  `accelerate` int(11) DEFAULT NULL,
  `rec_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `his_locatedata`
--

LOCK TABLES `his_locatedata` WRITE;
/*!40000 ALTER TABLE `his_locatedata` DISABLE KEYS */;
/*!40000 ALTER TABLE `his_locatedata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `his_location`
--

DROP TABLE IF EXISTS `his_location`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_location` (
  `card_id` bigint(20) NOT NULL,
  `start_time` datetime NOT NULL,
  `x` int(11) NOT NULL,
  `y` int(11) NOT NULL,
  `z` int(11) NOT NULL,
  `map_id` int(11) NOT NULL,
  `area_id` int(11) NOT NULL,
  `state` int(11) NOT NULL,
  PRIMARY KEY (`card_id`,`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='历史定位数据';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `his_location`
--

LOCK TABLES `his_location` WRITE;
/*!40000 ALTER TABLE `his_location` DISABLE KEYS */;
INSERT INTO `his_location` VALUES (10000000001,'2016-02-26 15:56:59',0,0,0,1,1,1);
/*!40000 ALTER TABLE `his_location` ENABLE KEYS */;
UNLOCK TABLES;


DROP TABLE IF EXISTS `his_call`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_call` (
  `call_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(32) COLLATE utf8_bin NOT NULL,
  `call_time_out` int(11) NOT NULL DEFAULT 5,
  `call_type_id` int(11) NOT NULL DEFAULT 0,
  `stations` varchar(256) COLLATE utf8_bin DEFAULT NULL,
  `cards` varchar(256) COLLATE utf8_bin DEFAULT NULL,
  `call_start_time` datetime DEFAULT NULL,
  `call_stop_time` datetime DEFAULT NULL,
  `call_stop_by` char(16) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`call_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `his_op_log`
--

DROP TABLE IF EXISTS `his_op_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `his_op_log` (
  `op_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(50) COLLATE utf8_bin NOT NULL,
  `time` datetime DEFAULT NULL,
  `ip` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `op_type_id` int(11) DEFAULT NULL,
  `detail` varchar(256) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`op_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2482 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `his_op_log`
--

LOCK TABLES `his_op_log` WRITE;
/*!40000 ALTER TABLE `his_op_log` DISABLE KEYS */;
/*!40000 ALTER TABLE `his_op_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `r_access_menu`
--

DROP TABLE IF EXISTS `r_access_menu`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `r_access_menu` (
  `access_id` int(11) DEFAULT NULL,
  `menu_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限组与菜单关联表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `r_access_menu`
--

LOCK TABLES `r_access_menu` WRITE;
/*!40000 ALTER TABLE `r_access_menu` DISABLE KEYS */;
/*!40000 ALTER TABLE `r_access_menu` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `r_role_dept`
--

DROP TABLE IF EXISTS `r_role_dept`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `r_role_dept` (
  `role_id` int(11) DEFAULT NULL,
  `dept_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='角色组与部门关联表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `r_role_dept`
--

LOCK TABLES `r_role_dept` WRITE;
/*!40000 ALTER TABLE `r_role_dept` DISABLE KEYS */;
INSERT INTO `r_role_dept` VALUES (1,1),(2,1);
/*!40000 ALTER TABLE `r_role_dept` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `rpt_attendance`
--

DROP TABLE IF EXISTS `rpt_attendance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `rpt_attendance` (
  `card_id` int(11) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `att_date` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='考勤表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rpt_attendance`
--

LOCK TABLES `rpt_attendance` WRITE;
/*!40000 ALTER TABLE `rpt_attendance` DISABLE KEYS */;
/*!40000 ALTER TABLE `rpt_attendance` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-03-23 22:48:32
