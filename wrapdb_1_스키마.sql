-- --------------------------------------------------------
-- 호스트:                          127.0.0.1
-- 서버 버전:                        10.4.6-MariaDB - mariadb.org binary distribution
-- 서버 OS:                        Win64
-- HeidiSQL 버전:                  9.5.0.5196
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- wrapdb_1 데이터베이스 구조 내보내기
CREATE DATABASE IF NOT EXISTS `wrapdb_1` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `wrapdb_1`;

-- 테이블 wrapdb_1.code_element 구조 내보내기
CREATE TABLE IF NOT EXISTS `code_element` (
  `element_cd` varchar(5) NOT NULL,
  `group_cd` varchar(5) NOT NULL,
  `element_nm` varchar(100) NOT NULL,
  PRIMARY KEY (`element_cd`,`group_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.code_group 구조 내보내기
CREATE TABLE IF NOT EXISTS `code_group` (
  `group_cd` varchar(5) NOT NULL,
  `group_nm` varchar(100) NOT NULL,
  PRIMARY KEY (`group_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.item 구조 내보내기
CREATE TABLE IF NOT EXISTS `item` (
  `cd` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `nm` varchar(100) NOT NULL,
  `isin` varchar(20) DEFAULT NULL,
  `value_type` varchar(5) DEFAULT NULL,
  `value_unit` varchar(5) DEFAULT NULL,
  `period_unit` varchar(5) DEFAULT NULL,
  `currency` varchar(5) DEFAULT NULL,
  `source` varchar(5) DEFAULT NULL,
  `ticker` varchar(50) DEFAULT NULL,
  `field` varchar(50) DEFAULT 'PX_LAST',
  `group` varchar(5) DEFAULT NULL,
  `use_yn` int(1) unsigned DEFAULT 2,
  `delay` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`cd`),
  KEY `INDEX1` (`nm`)
) ENGINE=InnoDB AUTO_INCREMENT=388 DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.ivalues 구조 내보내기
CREATE TABLE IF NOT EXISTS `ivalues` (
  `date` varchar(10) NOT NULL,
  `item_cd` int(10) NOT NULL,
  `value` float NOT NULL,
  `create_tm` datetime DEFAULT NULL,
  `update_tm` datetime DEFAULT NULL,
  PRIMARY KEY (`date`,`item_cd`),
  KEY `INDEX1` (`item_cd`),
  KEY `INDEX2` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.result 구조 내보내기
CREATE TABLE IF NOT EXISTS `result` (
  `start_dt` varchar(10) NOT NULL,
  `end_dt` varchar(10) NOT NULL,
  `curr_dt` varchar(10) NOT NULL,
  `target_cd` int(10) unsigned NOT NULL,
  `factors_num` int(2) unsigned NOT NULL,
  `multi_factors_nm` varchar(500) NOT NULL,
  `window_size` int(2) unsigned NOT NULL,
  `signal_cd` int(1) unsigned NOT NULL,
  `score` float NOT NULL,
  `term_type` int(1) unsigned NOT NULL,
  `model_profit` float NOT NULL,
  `bm_profit` float NOT NULL,
  `create_tm` datetime DEFAULT NULL,
  `update_tm` datetime DEFAULT NULL,
  PRIMARY KEY (`start_dt`,`end_dt`,`curr_dt`,`target_cd`,`multi_factors_nm`,`window_size`,`term_type`),
  KEY `INDEX1` (`start_dt`,`end_dt`,`target_cd`,`multi_factors_nm`),
  KEY `INDEX2` (`start_dt`,`end_dt`,`target_cd`,`term_type`,`model_profit`),
  KEY `INDEX3` (`start_dt`,`end_dt`,`target_cd`,`term_type`),
  KEY `INDEX4` (`target_cd`,`start_dt`,`end_dt`,`multi_factors_nm`,`factors_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.result_factor 구조 내보내기
CREATE TABLE IF NOT EXISTS `result_factor` (
  `start_dt` varchar(10) NOT NULL,
  `end_dt` varchar(10) NOT NULL,
  `curr_dt` varchar(10) NOT NULL,
  `target_cd` int(10) unsigned NOT NULL,
  `factor_cd` int(10) unsigned NOT NULL,
  `window_size` int(2) unsigned NOT NULL,
  `signal_cd` int(1) unsigned NOT NULL,
  `score` float NOT NULL,
  `term_type` int(1) unsigned NOT NULL,
  `factor_profit` float NOT NULL,
  `index_profit` float NOT NULL,
  `create_tm` datetime DEFAULT NULL,
  `update_tm` datetime DEFAULT NULL,
  PRIMARY KEY (`start_dt`,`end_dt`,`curr_dt`,`target_cd`,`factor_cd`,`window_size`,`term_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.result_last 구조 내보내기
CREATE TABLE IF NOT EXISTS `result_last` (
  `start_dt` varchar(10) NOT NULL,
  `end_dt` varchar(10) NOT NULL,
  `curr_dt` varchar(10) NOT NULL,
  `target_cd` int(10) unsigned NOT NULL,
  `factors_num` int(2) unsigned NOT NULL,
  `multi_factors_nm` varchar(500) NOT NULL,
  `factor_cd0` int(10) unsigned NOT NULL,
  `factor_cd1` int(10) unsigned DEFAULT NULL,
  `factor_cd2` int(10) unsigned DEFAULT NULL,
  `factor_cd3` int(10) unsigned DEFAULT NULL,
  `factor_cd4` int(10) unsigned DEFAULT NULL,
  `factor_cd5` int(10) unsigned DEFAULT NULL,
  `factor_cd6` int(10) unsigned DEFAULT NULL,
  `factor_cd7` int(10) unsigned DEFAULT NULL,
  `factor_cd8` int(10) unsigned DEFAULT NULL,
  `factor_cd9` int(10) unsigned DEFAULT NULL,
  `window_size` int(2) unsigned NOT NULL,
  `signal_cd` int(1) unsigned NOT NULL,
  `score` float NOT NULL,
  `term_type` int(1) unsigned NOT NULL,
  `model_profit` float NOT NULL,
  `bm_profit` float NOT NULL,
  `create_tm` datetime DEFAULT NULL,
  `update_tm` datetime DEFAULT NULL,
  PRIMARY KEY (`start_dt`,`end_dt`,`curr_dt`,`target_cd`,`multi_factors_nm`,`window_size`,`term_type`),
  KEY `INDEX1` (`start_dt`,`end_dt`,`target_cd`,`model_profit`,`term_type`),
  KEY `INDEX2` (`start_dt`,`end_dt`,`target_cd`,`window_size`),
  KEY `INDEX3` (`start_dt`,`end_dt`,`target_cd`,`term_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.target_factor_corr 구조 내보내기
CREATE TABLE IF NOT EXISTS `target_factor_corr` (
  `target_nm` varchar(128) NOT NULL,
  `factor_nm` varchar(128) NOT NULL,
  `end_dt` varchar(10) NOT NULL,
  `lag` int(8) NOT NULL,
  `window_size` int(8) NOT NULL,
  `norm_yn` int(8) NOT NULL,
  `value` float NOT NULL,
  `hit_ratio` float NOT NULL,
  `create_tm` datetime NOT NULL,
  `update_tm` datetime NOT NULL,
  PRIMARY KEY (`target_nm`,`factor_nm`,`end_dt`,`lag`,`window_size`,`norm_yn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 wrapdb_1.target_factor_corr_law_data 구조 내보내기
CREATE TABLE IF NOT EXISTS `target_factor_corr_law_data` (
  `target_nm` varchar(128) NOT NULL,
  `factor_nm` varchar(128) NOT NULL,
  `end_dt` varchar(10) NOT NULL,
  `lag` int(8) NOT NULL,
  `window_size` int(8) NOT NULL,
  `hist_dt` varchar(10) NOT NULL,
  `norm_yn` int(8) NOT NULL,
  `target_val` float NOT NULL,
  `factor_val` float NOT NULL,
  `hit_yn` int(8) NOT NULL,
  `create_tm` datetime NOT NULL,
  `update_tm` datetime NOT NULL,
  PRIMARY KEY (`target_nm`,`factor_nm`,`end_dt`,`lag`,`window_size`,`hist_dt`,`norm_yn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
