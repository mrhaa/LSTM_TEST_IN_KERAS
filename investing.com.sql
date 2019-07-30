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


-- investing.com 데이터베이스 구조 내보내기
CREATE DATABASE IF NOT EXISTS `investing.com` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `investing.com`;

-- 테이블 investing.com.economic_events 구조 내보내기
CREATE TABLE IF NOT EXISTS `economic_events` (
  `cd` int(4) unsigned NOT NULL,
  `nm_us` varchar(100) DEFAULT NULL,
  `nm_kr` varchar(100) DEFAULT NULL,
  `link` varchar(200) DEFAULT NULL,
  `ctry` varchar(200) DEFAULT NULL,
  `period` char(1) DEFAULT NULL,
  `imp_us` int(1) DEFAULT NULL,
  `imp_kr` int(1) DEFAULT NULL,
  PRIMARY KEY (`cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 investing.com.economic_events_schedule 구조 내보내기
CREATE TABLE IF NOT EXISTS `economic_events_schedule` (
  `event_cd` int(4) unsigned NOT NULL,
  `release_date` char(10) NOT NULL,
  `release_time` char(5) NOT NULL,
  `statistics_time` int(2) DEFAULT NULL,
  `bold_value` float NOT NULL,
  `fore_value` float DEFAULT NULL,
  PRIMARY KEY (`event_cd`,`release_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 investing.com.index_master 구조 내보내기
CREATE TABLE IF NOT EXISTS `index_master` (
  `cd` varchar(8) NOT NULL,
  `nm_us` varchar(64) NOT NULL,
  `nm_kr` varchar(128) DEFAULT NULL,
  `curr_id` int(8) NOT NULL,
  `smlID` int(16) NOT NULL,
  `type` char(1) DEFAULT NULL,
  PRIMARY KEY (`cd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
-- 테이블 investing.com.index_price 구조 내보내기
CREATE TABLE IF NOT EXISTS `index_price` (
  `idx_cd` varchar(8) NOT NULL,
  `date` varchar(10) NOT NULL,
  `close` float NOT NULL,
  `open` float NOT NULL,
  `high` float NOT NULL,
  `low` float NOT NULL,
  `vol` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 내보낼 데이터가 선택되어 있지 않습니다.
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
