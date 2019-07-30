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

-- 테이블 데이터 investing.com.economic_events:~24 rows (대략적) 내보내기
/*!40000 ALTER TABLE `economic_events` DISABLE KEYS */;
INSERT INTO `economic_events` (`cd`, `nm_us`, `nm_kr`, `link`, `ctry`, `period`, `imp_us`, `imp_kr`) VALUES
	(1, 'U.S. ADP Nonfarm Employment Change ', 'ADP 비농업부문 고용 변화', 'https://www.investing.com/economic-calendar/adp-nonfarm-employment-change-1', 'US', 'M', NULL, 3),
	(25, 'U.S. Building Permits ', '건축승인건수', 'https://www.investing.com/economic-calendar/building-permits-25', 'US', 'M', NULL, 3),
	(48, 'U.S. CB Consumer Confidence ', 'CB 소비자신뢰지수', 'https://www.investing.com/economic-calendar/cb-consumer-confidence-48', 'US', 'M', NULL, 3),
	(56, 'U.S. Core Consumer Price Index (CPI) MoM', '근원 소비자물가지수', 'https://www.investing.com/economic-calendar/core-cpi-56', 'US', 'M', NULL, 3),
	(59, 'U.S. Core Durable Goods Orders MoM', '근원 내구재수주', 'https://www.investing.com/economic-calendar/core-durable-goods-orders-59', 'US', 'M', NULL, 3),
	(63, 'U.S. Core Retail Sales MoM	', '근원 소매판매', 'https://www.investing.com/economic-calendar/core-retail-sales-63', 'US', 'M', NULL, 3),
	(75, 'U.S. Crude Oil Inventories ', '원유재고', 'https://www.investing.com/economic-calendar/eia-crude-oil-inventories-75', 'US', 'W', NULL, 3),
	(99, 'U.S. Existing Home Sales ', '기존주택판매', 'https://www.investing.com/economic-calendar/existing-home-sales-99', 'US', 'M', NULL, 3),
	(168, 'Fed Interest Rate Decision ', '금리결정 ', 'https://www.investing.com/economic-calendar/interest-rate-decision-168', 'US', NULL, NULL, 3),
	(173, 'U.S. ISM Manufacturing Purchasing Managers Index (PMI) ', 'ISM 제조업구매자지수', 'https://www.investing.com/economic-calendar/ism-manufacturing-pmi-173', 'US', 'M', NULL, 3),
	(176, 'U.S. ISM Non-Manufacturing Purchasing Managers Index (PMI) ', 'ISM 비제조업구매자지수', 'https://www.investing.com/economic-calendar/ism-non-manufacturing-pmi-176', 'US', 'M', NULL, 3),
	(222, 'U.S. New Home Sales ', '신규 주택판매', 'https://www.investing.com/economic-calendar/new-home-sales-222', 'US', 'M', NULL, 3),
	(227, 'U.S. Nonfarm Payrolls ', '비농업고용지수', 'https://www.investing.com/economic-calendar/nonfarm-payrolls-227', 'US', 'M', NULL, 3),
	(232, 'U.S. Pending Home Sales MoM ', '잠정주택매매', 'https://www.investing.com/economic-calendar/pending-home-sales-232', 'US', 'M', NULL, 3),
	(236, 'U.S. Philadelphia Fed Manufacturing Index ', '필라델피아 연준 제조업활동지수', 'https://www.investing.com/economic-calendar/philadelphia-fed-manufacturing-index-236', 'US', 'M', NULL, 3),
	(238, 'U.S. Producer Price Index (PPI) MoM ', '생산자물가지수', 'https://www.investing.com/economic-calendar/ppi-238', 'US', 'M', NULL, 3),
	(256, 'U.S. Retail Sales MoM ', '소매판매', 'https://www.investing.com/economic-calendar/retail-sales-256', 'US', 'M', NULL, 3),
	(300, 'U.S. Unemployment Rate ', '실업률', 'https://www.investing.com/economic-calendar/unemployment-rate-300', 'US', 'M', NULL, 3),
	(375, 'U.S. Gross Domestic Product (GDP) QoQ', 'GDP', 'https://www.investing.com/economic-calendar/gdp-375', 'US', 'Q', NULL, 3),
	(461, 'China Gross Domestic Product (GDP) YoY', '중국 GDP', 'https://www.investing.com/economic-calendar/chinese-gdp-461', 'CN', 'Q', NULL, 3),
	(462, 'China Industrial Production YoY ', '중국 산업생산', 'https://www.investing.com/economic-calendar/chinese-industrial-production-462', 'CN', 'M', NULL, 3),
	(594, 'China Manufacturing Purchasing Managers Index (PMI) ', '중국 제조업 구매관리자지수', 'https://www.investing.com/economic-calendar/chinese-manufacturing-pmi-594', 'CN', 'M', NULL, 3),
	(753, 'China Caixin Manufacturing Purchasing Managers Index (PMI) ', 'Caixin 중국 제조업 구매관리자지수', 'https://www.investing.com/economic-calendar/chinese-caixin-manufacturing-pmi-753', 'CN', 'M', NULL, 3),
	(1057, 'U.S. JOLTs Job Openings ', '노동부 채용 및 노동 회전률 조사', 'https://www.investing.com/economic-calendar/jolts-job-openings-1057', 'US', 'M', NULL, 3);
/*!40000 ALTER TABLE `economic_events` ENABLE KEYS */;

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

-- 테이블 데이터 investing.com.index_master:~0 rows (대략적) 내보내기
/*!40000 ALTER TABLE `index_master` DISABLE KEYS */;
INSERT INTO `index_master` (`cd`, `nm_us`, `nm_kr`, `curr_id`, `smlID`, `type`) VALUES
	('CL', 'Crude Oil WTI Futures', NULL, 8849, 300060, 'F'),
	('DJI', 'Dow Jones Industrial Average', NULL, 169, 2030170, 'I'),
	('GC', 'Gold Futures', NULL, 8830, 300004, 'F'),
	('HG', 'Copper Futures', NULL, 8831, 300012, 'F'),
	('NDX', 'Nasdaq 100', NULL, 20, 2030165, 'I'),
	('NG', 'Natural Gas Futures', NULL, 8862, 300092, 'F'),
	('SI', 'Silver Futures', NULL, 8836, 300044, 'F'),
	('SPX', 'S&P 500', NULL, 166, 2030167, 'I'),
	('STOXX50E', 'Euro Stoxx 50', NULL, 175, 2030175, 'I');
/*!40000 ALTER TABLE `index_master` ENABLE KEYS */;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
