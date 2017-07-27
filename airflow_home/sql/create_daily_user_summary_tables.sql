--Create stage table for daily pair activity summary
DROP TABLE IF EXISTS stage_daily_user_pair_duration CASCADE;
CREATE TABLE stage_daily_user_pair_duration  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN daily_seconds = 0 AND kl15_app_ver_count = 1 THEN 1 ELSE 0 END AS accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
  NVL(SUM(duration),0) AS daily_seconds,
  count(utc_start_time_epoch) AS daily_events,
  count(DISTINCT kl15_app_ver) AS kl15_app_ver_count
FROM pair_mode_events
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);

--Create stage table for daily background dc activity
DROP TABLE IF EXISTS stage_daily_user_background_data CASCADE;
CREATE TABLE stage_daily_user_background_data  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN daily_events = 0 AND kl15_app_ver_count = 1 THEN 1 ELSE 0 END AS accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
  count(utc_start_time_epoch) AS daily_events,
  count(DISTINCT kl15_app_ver) AS kl15_app_ver_count
FROM (
SELECT *,
   kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver
FROM rawsync.t10_syslog
WHERE log_id='M' AND log_code='BDC'
)
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);

--Create stage table for stuck accel activity
DROP TABLE IF EXISTS stage_daily_user_stuck_accel CASCADE;
CREATE TABLE stage_daily_user_stuck_accel  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN daily_events = 0 AND kl15_app_ver_count = 1 THEN 1 ELSE 0 END AS accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
  count(utc_start_time_epoch) AS daily_events,
  count(DISTINCT kl15_app_ver) AS kl15_app_ver_count
FROM (
SELECT *,
   kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver
FROM rawsync.t10_syslog
WHERE log_id='M' AND log_code='WLD'
)
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);

DROP TABLE IF EXISTS stage_daily_user_other_events CASCADE;
CREATE TABLE stage_daily_user_other_events  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT l.parse_user_id AS parse_user_id,
       l.parse_user_email AS parse_user_email,
       l.device_serial_number AS device_serial_number,
       l.utc_start_date AS utc_start_date,
       l.daily_events AS background_data_events,
       r.daily_events AS stuck_accel_events,
       CASE WHEN l.accept=1 AND r.accept=1 THEN 1 ELSE 0 END as accept
FROM stage_daily_user_background_data l
FULL JOIN stage_daily_user_stuck_accel r ON
        l.parse_user_id = r.parse_user_id AND l.device_serial_number = r.device_serial_number AND l.utc_start_date = r.utc_start_date;

--Create stage table for daily user_connection overhead
-- AAA TODO: replace inline constants with constants table index'ed by fw version
DROP TABLE IF EXISTS stage_daily_user_connection_overhead CASCADE;
-- Create a summary of connection related overheads per user per day
CREATE TABLE stage_daily_user_connection_overhead  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN kl15_app_ver_count = 1 THEN 1 ELSE 0 END as accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
   count(DISTINCT kl15_app_ver) AS kl15_app_ver_count,
   MAX(kl15_app_ver) AS kl15_app_ver,
   MAX(kl15_app_ver) AS kl15_app_ver_max,
   MIN(kl15_app_ver) AS kl15_app_ver_min,
   COUNT(utc_start_time_epoch) AS daily_events,
   SUM(bytes_overhead) AS bytes_overhead
FROM (
   SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
   log_data0 as type,
   kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver,
   utc_start_time_epoch,
   DECODE(log_data0,
        0,54,   -- disconnnect overhead 2*14 bytes + 26 bytes storage counters   (note actually varies by fw ver)
        1,54,   -- sync overhead 2*14 bytes + 26 bytes storage counters   (note actually varies by fw ver)
        2,68,   -- pair overhead 3*14 bytes + 26 bytes storage counters   (note actually varies by fw ver)
        5,28,   -- connect pending overhead 2*14 bytes
        28 -- default, though currently known cases covered
      ) AS bytes_overhead

   FROM rawsync.t10_syslog
   WHERE log_id='M'
     AND log_code='CON'
)
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);




--Create stage table for t31_finger summary
DROP TABLE IF EXISTS stage_daily_user_on_finger CASCADE;
CREATE TABLE stage_daily_user_on_finger  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN on_finger_ratio > 0.9 and on_finger_seconds > 0.81*24.0*60.0*60.0 AND kl15_app_ver_count = 1 THEN 1 ELSE 0 END AS accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
  SUM(duration) AS daily_seconds,
  count(DISTINCT kl15_app_ver) AS kl15_app_ver_count,
  count(utc_start_time_epoch) AS daily_events,
  SUM(on_finger_duration) AS on_finger_seconds,
  SUM(off_finger_duration) AS off_finger_seconds,
  CASE WHEN SUM(duration) > 0 THEN SUM(on_finger_duration)::float/SUM(duration)::float ELSE 0 END AS on_finger_ratio,
  CASE WHEN SUM(duration) > 0 THEN SUM(off_finger_duration)::float/SUM(duration)::float ELSE 0 END AS off_finger_ratio
FROM (
  SELECT *,
    kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver,
    (CASE WHEN finger_state=1 THEN duration else 0 END) AS on_finger_duration,
    (CASE WHEN finger_state=0 THEN duration else 0 END) AS off_finger_duration
  FROM
  (
    SELECT *,
      --NVL(lead(utc_start_time_epoch) OVER (partition by parse_user_id, device_serial_number ORDER BY tlv_synced_at, tlv_sequence_id)-utc_start_time_epoch,0) AS duration
      NVL(lead(utc_start_time_epoch) OVER (partition by parse_user_id, device_serial_number ORDER BY utc_start_time_epoch)-utc_start_time_epoch,0) AS duration
    FROM rawsync.t31_finger
  )
)
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);

--Create stage table for t28_storage_counters
DROP TABLE IF EXISTS stage_daily_user_storage_counters CASCADE;
CREATE TABLE stage_daily_user_storage_counters  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT *,
       CASE WHEN kl15_app_ver_count = 1 THEN 1 ELSE 0 END as accept
FROM (
SELECT parse_user_id, parse_user_email, device_serial_number, utc_start_date,
  --count(utc_start_time_epoch) AS daily_events,
  count(DISTINCT kl15_app_ver) AS kl15_app_ver_count,
  MAX(kl15_app_ver) AS kl15_app_ver_max,
  MIN(kl15_app_ver) AS kl15_app_ver_min,
  SUM(bytes_written_delta) as bytes_written_per_day FROM (
    SELECT DISTINCT bytes_written - lag(bytes_written) OVER (PARTITION BY parse_user_id, device_serial_number ORDER BY tlv_synced_at, tlv_sequence_id) as bytes_written_delta,
        DATE_TRUNC('DAY',utc_start_at) AS utc_start_date,
        kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver,
        *
    FROM rawsync.t28_storage_counters
)
WHERE bytes_written_delta > 0
GROUP BY parse_user_id, parse_user_email, device_serial_number, utc_start_date);



--Join vaious daily reports into a daily summary
DROP TABLE IF EXISTS stage_daily_user_sfp CASCADE;
CREATE TABLE stage_daily_user_sfp  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT f.parse_user_id AS parse_user_id,
       f.parse_user_email AS parse_user_email,
       f.device_serial_number AS device_serial_number,
       f.utc_start_date AS utc_start_date,
       f.on_finger_seconds AS on_finger_seconds,
       f.off_finger_seconds AS off_finger_seconds,
       f.on_finger_ratio AS on_finger_ratio,
       f.off_finger_ratio AS off_finger_ratio,
       f.kl15_app_ver_count AS kl15_app_ver_count,
       NVL(pd.daily_seconds,0) AS pair_duration,
       CASE WHEN f.accept=1 AND NVL(pd.accept,1)=1 THEN 1 ELSE 0 END as accept
FROM stage_daily_user_on_finger f
LEFT JOIN  stage_daily_user_pair_duration pd ON
        f.parse_user_id = pd.parse_user_id AND f.device_serial_number = pd.device_serial_number AND f.utc_start_date = pd.utc_start_date;

DROP TABLE IF EXISTS stage_daily_user_sfpsc CASCADE;
CREATE TABLE stage_daily_user_sfpsc  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT l.parse_user_id AS parse_user_id,
       l.parse_user_email AS parse_user_email,
       l.device_serial_number AS device_serial_number,
       l.utc_start_date AS utc_start_date,
       l.on_finger_seconds AS on_finger_seconds,
       l.off_finger_seconds AS off_finger_seconds,
       l.on_finger_ratio AS on_finger_ratio,
       l.off_finger_ratio AS off_finger_ratio,
       l.kl15_app_ver_count AS kl15_app_ver_count_f,
       r.kl15_app_ver_count AS kl15_app_ver_count_sc,
       l.pair_duration AS pair_duration,
       r.kl15_app_ver_max AS kl15_app_ver,
       r.bytes_written_per_day AS bytes_written_per_day,
       CASE WHEN l.accept=1 AND r.accept=1 THEN 1 ELSE 0 END as accept
FROM stage_daily_user_sfp l
INNER JOIN stage_daily_user_storage_counters r ON
        l.parse_user_id = r.parse_user_id AND l.device_serial_number = r.device_serial_number AND l.utc_start_date = r.utc_start_date;

DROP TABLE IF EXISTS stage_daily_user_sfpscoh CASCADE;
CREATE TABLE stage_daily_user_sfpscoh  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT l.parse_user_id AS parse_user_id,
       l.parse_user_email AS parse_user_email,
       l.device_serial_number AS device_serial_number,
       l.utc_start_date AS utc_start_date,
       l.on_finger_seconds AS on_finger_seconds,
       l.off_finger_seconds AS off_finger_seconds,
       l.on_finger_ratio AS on_finger_ratio,
       l.off_finger_ratio AS off_finger_ratio,
       l.kl15_app_ver_count_f AS kl15_app_ver_count_f,
       l.kl15_app_ver_count_sc AS kl15_app_ver_count_sc,
       r.kl15_app_ver_count AS kl15_app_ver_count_oh,
       l.kl15_app_ver AS kl15_app_ver,
       l.pair_duration AS pair_duration,
       l.bytes_written_per_day AS bytes_written_per_day,
       NVL(r.bytes_overhead,0) AS bytes_overhead,
       l.bytes_written_per_day - NVL(r.bytes_overhead,0) AS net_bytes_per_day,
       CASE WHEN l.accept=1 AND r.accept=1 THEN 1 ELSE 0 END as accept
FROM stage_daily_user_sfpsc l
LEFT JOIN stage_daily_user_connection_overhead r ON
        l.parse_user_id = r.parse_user_id AND l.device_serial_number = r.device_serial_number AND l.utc_start_date = r.utc_start_date;

DROP TABLE IF EXISTS stage_daily_user_sfpscohe CASCADE;
CREATE TABLE stage_daily_user_sfpscohe  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_date) AS
SELECT l.parse_user_id AS parse_user_id,
       l.parse_user_email AS parse_user_email,
       l.device_serial_number AS device_serial_number,
       l.utc_start_date AS utc_start_date,
       l.on_finger_seconds AS on_finger_seconds,
       l.off_finger_seconds AS off_finger_seconds,
       l.on_finger_ratio AS on_finger_ratio,
       l.off_finger_ratio AS off_finger_ratio,
       l.kl15_app_ver_count_f AS kl15_app_ver_count_f,
       l.kl15_app_ver_count_sc AS kl15_app_ver_count_sc,
       l.kl15_app_ver_count_oh AS kl15_app_ver_count_oh,
       l.kl15_app_ver AS kl15_app_ver,
       l.pair_duration AS pair_duration,
       l.bytes_written_per_day AS bytes_written_per_day,
       l.bytes_overhead AS bytes_overhead,
       l.net_bytes_per_day AS net_bytes_per_day,
       NVL(r.stuck_accel_events,0) AS stuck_accel_events,
       NVL(r.background_data_events,0) AS background_data_events,
       CASE WHEN l.accept=1 AND NVL(r.accept,1)=1 THEN 1 ELSE 0 END as accept
FROM stage_daily_user_sfpscoh l
LEFT JOIN stage_daily_user_other_events r ON
        l.parse_user_id = r.parse_user_id AND l.device_serial_number = r.device_serial_number AND l.utc_start_date = r.utc_start_date;

DROP TABLE IF EXISTS  daily_user_pair_duration CASCADE;
ALTER TABLE  stage_daily_user_pair_duration RENAME TO  daily_user_pair_duration;
DROP TABLE IF EXISTS  daily_user_other_events CASCADE;
ALTER TABLE  stage_daily_user_other_events RENAME TO  daily_user_other_events;
DROP TABLE IF EXISTS  daily_user_on_finger CASCADE;
ALTER TABLE  stage_daily_user_on_finger RENAME TO  daily_user_on_finger;
DROP TABLE IF EXISTS  daily_user_connection_overhead CASCADE;
ALTER TABLE  stage_daily_user_connection_overhead RENAME TO  daily_user_connection_overhead;
DROP TABLE IF EXISTS  daily_user_storage_counters CASCADE;
ALTER TABLE  stage_daily_user_storage_counters RENAME TO  daily_user_storage_counters;
DROP TABLE IF EXISTS  daily_user_sfpsc CASCADE;
ALTER TABLE  stage_daily_user_sfpsc RENAME TO  daily_user_sfpsc;
DROP TABLE IF EXISTS  daily_user_sfpscoh CASCADE;
ALTER TABLE  stage_daily_user_sfpscoh RENAME TO  daily_user_sfpscoh;
DROP TABLE IF EXISTS  daily_user_summary CASCADE;
ALTER TABLE  stage_daily_user_sfpscohe RENAME TO  daily_user_summary;

DROP TABLE IF EXISTS stage_user_fw_summary CASCADE;
CREATE TABLE stage_user_fw_summary  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(kl15_app_ver) AS
SELECT kl15_app_ver,parse_user_id, parse_user_email, device_serial_number,
       AVG(net_bytes_per_day) as avg_net_bytes_per_day,
       AVG(bytes_written_per_day) as avg_bytes_written_per_day,
       AVG(bytes_overhead) as avg_bytes_overhead,
       COUNT(bytes_written_per_day) as total_days
FROM daily_user_summary
WHERE accept = 1
GROUP BY kl15_app_ver,parse_user_id, parse_user_email, device_serial_number;

DROP TABLE IF EXISTS user_fw_summary CASCADE;
ALTER TABLE  stage_user_fw_summary RENAME TO  user_fw_summary;

-- AAA TODO: replace inline constants with constants table index'ed by fw version
DROP TABLE IF EXISTS stage_fw_summary CASCADE;
CREATE TABLE stage_fw_summary  DISTKEY(kl15_app_ver) INTERLEAVED SORTKEY(total_users) AS
SELECT kl15_app_ver,
       (254.0*1024.0)/(percentile_cont(0.2) within GROUP (ORDER BY avg_net_bytes_per_day DESC)) AS P20_storage_life_days,
       (254.0*1024.0)/(percentile_cont(0.5) within GROUP (ORDER BY avg_net_bytes_per_day DESC)) AS P50_storage_life_days,
       (254.0*1024.0)/(percentile_cont(0.8) within GROUP (ORDER BY avg_net_bytes_per_day DESC)) AS P80_storage_life_days,
       percentile_cont(0.2) within GROUP (ORDER BY avg_net_bytes_per_day DESC) AS P20_net_bytes_written_per_day,
       percentile_cont(0.5) within GROUP (ORDER BY avg_net_bytes_per_day DESC) AS P50_net_bytes_written_per_day,
       percentile_cont(0.8) within GROUP (ORDER BY avg_net_bytes_per_day DESC) AS P80_net_bytes_written_per_day,
       COUNT(parse_user_id) as total_users,
       SUM(total_days) as total_days,
       AVG(avg_net_bytes_per_day) as avg_net_bytes_per_day,
       (254.0*1024.0)/AVG(avg_net_bytes_per_day)::float as avg_storage_life_days,
       AVG(avg_bytes_written_per_day) as avg_bytes_written_per_day,
       AVG(avg_bytes_overhead) as avg_bytes_overhead
FROM user_fw_summary
GROUP BY kl15_app_ver;

DROP TABLE IF EXISTS fw_summary CASCADE;
ALTER TABLE  stage_fw_summary RENAME TO  fw_summary;


SELECT *
FROM fw_summary
ORDER BY kl15_app_ver LIMIT 1000;
