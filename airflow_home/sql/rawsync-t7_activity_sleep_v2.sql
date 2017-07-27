--DROP VIEW IF EXISTS t7_activity_sleep CASCADE;
DROP TABLE IF EXISTS t7_activity_sleep_stage;
CREATE TABLE t7_activity_sleep_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time) AS
SELECT *,
       DATEADD(s,utc_start_time,'1970-01-01 00:00') AS date,
       DATE_TRUNC('DAY', DATEADD(s,utc_start_time,'1970-01-01 00:00')) AS DAY
FROM (
  SELECT DISTINCT  raw_device_data.user_id,
                   raw_device_data.serial_number,
                   raw_device_data.sync_date,
                   raw_device_data.segment_number,
                   raw_device_data.utc_start_time,
                   raw_device_data.crc,
                  (json_extract_array_element_text(raw_device_data.payload::text, 6)::bigint +(json_extract_array_element_text(raw_device_data.payload::text, 7)::bigint * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 8)::bigint * 256 * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 9)::bigint * 256 * 256 * 256)) AS utc_end_time,
                  json_extract_array_element_text(raw_device_data.payload::text, 10)::int AS act_primary,
                  json_extract_array_element_text(raw_device_data.payload::text, 11)::int AS act_secondary
  FROM raw_device_data raw_device_data
  WHERE raw_device_data.data_type = 7 AND DATEADD(s,sync_date,'1970-01-01 00:00') >= DATEADD('DAY', -30,GETDATE()));

DROP TABLE IF EXISTS t7_activity_sleep_fw_stage;
CREATE TABLE t7_activity_sleep_fw_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, kl15_minor) AS
SELECT *
FROM
  (SELECT t2.*,
          t1.fw_ver,
          t1.kl15_ver,
          t1.kl15_major,
          t1.kl15_minor,
          t1.kl15_app,
          t1.kl15_alg,
          t1.nrf_app

   FROM
     (SELECT *
      FROM t19_version_changes) t1
   INNER JOIN t7_activity_sleep_stage t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start_time
   AND t1.utc_end_time >= t2.utc_start_time);

DROP TABLE IF EXISTS t7_activity_sleep_fw CASCADE;
ALTER TABLE t7_activity_sleep_fw_stage RENAME TO t7_activity_sleep_fw;
DROP TABLE IF EXISTS t7_activity_sleep CASCADE;
ALTER TABLE t7_activity_sleep_stage RENAME TO t7_activity_sleep;

SELECT * FROM t7_activity_sleep_fw ORDER BY date DESC LIMIT 100;
