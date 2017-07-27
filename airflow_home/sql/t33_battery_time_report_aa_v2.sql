
--DROP VIEW IF EXISTS t33_battery_time_report_aa CASCADE;
DROP TABLE IF EXISTS t33_battery_time_report_stage;



CREATE TABLE t33_battery_time_report_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start) AS
SELECT *,
       CASE WHEN (pct_start>pct_end) THEN round((100*(utc_end-utc_start)/(60*60*(pct_start-pct_end)))/2.4)/10.0 ELSE 0 END AS battery_life_days,
           CASE WHEN (pct_start>pct_end) THEN (100*(utc_end-utc_start)/(60*60*(pct_start-pct_end))) ELSE 0 END AS battery_life_hours,
       DATEADD(s,utc_start,'1970-01-01 00:00') AS date,
       DATE_TRUNC('DAY', DATEADD(s,utc_start,'1970-01-01 00:00')) AS DAY
FROM
  (SELECT DISTINCT raw_device_data.user_id,
                   raw_device_data.serial_number,
                   raw_device_data.sync_date,
                   raw_device_data.segment_number,
                   json_extract_array_element_text(raw_device_data.payload::text, 4)::int + (json_extract_array_element_text(raw_device_data.payload::text, 5)::int * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 6)::int * 256 * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 7)::int * 256 * 256 * 256) AS utc_start,
json_extract_array_element_text(raw_device_data.payload::text, 8)::int + (json_extract_array_element_text(raw_device_data.payload::text, 9)::int * 256) AS millivolt_start,
                                           json_extract_array_element_text(raw_device_data.payload::text, 10)::int AS pct_start,
                    json_extract_array_element_text(raw_device_data.payload::text, 11)::int AS status_start,
                    json_extract_array_element_text(raw_device_data.payload::text, 12)::int + (json_extract_array_element_text(raw_device_data.payload::text, 13)::int * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 14)::int * 256 * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 15)::int * 256 * 256 * 256) AS utc_end,
                                        json_extract_array_element_text(raw_device_data.payload::text, 16)::int + (json_extract_array_element_text(raw_device_data.payload::text, 17)::int * 256) AS millivolt_end,
                    json_extract_array_element_text(raw_device_data.payload::text, 18)::int AS pct_end,
                    json_extract_array_element_text(raw_device_data.payload::text, 19)::int AS status_end,
                    raw_device_data.crc
   FROM raw_device_data raw_device_data
WHERE raw_device_data.data_type = 33
AND DATEADD(s,sync_date,'1970-01-01 00:00') >= DATEADD('DAY', -30,GETDATE()));

DROP TABLE IF EXISTS t33_battery_time_report_fw_stage;
CREATE TABLE t33_battery_time_report_fw_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start, kl15_minor) AS
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
   INNER JOIN t33_battery_time_report_stage t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start
   AND t1.utc_end_time >= t2.utc_end);  -- this query will reject battery reports with mixed versioning


DROP TABLE IF EXISTS t33_battery_time_report_aa CASCADE;
ALTER TABLE t33_battery_time_report_stage RENAME TO t33_battery_time_report_aa;
DROP TABLE IF EXISTS t33_battery_time_report_fw CASCADE;
ALTER TABLE t33_battery_time_report_fw_stage RENAME TO t33_battery_time_report_fw;

DROP VIEW IF EXISTS battery_time_report_days;
CREATE VIEW battery_time_report_days AS
SELECT *
FROM t33_battery_time_report_fw
WHERE battery_life_days != 0 AND crc = TRUE ;



SELECT user_id, serial_number, fw_ver, battery_life_days, pct_start, pct_end, *
FROM
(SELECT *
 FROM battery_time_report_days
 WHERE (utc_start > (DATEDIFF(s,'1970-01-01 05:00',GETDATE())-15*24*60*60))
   AND (pct_start - pct_end > 60) ) --WHERE (battery_life_hours::float < 1.9*60.0*60.0)
ORDER BY kl15_ver DESC, pct_start-pct_end,
       pct_start DESC,
       pct_end LIMIT 100;
