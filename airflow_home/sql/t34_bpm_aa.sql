--DROP VIEW IF EXISTS t10_syslog_aa;

DROP TABLE IF EXISTS t34_bpm_stage;


CREATE TABLE t34_bpm_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time) AS
SELECT *,
       DATEADD(s,utc_start_time,'1970-01-01 00:00') AS date,
       DATE_TRUNC('DAY', DATEADD(s,utc_start_time,'1970-01-01 00:00')) AS DAY,
           lead(utc_start_time) OVER (PARTITION BY user_id, serial_number ORDER BY utc_start_time) AS next_utc_start_time,
           lead(utc_start_time) OVER (PARTITION BY user_id, serial_number ORDER BY utc_start_time) - utc_start_time AS next_utc_gap
FROM
  (SELECT DISTINCT raw_device_data.user_id,
                   raw_device_data.serial_number,
                   raw_device_data.sync_date,
                   raw_device_data.segment_number,
                   raw_device_data.utc_start_time,
                   raw_device_data.crc,
                   json_extract_array_element_text(raw_device_data.payload::text, 6)::integer AS bpm,
                   json_extract_array_element_text(raw_device_data.payload::text, 7)::integer AS sensor_time
   FROM raw_device_data raw_device_data
   WHERE (raw_device_data.data_type = 34)
     AND DATEADD(s,sync_date,'1970-01-01 00:00') >= DATEADD('DAY', -30,GETDATE()));

DROP TABLE IF EXISTS t34_bpm_fw_stage;
CREATE TABLE t34_bpm_fw_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, kl15_minor) AS
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
   INNER JOIN t34_bpm_stage t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start_time
   AND t1.utc_end_time >= t2.utc_start_time);

DROP TABLE IF EXISTS t34_bpm_fw CASCADE;
ALTER TABLE t34_bpm_fw_stage RENAME TO t34_bpm_fw;
DROP TABLE IF EXISTS t34_bpm_aa CASCADE;
ALTER TABLE t34_bpm_stage RENAME TO t34_bpm_aa;

DROP VIEW IF EXISTS t34_bpm_fw_sleep;
CREATE VIEW t34_bpm_fw_sleep AS
SELECT *
FROM
  (SELECT t2.*,
          NVL(t1.act_primary,0) AS act_primary,
          NVL(t1.act_secondary,0) AS act_secondary
   FROM
     (SELECT *
      FROM t7_activity_sleep) t1
   RIGHT JOIN t34_bpm_fw t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start_time
   AND t1.utc_end_time >= t2.utc_start_time);


SELECT *
FROM t34_bpm_fw_sleep
WHERE date > DATEADD('DAY',-1,GETDATE())
ORDER BY sync_date DESC,
         segment_number DESC LIMIT 100;
