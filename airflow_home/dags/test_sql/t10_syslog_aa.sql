--DROP VIEW IF EXISTS t10_syslog_aa;
DROP TABLE IF EXISTS airflow_test.t10_syslog_stage;

CREATE TABLE airflow_test.t10_syslog_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, code) AS
SELECT *,
       DATEADD(s,utc_start_time,'1970-01-01 00:00') AS date,
       DATE_TRUNC('DAY', DATEADD(s,utc_start_time,'1970-01-01 00:00')) AS DAY
FROM
  ( SELECT DISTINCT raw_device_data.user_id,
                    raw_device_data.serial_number,
                    raw_device_data.sync_date,
                    raw_device_data.segment_number,
                    chr(json_extract_array_element_text(raw_device_data.payload::text, 0)::integer) AS id,
                    chr(json_extract_array_element_text(raw_device_data.payload::text, 1)::integer) + chr(json_extract_array_element_text(raw_device_data.payload::text, 2)::integer) + chr(json_extract_array_element_text(raw_device_data.payload::text, 3)::integer) AS code,
                                                                                                                                                                                        (json_extract_array_element_text(raw_device_data.payload::text, 4)::bigint + 256 * json_extract_array_element_text(raw_device_data.payload::text, 5)::bigint + 256 * 256 * json_extract_array_element_text(raw_device_data.payload::text, 6)::bigint + 256 * 256 * 256 * json_extract_array_element_text(raw_device_data.payload::text, 7)::bigint) AS utc_start_time,
                                                                                                                                                                                        json_extract_array_element_text((raw_device_data.payload)::text, 8)::integer AS data0,
                                                                                                                                                                                        json_extract_array_element_text((raw_device_data.payload)::text, 9)::integer AS data1
   FROM raw_device_data raw_device_data
   WHERE (raw_device_data.data_type = 10)
     AND DATEADD(s,sync_date,'1970-01-01 00:00') >= DATEADD('DAY', -30,GETDATE()));

DROP TABLE IF EXISTS airflow_test.t10_syslog_fw_stage;
CREATE TABLE airflow_test.t10_syslog_fw_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, code) AS
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
   INNER JOIN airflow_test.t10_syslog_stage t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start_time
   AND t1.utc_end_time >= t2.utc_start_time);

DROP TABLE IF EXISTS airflow_test.t10_syslog_aa CASCADE;
ALTER TABLE airflow_test.t10_syslog_stage RENAME TO t10_syslog_aa;
DROP TABLE IF EXISTS airflow_test.t10_syslog_fw CASCADE;
ALTER TABLE airflow_test.t10_syslog_fw_stage RENAME TO t10_syslog_fw;

SELECT *
FROM airflow_test.t10_syslog_fw
WHERE date > DATEADD('DAY',-1,GETDATE())
ORDER BY sync_date DESC,
         segment_number DESC LIMIT 100;
