
DROP TABLE IF EXISTS t19_version_stage CASCADE;


CREATE TABLE t19_version_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, kl15_minor, nrf_app) AS
SELECT *,
       kl15_major::varchar || '.'||kl15_minor::varchar||'-'||nrf_app::varchar AS fw_ver,
       kl15_major::varchar || '.'||to_char(kl15_minor,'FM000') AS kl15_ver,
       DATEADD(s,utc_start_time,'1970-01-01 00:00') AS date,
       DATE_TRUNC('DAY', DATEADD(s,utc_start_time,'1970-01-01 00:00')) AS DAY
FROM
  (SELECT raw_device_data.created_at,
          raw_device_data.user_id,
          raw_device_data.sync_date,
          raw_device_data.segment_number,
          raw_device_data.crc,
          raw_device_data.serial_number,
          json_extract_array_element_text((raw_device_data.payload)::text, 0)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 1)::integer) AS crc16,
                                                                                         (json_extract_array_element_text((raw_device_data.payload)::text, 2)::integer) AS kl15_minor,
                                                                                         (json_extract_array_element_text((raw_device_data.payload)::text, 3)::integer) AS kl15_major,
                                                                                         json_extract_array_element_text((raw_device_data.payload)::text, 2)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 3)::integer) AS kl15_app,
                                                                                                                                                                        json_extract_array_element_text((raw_device_data.payload)::text, 4)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 5)::integer) AS kl15_boot,
json_extract_array_element_text((raw_device_data.payload)::text, 6)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 7)::integer) AS kl15_alg,
                                                                               json_extract_array_element_text((raw_device_data.payload)::text, 8)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 9)::integer) AS nrf_boot,
                                                                                                                                                              json_extract_array_element_text((raw_device_data.payload)::text, 10)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 11)::integer) AS nrf_app,
json_extract_array_element_text((raw_device_data.payload)::text, 12)::integer + (256*json_extract_array_element_text((raw_device_data.payload)::text, 13)::integer) AS nrf_sd,
                                                                                json_extract_array_element_text(raw_device_data.payload::text, 16)::int + (json_extract_array_element_text(raw_device_data.payload::text, 17)::int * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 18)::int * 256 * 256) + (json_extract_array_element_text(raw_device_data.payload::text, 19)::int * 256 * 256 * 256) AS utc_start_time
   FROM raw_device_data raw_device_data
WHERE (raw_device_data.data_type = 19)
AND user_id IS NOT NULL);


DROP TABLE IF EXISTS t19_version_changes_stage CASCADE;


CREATE TABLE t19_version_changes_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, kl15_minor, nrf_app) AS
SELECT a.*,
     a.utc_end_time - a.utc_start_time AS duration
FROM
( SELECT b.*,
         NVL(lead(b.utc_start_time) OVER (PARTITION BY b.user_id, b.serial_number
                                          ORDER BY b.utc_start_time),DATEDIFF(s,'1970-01-01 00:00',GETDATE())) AS utc_end_time
 FROM
   ( SELECT c.*
    FROM
      ( SELECT d.*,
               lag(d.fw_ver) OVER (PARTITION BY d.user_id, d.serial_number
                                   ORDER BY d.utc_start_time) AS prev_fw_ver
       FROM t19_version_stage d) AS c
    WHERE c.fw_ver != c.prev_fw_ver
      AND c.utc_start_time > 365*24*60*60) AS b) AS a;


DROP TABLE IF EXISTS t19_version CASCADE;
ALTER TABLE t19_version_stage RENAME TO t19_version;
DROP TABLE IF EXISTS t19_version_changes CASCADE;
ALTER TABLE t19_version_changes_stage RENAME TO t19_version_changes;

DROP TABLE IF EXISTS t35_alg_perf_stage CASCADE;
CREATE TABLE t35_alg_perf_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time) AS
SELECT DISTINCT raw_device_data.user_id, raw_device_data.serial_number,
DATE_TRUNC('DAY', DATEADD(s,raw_device_data.utc_start_time,'1970-01-01 00:00')) AS day,
DATEADD(s,raw_device_data.utc_start_time,'1970-01-01 00:00') AS start_date,
json_extract_array_element_text(raw_device_data.payload::text, 8)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 9)::int * 256) AS data_collect,
json_extract_array_element_text(raw_device_data.payload::text, 10)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 11)::int * 256) AS accel_resamp,
json_extract_array_element_text(raw_device_data.payload::text, 12)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 13)::int * 256) AS ppg_resamp,
json_extract_array_element_text(raw_device_data.payload::text, 14)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 15)::int * 256) AS ped_alg,
json_extract_array_element_text(raw_device_data.payload::text, 16)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 17)::int * 256) AS sleep_alg,
json_extract_array_element_text(raw_device_data.payload::text, 18)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 19)::int * 256) AS ppg_base,
json_extract_array_element_text(raw_device_data.payload::text, 20)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 21)::int * 256) AS ppg_filter,
json_extract_array_element_text(raw_device_data.payload::text, 22)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 23)::int * 256) AS optic,
json_extract_array_element_text(raw_device_data.payload::text, 24)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 25)::int * 256) AS finger,
json_extract_array_element_text(raw_device_data.payload::text, 26)::int +
(json_extract_array_element_text(raw_device_data.payload::text, 27)::int * 256) AS total,
raw_device_data.sync_date, raw_device_data.segment_number, raw_device_data.utc_start_time,
--raw_device_data.payload,
raw_device_data.crc
   FROM raw_device_data raw_device_data
  WHERE raw_device_data.data_type = 35;

DROP TABLE IF EXISTS t35_alg_perf_fw_stage;
CREATE TABLE t35_alg_perf_fw_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time, kl15_minor) AS
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
   INNER JOIN t35_alg_perf_stage t2 ON t1.user_id = t2.user_id
   AND t1.serial_number = t2.serial_number
   AND t1.utc_start_time <= t2.utc_start_time
   AND t1.utc_end_time >= t2.utc_start_time);



DROP TABLE IF EXISTS t35_alg_perf_aa CASCADE;
ALTER TABLE t35_alg_perf_stage RENAME TO t35_alg_perf_aa;
DROP TABLE IF EXISTS t35_alg_perf_fw CASCADE;
ALTER TABLE t35_alg_perf_fw_stage RENAME TO t35_alg_perf_fw;

SELECT day, user_id,
     serial_number,
     fw_ver,
     prev_fw_ver,
     utc_start_time,
     utc_end_time,
     duration,
     *
FROM t19_version_changes
ORDER BY day DESC, user_id, utc_start_time DESC LIMIT 100;

 --SELECT *
--FROM t19_version
--WHERE duration > 365*24*60*60
--ORDER BY utc_start, kl15_major DESC, kl15_minor DESC, nrf_app DESC LIMIT 100;
