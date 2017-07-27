--DROP VIEW IF EXISTS t31_finger_changes;
--DROP VIEW IF EXISTS t31_finger_changes_int;
--DROP VIEW IF EXISTS t31_finger;

DROP TABLE IF EXISTS t31_finger_stage;

CREATE TABLE t31_finger_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time) AS
SELECT  DISTINCT raw_device_data.user_id,
                 raw_device_data.serial_number,
                 raw_device_data.sync_date,
                 raw_device_data.segment_number,
                 raw_device_data.utc_start_time,
                                    raw_device_data.crc,
                                DATEADD(s,utc_start_time,'1970-01-01 00:00') AS date,
                                DATE_TRUNC('DAY', DATEADD(s,utc_start_time,'1970-01-01 00:00')) AS DAY,
                 json_extract_array_element_text(raw_device_data.payload::text, 6) AS finger_state
FROM raw_device_data raw_device_data
WHERE raw_device_data.data_type = 31
  AND raw_device_data.utc_start_time > 30*24*60*60
  AND user_id IS NOT NULL;

DROP VIEW IF EXISTS t31_finger_changes_int;
CREATE VIEW t31_finger_changes_int AS
SELECT d.*
FROM
  (SELECT d.*,
          lag(d.finger_state) OVER (PARTITION BY d.user_id, d.serial_number
                                    ORDER BY d.utc_start_time) AS previous_finger_state
   FROM t31_finger_stage d) AS d
WHERE d.finger_state != d.previous_finger_state;

DROP TABLE IF EXISTS t31_finger_changes_stage CASCADE;
CREATE TABLE t31_finger_changes_stage DISTKEY(user_id) INTERLEAVED SORTKEY(utc_start_time) AS
SELECT d.*,
       d.utc_end_time - d.utc_start_time AS duration,
(CASE WHEN d.finger_state::integer=1 THEN d.utc_end_time - d.utc_start_time ELSE 0 END)  AS on_finger_duration,
(CASE WHEN d.finger_state::integer=0 THEN d.utc_end_time - d.utc_start_time ELSE 0 END)  AS off_finger_duration
FROM
  (SELECT d.*,NVL(lead(d.utc_start_time) OVER (PARTITION BY d.user_id, d.serial_number
                                      ORDER BY d.utc_start_time),DATEDIFF(s,'1970-01-01 00:00',GETDATE())) AS utc_end_time,
          lag(d.utc_start_time) OVER (PARTITION BY d.user_id, d.serial_number
                                      ORDER BY d.utc_start_time) AS previous_utc_start_time
   FROM t31_finger_changes_int d) AS d;

DROP VIEW IF EXISTS t31_finger_changes_int;
DROP TABLE IF EXISTS  t31_finger CASCADE;
ALTER TABLE  t31_finger_stage RENAME TO  t31_finger;
DROP TABLE IF EXISTS  t31_finger_changes CASCADE;
ALTER TABLE  t31_finger_changes_stage RENAME TO  t31_finger_changes;

SELECT DAY, user_id, serial_number, finger_state, utc_start_time, utc_end_time, duration, on_finger_duration, off_finger_duration
FROM t31_finger_changes
ORDER BY --user_id DESC,
         DAY DESC, duration DESC, utc_start_time DESC LIMIT 1000;
