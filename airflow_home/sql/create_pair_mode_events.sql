DROP TABLE IF EXISTS pair_mode_events_stage;
CREATE TABLE pair_mode_events_stage DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_time_epoch) AS
SELECT * FROM (SELECT parse_user_id, parse_user_email, device_serial_number,
    state_utc_start_at AS utc_start_at, state_utc_start_date as utc_start_date, state_utc_start_time_epoch as utc_start_time_epoch,
    next_utc_start_at AS utc_end_at, next_utc_start_date as utc_end_date, next_utc_start_time_epoch as utc_end_time_epoch,
   next_utc_start_time_epoch - state_utc_start_time_epoch AS duration,
   kl15_app_major, kl15_app_minor, kl15_app_major::varchar || '.'|| to_char(kl15_app_minor,'FM000') AS kl15_app_ver,
   tlv_synced_at, tlv_sequence_id,state
FROM (
SELECT * ,
    (lag(next_utc_start_time_epoch) over (partition BY  parse_user_id,device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS state_utc_start_time_epoch,
    (lag(next_utc_start_date) over (partition BY  parse_user_id,device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS state_utc_start_date,
    (lag(next_utc_start_at) over (partition BY  parse_user_id,device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS state_utc_start_at

FROM (SELECT *,
    (lead(utc_start_time_epoch) over (partition BY parse_user_id, device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS next_utc_start_time_epoch,
    (lead(utc_start_date) over (partition BY parse_user_id, device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS next_utc_start_date,
    (lead(utc_start_at) over (partition BY parse_user_id, device_serial_number
                                            ORDER BY tlv_synced_at, tlv_sequence_id)) AS next_utc_start_at

FROM (
  SELECT *,
          log_data0 AS state,
          lead(log_data0) over (partition BY parse_user_id, device_serial_number
                                ORDER BY tlv_synced_at, tlv_sequence_id) AS next_state
   FROM rawsync.t10_syslog
   WHERE log_id='M'
     AND log_code='CON'
     AND ((log_data0 =1)
          OR (log_data0 = 2))
) )
WHERE state != next_state
)) WHERE state = 2  --and duration > 1
;
--  AND next_state = 1 -- GROUP BY parse_user_id, device_serial_number
--date > DATEADD('DAY',-1,GETDATE())


DROP TABLE IF EXISTS pair_mode_events CASCADE;
ALTER TABLE pair_mode_events_stage RENAME TO pair_mode_events;

SELECT * FROM pair_mode_events
ORDER BY tlv_synced_at DESC,
         tlv_sequence_id DESC LIMIT 100;

         
