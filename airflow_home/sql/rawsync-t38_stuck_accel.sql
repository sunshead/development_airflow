DROP TABLE IF EXISTS rawsync.t38_stuck_accel_stage;
CREATE TABLE rawsync.t38_stuck_accel_stage
  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_at, duration_sec)
AS
SELECT
  parse_user_id
  , u._created_at as parse_user_created_at
  , u.email as parse_user_email
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 4, 2) as crc_16
  , tlv_valid_crc as crc_valid
  , DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4),'1970-01-01 00:00') AS utc_start_at
  , DATE_TRUNC('DAY', DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4),'1970-01-01 00:00')) AS utc_start_date
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4) AS utc_start_time_epoch

  , DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 4),'1970-01-01 00:00') AS utc_end_at
  , DATE_TRUNC('DAY', DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 4),'1970-01-01 00:00')) AS utc_end_date
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 4) AS utc_end_time_epoch

  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 4) - f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4) as duration_sec

  , device_serial_number
  , kl15_app_major
  , kl15_app_minor
  , kl15_boot_major
  , kl15_boot_minor
  , nrf_app_major
  , nrf_app_minor
  , nrf_boot_major
  , nrf_boot_minor
  , app_version_name
  , app_version_code
  , hardware_major
  , hardware_minor
  , ingested_at
  , tlv_synced_at
FROM rawsync.raw_tlvs tlv
  FULL OUTER JOIN parse.user u ON u._metadata_doc_id = tlv.parse_user_id
WHERE tlv_type = 38;

DROP TABLE IF EXISTS rawsync.t38_stuck_accel;
SET search_path TO rawsync;
ALTER TABLE rawsync.t38_stuck_accel_stage RENAME TO t38_stuck_accel;

SELECT utc_start_date, COUNT(utc_start_time_epoch)
FROM t38_stuck_accel
GROUP BY utc_start_date
ORDER BY utc_start_date DESC;