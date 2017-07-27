DROP TABLE IF EXISTS t22_steps_activity_stage;

CREATE TABLE rawsync.t22_steps_activity_stage DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_at) AS
SELECT
  parse_user_id
  , u._created_at AS parse_user_created_at
  , u.email AS parse_user_email
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 4, 2) AS crc_16
  , tlv_valid_crc  AS crc_valid

  , DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00') AS utc_start_at
  , DATE_TRUNC('DAY', DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00')) AS utc_start_date
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4) AS utc_start_time_epoch

  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 1) AS duration
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 11, 1) AS steps
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 12, 1) AS step_type

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
WHERE tlv_type = 22;

DROP TABLE IF EXISTS rawsync.t22_steps_activity CASCADE;
SET search_path TO rawsync;
ALTER TABLE rawsync.t22_steps_activity_stage
  RENAME TO t22_steps_activity;
