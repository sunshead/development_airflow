DROP TABLE IF EXISTS rawsync.t10_syslog_stage;

CREATE TABLE rawsync.t10_syslog_stage
  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_at, log_id, log_code)
AS
SELECT
  parse_user_id
  , u._created_at as parse_user_created_at
  , u.email as parse_user_email
  , f_base64_extract_little_endian_ascii(tlv_raw_bytes_base64, 4, 1) AS log_id
  , f_base64_extract_little_endian_ascii(tlv_raw_bytes_base64, 5, 3) AS log_code
  , DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 8, 4),'1970-01-01 00:00') AS utc_start_at
  , DATE_TRUNC('DAY', DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 8, 4),'1970-01-01 00:00')) AS utc_start_date
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 8, 4) AS utc_start_time_epoch
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 12, 1) AS log_data0
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 13, 1) AS log_data1
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
  , s3_raw_sync_file
  , sha512_signature
  , tlv_sequence_id
FROM rawsync.raw_tlvs tlv
FULL OUTER JOIN parse.user u ON u._metadata_doc_id = tlv.parse_user_id
WHERE tlv_type = 10;

DROP TABLE IF EXISTS rawsync.t10_syslog;
SET search_path TO rawsync;
ALTER TABLE rawsync.t10_syslog_stage RENAME TO t10_syslog;
