DROP TABLE IF EXISTS rawsync.t28_storage_counters_stage;

CREATE TABLE rawsync.t28_storage_counters_stage
  DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_at)
AS
SELECT
  parse_user_id
  , u._created_at as parse_user_created_at
  , u.email as parse_user_email
  , DATEADD(s,f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4),'1970-01-01 00:00') AS utc_start_at
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 4) AS bytes_written
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 14, 4) AS bytes_read
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
WHERE tlv_type = 28;

DROP TABLE IF EXISTS rawsync.t28_storage_counters;
SET search_path TO rawsync;
ALTER TABLE rawsync.t28_storage_counters_stage RENAME TO t28_storage_counters;

SELECT utc_start_date, COUNT(utc_start_time_epoch)
FROM t28_storage_counters
GROUP BY utc_start_date
ORDER BY utc_start_date DESC
