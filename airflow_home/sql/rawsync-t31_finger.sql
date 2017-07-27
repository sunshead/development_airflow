DROP TABLE IF EXISTS rawsync.t31_finger_stage CASCADE;
CREATE TABLE rawsync.t31_finger_stage
  DISTKEY (parse_user_id) INTERLEAVED SORTKEY (utc_start_at)
AS
SELECT
  parse_user_id
  , u._created_at AS parse_created_at
  , u.email AS parse_user_email

  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 4, 2) AS crc_16
  , tlv_valid_crc AS crc_valid

  , DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00') AS utc_start_at
  , DATE_TRUNC('DAY', DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00')) AS utc_start_date
  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4) AS utc_start_time_epoch

  , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 1) AS finger_state

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
  FULL OUTER JOIN parse.user u
    ON u._metadata_doc_id = tlv.parse_user_id
WHERE tlv_type = 31;

DROP TABLE IF EXISTS rawsync.t31_finger CASCADE;
SET search_path TO rawsync;
ALTER TABLE rawsync.t31_finger_stage
  RENAME TO t31_finger;

DROP TABLE IF EXISTS rawsync.t31_finger_changes_int CASCADE;
CREATE TABLE rawsync.t31_finger_changes_int
  DISTKEY (parse_user_id) INTERLEAVED SORTKEY (utc_start_at)
AS
SELECT
  t1.*
FROM
(
  SELECT
    f.*
    , LAG(f.finger_state) OVER (
  PARTITION BY f.parse_user_id, f.device_serial_number
  ORDER BY f.utc_start_at) AS previous_finger_state
  FROM rawsync.t31_finger f
) t1
WHERE t1.previous_finger_state != t1.finger_state;

DROP TABLE IF EXISTS rawsync.t31_finger_change_stage;
CREATE TABLE rawsync.t31_finger_change_stage
  DISTKEY (parse_user_id) INTERLEAVED SORTKEY (utc_start_at)
AS
SELECT
  t1.*
  , (t1.utc_end_time_epoch - t1.utc_start_time_epoch) AS duration
  --, (CASE WHEN t1.finger_state = 1 THEN t1.utc_end_time_epoch - t1.utc_start_time_epoch ELSE 0 END) AS on_finger_duration
  --, (CASE WHEN t1.finger_state = 0 THEN t1.utc_end_time_epoch - t1.utc_start_time_epoch ELSE 0 END) AS off_finger_duration
FROM
(
  SELECT
    d.*
    , NVL(LEAD(d.utc_start_at)
          OVER (
            PARTITION BY d.parse_user_id, d.device_serial_number
            ORDER BY d.utc_start_at ), GETDATE()) AS utc_end_at
    , LAG(d.utc_start_at)
      OVER (
        PARTITION BY d.parse_user_id, d.device_serial_number
        ORDER BY d.utc_start_at) AS previous_utc_start_at
  FROM t31_finger_changes_int d
) AS t1;

DROP TABLE IF EXISTS rawsync.t31_finger_changes CASCADE;
SET search_path TO rawsync;
ALTER TABLE rawsync.t31_finger_changes_stage
  RENAME TO t31_finger_changes;
