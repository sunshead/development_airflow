DROP TABLE IF EXISTS t33_battery_time_report_stage;

CREATE TABLE rawsync.t33_battery_time_report_stage DISTKEY(parse_user_id) INTERLEAVED SORTKEY(utc_start_at) AS
SELECT
  t1.*
  , CASE WHEN (pct_start > pct_end)
    THEN round((100*(utc_end_time_epoch-utc_start_time_epoch)/(60*60*(pct_start-pct_end)))/2.4)/10.0
    ELSE 0
  END AS battery_life_days
  , CASE WHEN (pct_start > pct_end)
    THEN (100*(utc_end_time_epoch-utc_start_time_epoch)/(60*60*(pct_start-pct_end)))
    ELSE 0
  END AS battery_life_hours
FROM
(
  SELECT
    parse_user_id
    , u._created_at AS parse_user_created_at
    , u.email AS parse_user_email
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 4, 2) AS crc_16
    , tlv_valid_crc  AS crc_valid

    , DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00') AS utc_start_at
    , DATE_TRUNC('DAY', DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4), '1970-01-01 00:00')) AS utc_start_date
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 6, 4) AS utc_start_time_epoch
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 10, 2) AS millivolt_start
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 12, 1) AS pct_start
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 13, 1) AS status_start

    , DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 14, 4), '1970-01-01 00:00') AS utc_end_at
    , DATE_TRUNC('DAY', DATEADD(s, f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 14, 4), '1970-01-01 00:00')) AS utc_end_date
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 14, 4) AS utc_end_time_epoch
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 18, 2) AS millivolt_end
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 20, 1) AS pct_end
    , f_base64_extract_little_endian_int(tlv_raw_bytes_base64, 21, 1) AS status_end

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
  WHERE tlv_type = 33
) t1
