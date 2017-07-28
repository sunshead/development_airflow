SELECT DISTINCT raw_device_data.user_id
FROM raw_device_data raw_device_data
WHERE (raw_device_data.data_type = 34)
  AND DATEADD(s,sync_date,'1970-01-01 00:00') >= DATEADD('DAY', -30,GETDATE())
  LIMIT 2;
