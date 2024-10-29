SELECT
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  (
  SELECT
    value.int_value
  FROM
    UNNEST(event_params) AS ep
  WHERE
    KEY = 'ga_session_id') AS session_id,
  user_pseudo_id,
  stream_id,
  geo.country,
  device.category,
  traffic_source.source AS SOURCE,
  traffic_source.medium AS medium,
  item.promotion_name,
  event_name
  FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
  UNNEST(items) AS item
  WHERE  
    event_name IN ('session_start','view_item','add_to_cart','begin_checkout','add_payment_info','add_shipping_info','purchase')
LIMIT
  1000;









