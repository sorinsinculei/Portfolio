WITH sessions AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    user_pseudo_id,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'ga_session_id'
    ) AS session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    event_name = 'session_start'
),

events_aggregated AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    user_pseudo_id,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'ga_session_id'
    ) AS session_id,
    COUNTIF(event_name = 'add_to_cart') AS add_to_cart_count,
    COUNTIF(event_name = 'begin_checkout') AS begin_checkout_count,
    COUNTIF(event_name = 'purchase') AS purchase_count
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    event_name IN ('add_to_cart', 'begin_checkout', 'purchase')
  GROUP BY
    event_date, source, medium, campaign, user_pseudo_id, session_id
)

SELECT
  s.event_date,
  s.source,
  s.medium,
  s.campaign,
  COUNT(DISTINCT s.user_pseudo_id) AS user_sessions_count,
  SUM(e.add_to_cart_count) / COUNT(DISTINCT s.session_id) AS visit_to_cart,
  SUM(e.begin_checkout_count) / COUNT(DISTINCT s.session_id) AS visit_to_checkout,
  SUM(e.purchase_count) / COUNT(DISTINCT s.session_id) AS visit_to_purchase
FROM
  sessions AS s
LEFT JOIN
  events_aggregated AS e
ON
  s.user_pseudo_id = e.user_pseudo_id
  AND s.session_id = e.session_id
  AND s.event_date = e.event_date
  AND s.source = e.source
  AND s.medium = e.medium
  AND s.campaign = e.campaign
GROUP BY
  s.event_date,
  s.source,
  s.medium,
  s.campaign
ORDER BY
  s.event_date;