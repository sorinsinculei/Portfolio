SELECT 
    ad_date,
    campaign_id,
    SUM(spend) AS total_cost,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value,
    SUM(spend)/SUM(clicks) AS CPC,
    (SUM(spend)/SUM(impressions)) * 1000 as CPM,
    (SUM(clicks)/SUM(impressions)) * 100 as CTR,
    ((SUM(value)-SUM(spend))/SUM(spend)) * 100 as ROMI
from
    facebook_ads_basic_daily
WHERE
    clicks > 0
GROUP BY
    ad_date,
    campaign_id;
   
