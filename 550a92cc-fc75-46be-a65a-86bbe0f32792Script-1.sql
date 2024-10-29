WITH
    ads_daily AS (
        SELECT 
            ad_date,
            campaign_id,
            SUM(spend) AS total_spend,
            SUM(impressions) AS total_impressions,
            SUM(clicks) AS total_clicks,
            SUM(value) AS total_value
        FROM 
            facebook_ads_basic_daily
        GROUP BY
            ad_date,
            campaign_id
    ),
    fb_adset AS (
        SELECT 
            adset_name,
            adset_id
        FROM 
            facebook_adset
    ),
    fb_campaign AS (
        SELECT 
            campaign_id,
            campaign_name
        FROM 
            facebook_campaign
    )
SELECT 
    ads_daily.ad_date,
    ads_daily.campaign_id,
    ads_daily.total_spend,
    ads_daily.total_impressions,
    ads_daily.total_clicks,
    ads_daily.total_value,
    fb_adset.adset_name,
    fb_campaign.campaign_name
FROM 
    ads_daily
LEFT JOIN 
    fb_campaign ON ads_daily.campaign_id = fb_campaign.campaign_id
LEFT JOIN 
    fb_adset ON ads_daily.campaign_id = fb_adset.adset_id

UNION ALL

SELECT 
    ad_date, 
    campaign_name,
    SUM(spend) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value
FROM
    google_ads_basic_daily
GROUP BY
    ad_date,
    campaign_name;
