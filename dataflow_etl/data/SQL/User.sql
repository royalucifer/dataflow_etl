SELECT
  PARSE_DATE('%Y%m%d', `date`) AS visitDate,
  clientId AS cookies,
  EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', `date`)) AS visitWeek,
  CASE
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 3 THEN 1 --00:00 ~ 02:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 6 THEN 2 --03:00 ~ 05:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 9 THEN 3 --06:00 ~ 08:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 12 THEN 4 --09:00 ~ 11:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 15 THEN 5 --12:00 ~ 14:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 18 THEN 6 --15:00 ~ 17:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 21 THEN 7 --18:00 ~ 20:59
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) < 24 THEN 8 --21:00 ~ 23:59
    ELSE 0
  END visitTime,
  CASE
    WHEN device.deviceCategory = 'mobile' THEN 1 --mobile
    WHEN device.deviceCategory = 'desktop' THEN 2 --desktop
    WHEN device.deviceCategory = 'tablet' THEN 3 --tablet
    ELSE 0
  END device,
  CASE
    WHEN device.mobileDeviceBranding = 'Apple' THEN 1 --'apple'
    WHEN device.mobileDeviceBranding = 'Samsung' THEN 2 --'samsung'
    WHEN device.mobileDeviceBranding = 'Asus' THEN 3 --'asus'
    WHEN device.mobileDeviceBranding = 'Sony' AND device.mobileDeviceBranding = 'SonyEricsson' THEN 4 --'sony'
    WHEN device.mobileDeviceBranding = 'HTC' THEN 5 --'htc'
    WHEN device.mobileDeviceBranding = 'OPPO' THEN 6 --'oppo'
    WHEN device.mobileDeviceBranding = 'Xiaomi' THEN 7 --'mi'
    WHEN device.mobileDeviceBranding = 'Huawei' THEN 8 --'huawei'
    WHEN device.mobileDeviceBranding = 'LG' THEN 9 --'lg'
    WHEN device.mobileDeviceBranding = 'Google' THEN 10 --'google'
    WHEN device.mobileDeviceBranding = 'Nokia' THEN 11 --'nokia'
    WHEN device.mobileDeviceBranding = 'InFocus' THEN 12 --'infocus'
    WHEN device.mobileDeviceBranding = 'Acer' THEN 13 --'acer'
    WHEN device.mobileDeviceBranding = 'Motorola' THEN 14 --'moto'
    WHEN device.mobileDeviceBranding = 'Vivo' THEN 15 --'vivo'
    WHEN device.mobileDeviceBranding = 'Sharp' THEN 16 --'sharp'
    ELSE 0
  END brand,
  CASE
    WHEN geoNetwork.region = 'Taipei City' AND geoNetwork.city = 'Keelung City' THEN 1 --'基隆'
    WHEN geoNetwork.region = 'Taipei City' THEN 2 --'台北'
    WHEN geoNetwork.region = 'New Taipei City' THEN 3 --'新北'
    WHEN geoNetwork.region = 'Taipei City' AND geoNetwork.city = 'New Taipei City' THEN 3 --'新北'
    WHEN geoNetwork.region = 'Taipei City' AND geoNetwork.city = 'Sanzhong District' THEN 3 --'新北'
    WHEN geoNetwork.region = 'Taoyuan County' THEN 4 --'桃園'
    WHEN geoNetwork.region = 'Taipei City' AND geoNetwork.city = 'Taoyuan District' THEN 4 --'桃園'
    WHEN geoNetwork.region = 'Hsinchu County' THEN 5 --'新竹'
    WHEN geoNetwork.region = 'Miaoli County' THEN 6 --'苗栗'
    WHEN geoNetwork.region = 'Taichung City' THEN 7 --'台中'
    WHEN geoNetwork.region = 'Nantou County' THEN 8 --'南投'
    WHEN geoNetwork.region = 'Changhua County' THEN 9 --'彰化'
    WHEN geoNetwork.region = 'Yunlin County' THEN 10 --'雲林'
    WHEN geoNetwork.region = 'Chiayi County' THEN 11 --'嘉義'
    WHEN geoNetwork.region = 'Tainan City' THEN 12 --'台南'
    WHEN geoNetwork.region = 'Kaohsiung City' THEN 13 --'高雄'
    WHEN geoNetwork.region = 'Pingtung County' THEN 14 --'屏東'
    WHEN geoNetwork.region = 'Yilan County' THEN 15 --'宜蘭'
    WHEN geoNetwork.region = 'Hualien County' THEN 16 --'花蓮'
    WHEN geoNetwork.region = 'Taitung County' THEN 17 --'台東'
    WHEN geoNetwork.region = 'Penghu County' THEN 18 --'澎湖'
    WHEN geoNetwork.country = 'Taiwan' THEN 19 --'台灣其他'
    ELSE 0
  END region,
  CASE
    WHEN REGEXP_CONTAINS(trafficSource.source, '(?i)fb')
      OR REGEXP_CONTAINS(trafficSource.source, '(?i)facebook')
    THEN 1 --'fb'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'line.me')
      OR REGEXP_CONTAINS(trafficSource.source, 'line-apps')
      OR REGEXP_CONTAINS(trafficSource.source, '_line')
      OR REGEXP_CONTAINS(trafficSource.source, 'line_')
      OR trafficSource.source = 'line'
      OR trafficSource.source = 'ednline'
      OR trafficSource.source = 'linepush'
      OR REGEXP_CONTAINS(trafficSource.source, 'L(?i)ine')
    THEN 2 --'line'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'google')
      OR trafficSource.source = 'search.app.goo.gl'
    THEN 3 --'google'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'edn')
      OR REGEXP_CONTAINS(trafficSource.source, 'EDN')
    THEN 4 --'edn'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'udn')
      OR REGEXP_CONTAINS(trafficSource.source, 'UDN')
    THEN 5 --'udn'
    WHEN trafficSource.source = 'bing'
      OR trafficSource.source = 'msn.com'
    THEN 6 --'bing'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'yahoo') THEN 7--'yahoo'
    WHEN trafficSource.source = 'nativeapp.toutiao.com' THEN 8--'toutiao'
    WHEN trafficSource.source = 'socialife.app.sony.jp' THEN 9--'sony_app'
    WHEN trafficSource.source = 'ptt.cc' THEN 10--'ptt'
    WHEN trafficSource.source = 'cmoney.tw' THEN 11--'cmoney'
    WHEN REGEXP_CONTAINS(trafficSource.source, 'cnyes') THEN 12--'cnyes'
    WHEN trafficSource.source = '(direct)' THEN 13--'direct'
    ELSE 0 --'other'
  END source,
  COUNT(*) AS num
FROM `gap-ua-65075410-1.40312565.ga_sessions_*`
WHERE
  clientId IS NOT NULL
  AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE '{date}')
GROUP BY
  cookies,
  visitDate,
  visitWeek,
  visitTime,
  device,
  brand,
  region,
  source