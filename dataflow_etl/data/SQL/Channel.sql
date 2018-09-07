#standardSQL
SELECT
  cookies,
  channel,
  COUNT(*) AS totalPageviews,
  SUM(duration) / 1000 AS totalDuration,
  AVG(duration) / 1000 AS avgDuration
FROM (
  SELECT
    cookies,
    CASE
      WHEN title_2 = '文教' OR title_3 = '文教' THEN '文教'
      WHEN title_2 = '生活' OR title_3 = '生活' THEN '生活'
      WHEN title_2 = '地方' OR title_3 = '地方' THEN '地方'
      WHEN title_2 = '即時' OR title_3 = '即時' THEN '即時'
      WHEN title_2 = '兩岸' OR title_3 = '兩岸' THEN '兩岸'
      WHEN title_2 = '社會' OR title_3 = '社會' THEN '社會'
      WHEN title_2 = '數位' OR title_3 = '數位' THEN '數位'
      WHEN title_2 = '旅遊' OR title_3 = '旅遊' THEN '旅遊'
      WHEN title_2 = '噓！星聞' OR title_3 = '噓！星聞' THEN '娛樂'
      WHEN title_1 = '元氣網' OR title_2 = '元氣網' OR title_3 = '元氣網' THEN '健康'
      WHEN title_3 = '發燒車訊' THEN '汽車'
      WHEN title_3 = 'udn遊戲角落' THEN '遊戲'
      WHEN title_3 = '房地產' THEN '房產'
      WHEN title_3 = 'OOPS! 新鮮事' THEN '趣聞'
      WHEN title_3 = 'udnSTYLE 時尚.名人.生活風格' THEN '時尚'

      WHEN title_0 = '2018九合一選舉' OR title_1 = '2018九合一選舉' THEN '政治'
      WHEN title_1 = '政治' OR title_1 = '九合一選戰焦點' THEN '政治'
      WHEN title_2 = '要聞' OR title_3 = '要聞' THEN '要聞'

      WHEN title_2 = '產經' OR title_3 = '產經' THEN '財經'
      WHEN title_2 = '股市' OR title_3 = '股市' THEN '財經'
      WHEN title_2 = '基金' OR title_3 = '基金' THEN '財經'
      WHEN title_3 = '經濟日報' THEN '財經'

      WHEN title_2 = '全球' OR title_3 = '全球' THEN '國際'
      WHEN title_2 = '轉角國際 udn Global' OR title_3 = '轉角國際 udn Global' THEN '國際'

      WHEN title_2 = '倡議家' OR title_3 = '倡議家' THEN '專題'
      WHEN title_2 = '時事話題' OR title_3 = '時事話題' THEN '專題'
      WHEN hostname = 'vision.udn.com' OR page_lv1 = '/upf/' THEN '專題'
      WHEN hostname = 'topic.udn.com'THEN '專題'

      WHEN title_2 = '評論' OR title_3 = '評論' THEN '評論'
      WHEN title_2 = '鳴人堂' OR title_3 = '鳴人堂' THEN '評論'

      WHEN title_2 = '運動' OR title_3 = '運動' THEN '運動'
      WHEN title_0 = '2018世足賽起義時刻' OR title_1 = '2018世足賽起義時刻' OR title_2 = '2018世足賽起義時刻' OR title_3 = '2018世足賽起義時刻' THEN '運動'
      WHEN title_0 = 'NBA 台灣' OR title_1 = 'NBA 台灣' OR title_2 = 'NBA 台灣' OR title_3 = 'NBA 台灣' THEN '運動'

      WHEN title_2 = '雜誌' OR title_3 = '雜誌' THEN '閱讀'
      WHEN title_2 = 'udn 讀書吧' OR title_3 = 'udn 讀書吧' THEN '閱讀'
      WHEN title_2 = '讀創故事' OR title_3 = '讀創故事' THEN '閱讀'
      WHEN title_3 = '讀.書.人' THEN '閱讀'

      WHEN hostname IN ('udesign.udnfunlife.com', 'tickets.udnfunlife.com', 'shopping.udn.com') THEN '電商'

      ELSE '其他'
    END channel,
    duration
  FROM (
    SELECT
      cookies,
      hostname,
      page_lv1,
      SPLIT(title, ' | ')[SAFE_OFFSET(0)] AS title_0,
      SPLIT(title, ' | ')[SAFE_OFFSET(1)] AS title_1,
      SPLIT(title, ' | ')[SAFE_OFFSET(2)] AS title_2,
      SPLIT(title, ' | ')[SAFE_OFFSET(3)] AS title_3,
      view_time,
      lead_view_time - view_time AS duration
    FROM (
      SELECT
        clientId AS cookies,
        h.page.hostname AS hostname,
        h.page.pagePathLevel1 AS page_lv1,
        h.page.pageTitle AS title,
        h.time AS view_time,
        LEAD(h.time) OVER (PARTITION BY clientId, visitNumber ORDER BY h.hitNumber) AS lead_view_time
      FROM
        `gap-ua-65075410-1.40312565.ga_sessions_*`, UNNEST(hits) AS h
      WHERE
        h.type = 'PAGE'
        AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE '{date}')
        AND h.page.hostname NOT IN (
          'member.udn.com',
          'blog.udn.com',
          'classic-blog.udn.com',
          'udndata.com',
          'paper.udn.com',
          'video.udn.com') ) ))
GROUP BY
  cookies,
  channel