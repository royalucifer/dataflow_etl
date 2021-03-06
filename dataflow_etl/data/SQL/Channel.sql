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
      WHEN title_0 = '文教' OR title_1 = '文教' OR title_2 = '文教' OR title_3 = '文教' THEN 'education' --'文教'
      WHEN title_0 = '地方' OR title_1 = '地方' OR title_2 = '地方' OR title_3 = '地方' THEN 'local' --'地方'
      WHEN title_0 = '即時' OR title_1 = '即時' OR title_2 = '即時' OR title_3 = '即時' THEN 'real_time' --'即時'
      WHEN title_0 = '兩岸' OR title_1 = '兩岸' OR title_2 = '兩岸' OR title_3 = '兩岸' THEN 'china' --'兩岸'
      WHEN title_0 = '社會' OR title_1 = '社會' OR title_2 = '社會' OR title_3 = '社會' THEN 'society' --'社會'
      WHEN title_0 = '數位' OR title_1 = '數位' OR title_2 = '數位' OR title_3 = '數位' THEN 'digital' --'數位'
      WHEN title_0 = '旅遊' OR title_1 = '旅遊' OR title_2 = '旅遊' OR title_3 = '旅遊' THEN 'travel' --'旅遊'

      WHEN title_0 = '噓！星聞' OR title_1 = '噓！星聞' OR title_2 = '噓！星聞' OR title_3 = '噓！星聞' THEN 'entertainment' --'娛樂'
      WHEN hostname = 'star.udn.com' THEN 'entertainment' --'娛樂'

      WHEN title_0 = '元氣網' OR title_1 = '元氣網' OR title_2 = '元氣網' OR title_3 = '元氣網' THEN 'health' --'健康'
      WHEN hostname = 'health.udn.com' THEN 'health' --'健康'

      WHEN title_0 = '發燒車訊' OR title_1 = '發燒車訊' OR title_2= '發燒車訊' OR title_3 = '發燒車訊' THEN 'car' --'汽車'
      WHEN hostname = 'autos.udn.com' THEN 'car' --'汽車'

      WHEN title_0 = 'udn遊戲角落' OR title_1 = 'udn遊戲角落' OR title_2= 'udn遊戲角落' OR title_3 = 'udn遊戲角落' THEN 'game' --'遊戲'
      WHEN hostname = 'game.udn.com' THEN 'game' --'遊戲'

      WHEN title_0 = '房地產' OR title_1 = '房地產' OR title_2= '房地產' OR title_3 = '房地產' THEN 'house' --'房產'
      WHEN hostname = 'house.udn.com' THEN 'house' --'房產'

      WHEN title_0 = 'OOPS! 新鮮事' OR title_1 = 'OOPS! 新鮮事' OR title_2= 'OOPS! 新鮮事' OR title_3 = 'OOPS! 新鮮事' THEN 'fun' --'趣聞'
      WHEN hostname = 'oops.udn.com' THEN 'fun' --'趣聞'

      WHEN title_0 = 'udnSTYLE 時尚.名人.生活風格' OR title_1 = 'udnSTYLE 時尚.名人.生活風格' OR title_2= 'udnSTYLE 時尚.名人.生活風格' OR title_3 = 'udnSTYLE 時尚.名人.生活風格' THEN 'fashion' --'時尚'
      WHEN hostname = 'style.udn.com' THEN 'fashion' --'時尚'

      WHEN title_0 = '2018九合一選舉' OR title_1 = '2018九合一選舉' THEN 'politic' --'政治'
      WHEN title_1 IN ('政治','九合一選戰焦點','藍綠各黨拚選戰','藍綠各黨拚2018','戰北市選情有變','九合一選後焦點') THEN 'politic' --'政治'
      WHEN title_0 = '要聞' OR title_1 = '要聞' OR title_2 = '要聞' OR title_3 = '要聞' THEN 'news' --'要聞'

      WHEN title_1 = '流行消費' THEN 'shopping' --'消費'
      WHEN title_0 = '生活' OR title_1 = '生活' OR title_2 = '生活' OR title_3 = '生活' THEN 'life' --'生活'

      WHEN title_0 = '股市' OR title_1 = '股市' OR title_2 = '股市' OR title_3 = '股市' THEN 'investment' --'投資'
      WHEN title_0 = '基金' OR title_1 = '基金' OR title_2 = '基金' OR title_3 = '基金' THEN 'investment' --'投資'
      WHEN hostname = 'fund.udn.com' THEN 'investment' --'投資'

      WHEN title_0 = '產經' OR title_1 = '產經' OR title_2 = '產經' OR title_3 = '產經' THEN 'finance' --'財經'
      WHEN title_0 = '經濟日報' OR title_1 = '經濟日報' OR title_2 = '經濟日報' OR title_3 = '經濟日報' THEN 'finance' --'財經'
      WHEN hostname = 'money.udn.com' THEN 'finance' --'投資'

      WHEN title_0 = '全球' OR title_1 = '全球' OR title_2 = '全球' OR title_3 = '全球' THEN 'global' --'國際'
      WHEN title_0 = '轉角國際' OR title_1 = '轉角國際' OR title_2 = '轉角國際 udn Global' OR title_3 = '轉角國際 udn Global' THEN 'global' --'國際'
      WHEN hostname = 'global.udn.com' THEN 'global' --'國際'

      WHEN title_0 = '倡議家' OR title_1 = '倡議家' OR title_2 = '倡議家' OR title_3 = '倡議家' THEN 'topic' --'專題'
      WHEN title_0 = '時事話題' OR title_1 = '時事話題' OR title_2 = '時事話題' OR title_3 = '時事話題' THEN 'topic' --'專題'
      WHEN hostname = 'vision.udn.com' OR page_lv1 = '/upf/' THEN 'topic' --'專題'
      WHEN hostname = 'topic.udn.com' THEN 'topic' --'專題'
      WHEN hostname = 'ubrand.udn.com' THEN 'topic' --'專題'

      WHEN title_0 = '評論' OR title_1 = '評論' OR title_2 = '評論' OR title_3 = '評論' THEN 'comment' --'評論'
      WHEN title_0 = '鳴人堂' OR title_1 = '鳴人堂' OR title_2 = '鳴人堂' OR title_3 = '鳴人堂' THEN 'comment' --'評論'
      WHEN hostname = 'opinion.udn.com' THEN 'comment' --'評論'

      WHEN title_0 = '運動' OR title_1 = '運動' OR title_2 = '運動' OR title_3 = '運動' THEN 'sport' --'運動'
      WHEN title_0 = '2018世足賽起義時刻' OR title_1 = '2018世足賽起義時刻' OR title_2 = '2018世足賽起義時刻' OR title_3 = '2018世足賽起義時刻' THEN 'sport' --'運動'
      WHEN title_0 = 'NBA 台灣' OR title_1 = 'NBA 台灣' OR title_2 = 'NBA 台灣' OR title_3 = 'NBA 台灣' THEN 'sport' --'運動'
      WHEN hostname = 'nba.udn.com' THEN 'sport' --'運動'

      WHEN title_0 = '雜誌' OR title_1 = '雜誌' OR title_2 = '雜誌' OR title_3 = '雜誌' THEN 'book' --'閱讀'
      WHEN title_0 = 'udn 讀書吧' OR title_1 = 'udn 讀書吧' OR title_2 = 'udn 讀書吧' OR title_3 = 'udn 讀書吧' THEN 'book' --'閱讀'
      WHEN title_0 = 'udn 讀書吧' OR title_1 = 'udn 讀書吧' OR title_2 = '讀創故事' OR title_3 = '讀創故事' THEN 'book' --'閱讀'
      WHEN title_0 = '讀.書.人' OR title_1 = '讀.書.人' OR title_2 = '讀.書.人' OR title_3 = '讀.書.人' THEN 'book' --'閱讀'

      WHEN hostname = 'video.udn.com' THEN 'video' --'影音'
      WHEN hostname IN ('udesign.udnfunlife.com', 'tickets.udnfunlife.com', 'shopping.udn.com') THEN 'e_commerce' --'電商'

      ELSE 'other' --'其他'
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
        AND clientId IS NOT NULL
        AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE '{date}')
        AND h.page.hostname NOT IN (
          'member.udn.com',
          'blog.udn.com',
          'classic-blog.udn.com',
          'udndata.com',
          'paper.udn.com') ) ))
GROUP BY
  cookies,
  channel