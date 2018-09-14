SELECT
  clientId AS cookies,
  SUM(totals.visits) AS totalSessions,
  SUM(totals.timeOnSite) AS totalDuration,
  SUM(totals.pageviews) AS totalPageviews,
  SUM(totals.timeOnSite) / SUM(totals.visits) AS avgTimePerSession,
  SUM(totals.pageviews) / SUM(totals.visits) AS avgPageviewsPerSession
FROM `gap-ua-65075410-1.40312565.ga_sessions_*`
WHERE
  clientId IS NOT NULL
  AND _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE '{date}')
GROUP BY
  cookies