-- recent_views_query.sql

WITH recent_views AS (
  SELECT
    pv.*,
    ROW_NUMBER() OVER (PARTITION BY pv.user_id ORDER BY pv.event_time DESC) AS row_num
  FROM page_views pv
  JOIN customers c ON pv.user_id = c.id
  WHERE
    c.plan_type = ?
    AND pv.page IN ('Pricing', 'Settings')
    AND pv.event_time >= DATETIME('now', '-7 days')
)
SELECT
  c.id        AS user_id,
  c.email,
  c.first_name,
  c.last_name,
  c.plan_type,
  c.candidate,
  rv.page,
  rv.device,
  rv.browser,
  rv.location,
  rv.event_time
FROM recent_views rv
JOIN customers c ON rv.user_id = c.id
WHERE rv.row_num = 1;
