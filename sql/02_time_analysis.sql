SELECT 
  DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00') AS hour,
  gateway,
  COUNT(*) AS total_txn,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_txn
FROM transactions
GROUP BY hour, gateway
ORDER BY hour;

SELECT 
  gateway,
  ROUND(AVG(failure_rate),2) AS avg_failure_rate
FROM (
    SELECT 
      gateway,
      DATE_FORMAT(created_at, '%Y-%m-%d %H') AS hour,
      100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*) AS failure_rate
    FROM transactions
    GROUP BY gateway, hour
) t
GROUP BY gateway
ORDER BY avg_failure_rate DESC;

SELECT *
FROM (
    SELECT 
      gateway,
      DATE_FORMAT(created_at, '%Y-%m-%d %H') AS hour,
      COUNT(*) AS total_txn,
      SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
      ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
    FROM transactions
    GROUP BY gateway, hour
) t
WHERE failure_rate > 40
ORDER BY failure_rate DESC;

SELECT 
  gateway,
  DATE_FORMAT(created_at, '%Y-%m-%d %H') AS hour,
  SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
  ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) /
        SUM(SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)) OVER (PARTITION BY gateway), 2) 
        AS contribution_to_failures
FROM transactions
GROUP BY gateway, hour
ORDER BY contribution_to_failures DESC;

SELECT 
  user_id,
  COUNT(*) AS total_txn,
  SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
  ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
FROM transactions
GROUP BY user_id
HAVING failed_txn >= 5
ORDER BY failed_txn DESC;

SELECT 
  user_id,
  COUNT(*) AS txn_count,
  MIN(created_at) AS first_txn,
  MAX(created_at) AS last_txn,
  TIMESTAMPDIFF(MINUTE, MIN(created_at), MAX(created_at)) AS time_diff_minutes
FROM transactions
GROUP BY user_id
HAVING txn_count >= 10
ORDER BY txn_count DESC;
