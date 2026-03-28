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

SELECT 
  merchant_id,
  COUNT(*) AS total_txn,
  SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
  ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
FROM transactions
GROUP BY merchant_id
HAVING failure_rate > 30
ORDER BY failure_rate DESC;

SELECT t1.user_id, COUNT(*) AS txn_count
FROM transactions t1
JOIN transactions t2 
  ON t1.user_id = t2.user_id
 AND t2.created_at BETWEEN t1.created_at 
                         AND t1.created_at + INTERVAL 10 MINUTE
GROUP BY t1.user_id,t1.created_at
HAVING COUNT(*) >= 5;

WITH user_metrics AS (
  SELECT 
    user_id,
    COUNT(*) AS total_txn,
    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
    ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
  FROM transactions
  GROUP BY user_id
)
SELECT * FROM user_metrics;
