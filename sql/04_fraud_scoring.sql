WITH user_metrics AS (
  SELECT 
    user_id,
    COUNT(*) AS total_txn,
    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
    ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
  FROM transactions
  GROUP BY user_id
)

SELECT 
  user_id,
  total_txn,
  failed_txn,
  failure_rate,

  -- Rule 1: High failure rate
  CASE WHEN failure_rate > 30 THEN 2 ELSE 0 END AS rule_failure,

  -- Rule 2: Too many transactions
  CASE WHEN total_txn >= 10 THEN 1 ELSE 0 END AS rule_volume,

  -- Final score
  (CASE WHEN failure_rate > 30 then 2 else 0 end +
   CASE WHEN total_txn >= 10 THEN 1 ELSE 0 END) AS fraud_score

FROM user_metrics;

WITH velocity_flag AS (
  SELECT DISTINCT t1.user_id
  FROM transactions t1
  JOIN transactions t2 
    ON t1.user_id = t2.user_id
   AND t2.created_at BETWEEN t1.created_at 
                           AND t1.created_at + INTERVAL 10 MINUTE
  GROUP BY t1.user_id, t1.created_at
  HAVING COUNT(*) >= 5
),

user_metrics AS (
  SELECT 
    user_id,
    COUNT(*) AS total_txn,
    SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn,
    ROUND(100.0 * SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate
  FROM transactions
  GROUP BY user_id
)

SELECT 
  u.user_id,
  total_txn,
  failed_txn,
  failure_rate,

  CASE WHEN failure_rate > 30 THEN 2 ELSE 0 END AS rule_failure,
  CASE WHEN total_txn >= 10 THEN 1 ELSE 0 END AS rule_volume,
  CASE WHEN v.user_id IS NOT NULL THEN 3 ELSE 0 END AS rule_velocity,

  (
    CASE WHEN failure_rate > 30 THEN 2 ELSE 0 END +
    CASE WHEN total_txn >= 10 THEN 1 ELSE 0 END +
    CASE WHEN v.user_id IS NOT NULL THEN 3 ELSE 0 END
  ) AS fraud_score

FROM user_metrics u
LEFT JOIN velocity_flag v 
  ON u.user_id = v.user_id
ORDER BY fraud_score DESC;
