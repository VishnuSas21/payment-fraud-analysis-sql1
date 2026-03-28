SELECT 
  gateway,
  COUNT(*) AS total_txn,
  SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_txn
FROM transactions
GROUP BY gateway;