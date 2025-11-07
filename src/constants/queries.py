drop_query = """
DROP TABLE IF EXISTS _TablePlaceHolder_;
"""
transform_query = """
SELECT
    a.transaction_id,
    DATE(a.transaction_date) transaction_date,
    a.account_number,
    b.account_name,
    CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END AS debit_amount,
    CASE WHEN a.amount < 0 THEN ABS(a.amount) ELSE 0 END AS credit_amount,
    CASE 
        WHEN strftime('%Y', DATE(a.transaction_date)) = '2024' AND b.account_number IS NOT NULL 
        THEN 1 
        ELSE 0 
    END AS is_valid_transaction
FROM journal_entries a
LEFT JOIN accounts b ON a.account_number = b.account_number
"""
amount_query = """
WITH transformed AS (
  SELECT
    transaction_id,
    CASE WHEN amount > 0 THEN amount ELSE 0 END AS debit_amount,
    CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END AS credit_amount
  FROM journal_entries
)
SELECT
  transaction_id,
  SUM(debit_amount) AS total_debits,
  SUM(credit_amount) AS total_credits
FROM transformed
GROUP BY transaction_id
HAVING SUM(debit_amount) != SUM(credit_amount)
LIMIT 10
"""
acount_summary = """
WITH
ValidationAcount AS (
    SELECT
        a.transaction_id,
        DATE(a.transaction_date) transaction_date,
        a.account_number,
        b.account_name,
        CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END AS debit_amount,
        CASE WHEN a.amount < 0 THEN ABS(a.amount) ELSE 0 END AS credit_amount,
        CASE 
            WHEN strftime('%Y', DATE(a.transaction_date)) = '2024' AND b.account_number IS NOT NULL 
            THEN 1 
            ELSE 0 
        END AS is_valid_transaction
    FROM journal_entries a
    LEFT JOIN accounts b ON a.account_number = b.account_number
),
ValidationTrx AS (
    SELECT
        transaction_id,
        SUM(debit_amount) AS total_debits,
        SUM(credit_amount) AS total_credits
    FROM ValidationAcount
    GROUP BY transaction_id
    HAVING SUM(debit_amount) = SUM(credit_amount)
)
SELECT
    account_name,
    SUM(debit_amount) - SUM(credit_amount) AS final_balance
FROM ValidationAcount
WHERE transaction_id IN (SELECT transaction_id FROM ValidationTrx) AND is_valid_transaction = 1
GROUP BY account_name
"""