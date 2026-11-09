
  
    

        create or replace transient table EXPRESS_DWH.GOLD.dim_date
         as
        (WITH date_spine AS (
    SELECT 
        DATEADD('day', (ROW_NUMBER() OVER (ORDER BY NULL) - 1), '2024-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    -1 AS date_key,
    '1900-01-01'::DATE AS full_date,
    1900 AS year, 1 AS quarter, 1 AS month, 'Unknown' AS month_name,
    1 AS week, 1 AS day_of_week, 1 AS day_of_month,
    FALSE AS is_weekend, FALSE AS is_peak_season
UNION ALL
SELECT
    TO_NUMBER(TO_VARCHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    TO_VARCHAR(full_date, 'MMMM') AS month_name,
    WEEKISO(full_date) AS week,
    DAYOFWEEKISO(full_date) AS day_of_week,
    DAY(full_date) AS day_of_month,
    IFF(DAYOFWEEKISO(full_date) IN (6, 7), TRUE, FALSE) AS is_weekend,
    IFF(MONTH(full_date) IN (1, 11, 12), TRUE, FALSE) AS is_peak_season
FROM date_spine
WHERE full_date <= GREATEST('2028-12-31'::DATE, DATEADD('year', 2, CURRENT_DATE()))
        );
      
  