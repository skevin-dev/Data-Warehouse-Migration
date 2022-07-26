{{ config(materialized='table') }}
SELECT * 
  FROM traffic_table_
LIMIT 10