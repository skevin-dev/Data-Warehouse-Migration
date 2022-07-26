{{ config(materialized='table') }}

SELECT *
 FROM  traffic_table_
where type = ' Car'

