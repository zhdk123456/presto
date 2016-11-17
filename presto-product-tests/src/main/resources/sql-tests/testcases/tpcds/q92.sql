-- database: presto_tpcds; groups: tpcds; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
SELECT sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales,
     item,
     date_dim
WHERE i_manufact_id = 350 
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN cast('2000-01-27' as date) AND (cast('2000-01-27' AS date) + INTERVAL '90' DAY)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt >
    (SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM web_sales,
          date_dim
     WHERE ws_item_sk = i_item_sk
       AND d_date BETWEEN cast('2000-01-27' as date) AND (cast('2000-01-27' AS date) + INTERVAL '90' DAY)
       AND d_date_sk = ws_sold_date_sk)
ORDER BY sum(ws_ext_discount_amt) LIMIT 100;
