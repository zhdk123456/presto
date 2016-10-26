-- database: presto; groups: tpcds
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM "tpcds"."sf1".item,
     "tpcds"."sf1".inventory,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".catalog_sales
WHERE i_current_price BETWEEN 68 AND 68 + 30
  AND inv_item_sk = i_item_sk
  AND d_date_sk=inv_date_sk
  AND cast(d_date as date) BETWEEN cast('2000-02-01' AS date) AND (cast('2000-02-01' AS date) + INTERVAL '60' DAY)
  AND i_manufact_id IN (677,
                        940,
                        694,
                        808)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND cs_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id LIMIT 100;
