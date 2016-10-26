-- database: presto; groups: tpcds
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM "tpcds"."sf1".item,
     "tpcds"."sf1".inventory,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".store_sales
WHERE i_current_price BETWEEN 62 AND 62+30
  AND inv_item_sk = i_item_sk
  AND d_date_sk=inv_date_sk
  AND cast(d_date as date) BETWEEN cast('2000-05-25' AS date) AND (cast('2000-05-25' AS date) + INTERVAL '60' DAY)
  AND i_manufact_id IN (129,
                        270,
                        821,
                        423)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND ss_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id LIMIT 100;
