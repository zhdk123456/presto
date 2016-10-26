-- database: presto; groups: tpcds
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       avg(inv_quantity_on_hand) qoh
FROM "tpcds"."sf1".inventory,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".item,
     "tpcds"."sf1".warehouse
WHERE inv_date_sk=d_date_sk
  AND inv_item_sk=i_item_sk
  AND inv_warehouse_sk = w_warehouse_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY rollup(i_product_name,i_brand,i_class,i_category)
ORDER BY qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category LIMIT 100;
