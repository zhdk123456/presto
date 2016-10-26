-- database: presto; groups: tpcds,quarantine
--- fails with:  no viable alternative at input 'grouping'
SELECT i_item_id,
       s_state,
       grouping(s_state) g_state,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
FROM "tpcds"."sf1".store_sales,
     "tpcds"."sf1".customer_demographics,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".store,
     "tpcds"."sf1".item
WHERE ss_sold_date_sk = d_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk
  AND ss_cdemo_sk = cd_demo_sk
  AND cd_gender = 'M'
  AND cd_marital_status = 'M'
  AND cd_education_status = 'College'
  AND d_year = 2002 
  AND s_state IN ('TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN') -- wtf? see the spec B.27
GROUP BY ROLLUP (i_item_id,
                 s_state)
ORDER BY i_item_id,
         s_state LIMIT 100;
