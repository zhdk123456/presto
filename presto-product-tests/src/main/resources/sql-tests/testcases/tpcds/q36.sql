-- database: presto_tpcds; groups: tpcds, quarantine; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
--- fails with:  no viable alternative at input 'grouping'

SELECT sum(ss_net_profit)/sum(ss_ext_sales_price) AS gross_margin,
       i_category,
       i_class,
       grouping(i_category)+grouping(i_class) AS lochierarchy,
       rank() over (partition BY grouping(i_category)+grouping(i_class), CASE WHEN grouping(i_class) = 0 THEN i_category END
                    ORDER BY sum(ss_net_profit)/sum(ss_ext_sales_price) ASC) AS rank_within_parent
FROM store_sales,
     date_dim d1,
     item,
     store
WHERE d1.d_year = 2001
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND s_state IN ('TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN',
                  'TN') -- see the spec B.36
GROUP BY rollup(i_category,i_class)
ORDER BY lochierarchy DESC,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END,
         rank_within_parent LIMIT 100;
