-- database: presto_tpcds; groups: tpcds,quarantine; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
--- returns incorrect results
SELECT i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
FROM store_sales,
     item,
     date_dim
WHERE ss_item_sk = i_item_sk
  AND i_category IN ('Sports', 'Books', 'Home')
  AND ss_sold_date_sk = d_date_sk
  AND cast(d_date as date) BETWEEN cast('1999-02-22' AS date) AND (cast('1999-02-22' AS date) + INTERVAL '30' DAY)
GROUP BY 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
ORDER BY 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;
