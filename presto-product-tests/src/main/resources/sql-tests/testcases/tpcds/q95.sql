-- database: presto; groups: tpcds
WITH ws_wh AS
  (SELECT ws1.ws_order_number,
          ws1.ws_warehouse_sk wh1,
          ws2.ws_warehouse_sk wh2
   FROM "tpcds"."sf1".web_sales ws1,
        "tpcds"."sf1".web_sales ws2
   WHERE ws1.ws_order_number = ws2.ws_order_number
     AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)

SELECT count(DISTINCT ws_order_number) AS "order count",
       sum(ws_ext_ship_cost) AS "total shipping cost",
       sum(ws_net_profit) AS "total net profit"
FROM "tpcds"."sf1".web_sales ws1,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".customer_address,
     "tpcds"."sf1".web_site
WHERE cast(d_date as date) BETWEEN cast('1999-2-01' as date) AND (cast('1999-2-01' AS date) + INTERVAL '60' DAY)
  AND ws1.ws_ship_date_sk = d_date_sk
  AND ws1.ws_ship_addr_sk = ca_address_sk
  AND ca_state = 'IL'
  AND ws1.ws_web_site_sk = web_site_sk
  AND web_company_name = 'pri'
  AND ws1.ws_order_number IN
    (SELECT ws_order_number
     FROM ws_wh)
  AND ws1.ws_order_number IN
    (SELECT wr_order_number
     FROM "tpcds"."sf1".web_returns,
          ws_wh
     WHERE wr_order_number = ws_wh.ws_order_number)
ORDER BY count(DISTINCT ws_order_number) LIMIT 100;
