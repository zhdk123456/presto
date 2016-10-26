-- database: presto; groups: tpcds
SELECT count(DISTINCT cs_order_number) AS "order count",
       sum(cs_ext_ship_cost) AS "total shipping cost",
       sum(cs_net_profit) AS "total net profit"
FROM "tpcds"."sf1".catalog_sales cs1,
     "tpcds"."sf1".date_dim,
     "tpcds"."sf1".customer_address,
     "tpcds"."sf1".call_center
WHERE d_date BETWEEN cast('2002-2-01' AS date) AND (cast('2002-2-01' AS date) + INTERVAL '60' DAY)
  AND cs1.cs_ship_date_sk = d_date_sk
  AND cs1.cs_ship_addr_sk = ca_address_sk
  AND ca_state = 'GA'
  AND cs1.cs_call_center_sk = cc_call_center_sk
  AND cc_county IN ('Williamson County',
                    'Williamson County',
                    'Williamson County',
                    'Williamson County',
                    'Williamson County') -- wtf? see the spec B.16
  AND EXISTS
    (SELECT *
     FROM "tpcds"."sf1".catalog_sales cs2
     WHERE cs1.cs_order_number = cs2.cs_order_number
       AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  AND NOT EXISTS
    (SELECT *
     FROM "tpcds"."sf1".catalog_returns cr1
     WHERE cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number) LIMIT 100;
