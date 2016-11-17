-- database: presto_tpcds; groups: tpcds, quarantine; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
--- returns incorrect resutls could be due some issues with CASE
select *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('2000-03-11' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('2000-03-11' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   FROM inventory,
        warehouse,
        item,
        date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('2000-03-11' as date) - INTERVAL '30' DAY)
                    and (cast ('2000-03-11' as date) + INTERVAL '30' DAY)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then cast(inv_after as decimal(7,2)) / inv_before
             else null
             end) between 2.00/3.00 and 3.00/2.00
 order by w_warehouse_name
         ,i_item_id
 LIMIT 100;
