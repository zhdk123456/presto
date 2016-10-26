-- database: presto; groups: tpcds
SELECT distinct(i_product_name)
FROM "tpcds"."sf1".item i1
WHERE i_manufact_id BETWEEN 738 AND 738+40
  AND
    (SELECT count(*) AS item_cnt
     FROM "tpcds"."sf1".item
     WHERE (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'powder' or i_color = 'khaki') and 
        (i_units = 'Ounce' or i_units = 'Oz') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'brown' or i_color = 'honeydew') and
        (i_units = 'Bunch' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'deep') and
        (i_units = 'N/A' or i_units = 'Dozen') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'cornflower') and
        (i_units = 'Box' or i_units = 'Pound') and
        (i_size = 'medium' or i_size = 'extra large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'midnight' or i_color = 'snow') and 
        (i_units = 'Pallet' or i_units = 'Gross') and
        (i_size = 'medium' or i_size = 'extra large')
        ) or
        (i_category = 'Women' and
        (i_color = 'cyan' or i_color = 'papaya') and
        (i_units = 'Cup' or i_units = 'Dram') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'orange' or i_color = 'frosted') and
        (i_units = 'Each' or i_units = 'Tbl') and
        (i_size = 'petite' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'forest' or i_color = 'ghost') and
        (i_units = 'Lb' or i_units = 'Bundle') and
        (i_size = 'medium' or i_size = 'extra large')
        )))) > 0
ORDER BY i_product_name LIMIT 100;
