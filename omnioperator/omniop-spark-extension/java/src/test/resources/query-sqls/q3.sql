select *
from
 (select
    i_manufact_id,
    sum(ss_sales_price) sum_sales,
    avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
   from
     item,
     store_sales,
     date_dim,
     store
   where
     ss_item_sk = i_item_sk
      and ss_sold_date_sk = d_date_sk
      and ss_store_sk = s_store_sk
      and d_month_seq IN (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1215 + 5, 1215 + 6, 1215 + 7, 1215 + 8, 1215 + 9, 1215 + 10, 1215 + 11)
      and ((i_category IN ('Books', 'Children', 'Electronics')
            and i_class IN ('personal', 'portable', 'reference', 'self-help')
            and i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
         or (i_category IN ('Women', 'Music', 'Men')
            and (i_class IN ('accessories', 'classical', 'fragrances', 'pants'))
            and i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
   group by
    i_manufact_id,
    d_qoy
 )  tmp1
where
    case when (avg_quarterly_sales > 0) then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales else null end > 0.1
order by
 avg_quarterly_sales,
 sum_sales,
 i_manufact_id
limit 100;