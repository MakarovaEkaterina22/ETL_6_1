select * 
from tinkoff_pageview_counts tpc ;

select page_name ,SUM(distinct views_count ) as total_view 
from tinkoff_pageview_counts tpc 
group by page_name ;

