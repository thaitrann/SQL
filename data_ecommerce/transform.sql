select
	product_list.sku,
	product_list.category,
	product_list.subcategory,
	product_list.launching_date,
	cast(product_list.first_po_di_quantity as float) as first_po_di_quantity,
	[log_date],
	[asin],
	transactions.[country] as transaction_country,
	[channel],
	[label_currency],
	[glance_view],
	[ordered_units],
	[orders],
	[ordered_gmv],
	[ordered_revenue],
	[shipped_units] as real_sales,
	[shipped_gmv],
	[sb_impressions],
	[sb_clicks],
	[sb_ordered_units],
	[sb_orders],
	[sb_ordered_gmv],
	[sb_spend],
	[sd_impressions],
	[sd_clicks],
	[sd_ordered_units],
	[sd_orders],
	[sd_ordered_gmv],
	[sd_spend],
	[sp_impressions],
	[sp_clicks],
	[sp_ordered_units],
	[sp_orders],
	[sp_ordered_gmv],
	[sp_spend],
	[sbv_impressions],
	[sbv_clicks],
	[sbv_ordered_units],
	[sbv_orders],
	[sbv_ordered_gmv],
	[sbv_spend],
	[dsp_halo_impressions],
	[dsp_halo_clicks],
	[dsp_halo_ordered_units],
	[dsp_halo_orders],
	[dsp_halo_ordered_gmv],
	[dsp_halo_spend],
	[dsp_promoted_impressions],
	[dsp_promoted_clicks],
	[dsp_promoted_ordered_units],
	[dsp_promoted_orders],
	[dsp_promoted_ordered_gmv],
	[dsp_promoted_spend],
	[coupon_spend],
	[vc_promo_ordered_units],
	[vc_promo_ordered_gmv],
	[vc_promo_spend],
	[vm_promo_ordered_units],
	[vm_promo_ordered_gmv],
	[vm_promo_spend],
	[shipped_gross_profit],
	[ordered_gross_profit],
	inventory.country as inventory_country,
	inventory.inventory_UTD,
	inventory.total_incoming,
	coupon_spend + vc_promo_spend + vm_promo_spend as promotion,
	sbv_spend + sp_spend + sd_spend + sb_spend + dsp_halo_spend + dsp_promoted_spend as ads
-- into product_data
from product_list
left join transactions
	on product_list.sku = transactions.sku
left join inventory
	on product_list.sku = inventory.sku

--promotion = counpon + vc_promo + vm_promo
 -- vc: 
 -- vm: 
--RS = real sale = shipped_unit

--ads = sbv + sp + sd + sb + dsp (spend)
 -- sbv: Sponsored Brands video
 -- sp: sponsor product
 -- sd: sponsor display
 -- sb: sponsor brand
 -- dsp: demand-side platform
 -- Impressions are the number of times your ads appear on Amazon be it on the product listing page or search results page
-- frozen = now + 3 months
-- gmv: doanh thu = giá bán * RS	
-- ordered date = khách đặt
-- shipped date = khách nhận
-- Glance view shows how many times a product detail page has been viewed. conversion rate = total orders/customer glance view. tính trên pbi
-- Click Through Rate (CTR) = TOTAL CLICKS / TOTAL IMPRESSIONS
-- advertising cost of sales (ACOS)
-- Return on ad spend (ROAS)
-- ppc: Pay Per Click

-- create table exchange_rate to get CAD/USD from shipped_gmv
with cte2 as
(
	select
		log_date,
		label_currency,
		SUM(shipped_gmv) as total_shipped_gmv,
		LEAD(SUM(shipped_gmv),1) over(partition by log_date order by log_date, label_currency) as total_shipped_gmv_2
	from product_data
	group by log_date, label_currency
	having log_date is not null
)
select 
	log_date,
	total_shipped_gmv,
	total_shipped_gmv_2,
	total_shipped_gmv/total_shipped_gmv_2 as 'cad/usd'
into exchange_rate
from cte2
where total_shipped_gmv_2 > 0

-- get date have null cad/usd
with cte3 as
(
	select 
		log_date
	from product_data
	group by log_date
	having log_date is not null
)
select 
	cte3.log_date,
	exchange_rate.[cad/usd]
into exchange_rate_updated
from cte3
left join exchange_rate
on cte3.log_date = exchange_rate.log_date
order by cte3.log_date

-- update cad/usd null value with avg cad/usd
declare @avg_cad_usd as float
select @avg_cad_usd = AVG([cad/usd])
from exchange_rate
print @avg_cad_usd;
UPDATE exchange_rate_updated 
SET [cad/usd] = @avg_cad_usd 
WHERE [cad/usd] is null;

-- convert price in cad label to usd: usd = cad/exchange_rate
UPDATE product_data
SET exchange_rate = exchange_rate_updated.[cad/usd]
FROM product_data
left JOIN exchange_rate_updated
ON product_data.log_date = exchange_rate_updated.log_date

DECLARE @column_name nvarchar(200), @sql nvarchar(max)
DECLARE cursorProduct CURSOR FOR
with cte1 as 
(
	select 
		*
	from INFORMATION_SCHEMA.COLUMNS 
	where TABLE_NAME = 'product_data' 
)
select 
	COLUMN_NAME
from cte1
where COLUMN_NAME like '%gmv'
or COLUMN_NAME like '%spend'
or COLUMN_NAME like '%profit'
or COLUMN_NAME like '%revenue'   

OPEN cursorProduct

FETCH NEXT FROM cursorProduct
      INTO @column_name

WHILE @@FETCH_STATUS = 0
BEGIN
	--set @sql = 'select ' + @column_name + ' from product_data where label_currency = ''CAD'''
	set @sql = 'UPDATE product_data SET ' + @column_name + '=' + @column_name +'/ exchange_rate FROM product_data where label_currency = ''CAD'''
	EXEC sp_sqlexec @sql
    FETCH NEXT FROM cursorProduct INTO @column_name
END
CLOSE cursorProduct   
DEALLOCATE cursorProduct

