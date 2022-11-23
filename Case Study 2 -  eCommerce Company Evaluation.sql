-- creates temporary table based on an inner join of products_category_name_translation
-- on products_dataset
CREATE TABLE complete_dataset AS (
	SELECT 
	items.order_id,
	items.product_id,
	new_products.product_category_name_english,
	new_orders.order_approved_at,
	items.price,
	items.freight_value,
	payments.payment_type,
	payments.payment_value,
	total_payment.total_payment,
	new_orders.customer_id,
	new_orders.customer_zip_code_prefix,
	new_orders.customer_city,
	new_orders.customer_state,
	reviews.review_score
	FROM public.order_items_dataset AS items
	
	INNER JOIN (
		-- inner joining customer_dataset onto orders_dataset based on customer_id
		SELECT 
		orders.order_id,
		orders.customer_id,
		orders.order_approved_at,
		customer.customer_zip_code_prefix,
		customer.customer_city,
		customer.customer_state
		FROM public.orders_dataset AS orders
		INNER JOIN public.customer_dataset AS customer ON 
		orders.customer_id = customer.customer_id
		-- creating a filter because the "company" was created on 2017 and it is currently 2018-09
		WHERE orders.order_approved_at >= '2017-01-01 00:00:00' 
		AND  orders.order_approved_at <  '2018-10-01 00:00:00') AS new_orders ON 
	items.order_id = new_orders.order_id
	
	INNER JOIN (
		-- inner join product_category_name_translated onto products_dataset with
		-- translated product names
		SELECT 
		products.product_id,
		products_english.product_category_name_english
		FROM public.products_dataset AS products
		INNER JOIN public.product_category_name_translation AS products_english ON 
		products.product_category_name = products_english.product_category_name) AS new_products ON 
	items.product_id = new_products.product_id
	
	-- inner join order_payments onto order_items_dataset to include all payment related information
	INNER JOIN public.order_payments_dataset AS payments ON 
	items.order_id = payments.order_id
	
	-- inner join aggregated payments onto order_items_dataset 
	-- this was necessary as there were some orders were involved multiple products
	INNER JOIN (
		SELECT 
		order_id,
		ROUND(SUM(payment_value), 2) AS total_payment
		FROM public.order_payments_dataset
		GROUP BY
		  order_id) AS total_payment ON 
	items.order_id = total_payment.order_id
	
	-- inner join order_reviews_dataset onto order_items_dataset to include all review information
	INNER JOIN public.order_reviews_dataset AS reviews ON 
	items.order_id = reviews.order_id	
)