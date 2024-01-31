-- Data analysis allows for getting a glimpse of whats within the rows and columns

-- With dataset on sales, inventory, products, stores attention will shift to getting insight from the data to help with
-- making informative decisions for the business

-- Follow through
	-- creating the data base to house the seperate datasets
	-- create a table for each dataset
	-- Querying each table
	-- Cleaning data necessary
	-- Joining related tables
	-- Using case stateent for classfication if possible
	
-- Create database (sales_data)

create database sales_data -- this line of code will create the database to house the datasets

-- Create the tables that will hold each specific data set 
-- There are 4 data tables
	-- Stores  - has store name, city and location
	-- Inventory - has stock at hand
	-- Products - has product name, category, cost, price
	-- Sales - has transaction date, units sold

-- Create Stores table - the table will features such as store_id, store_name, store_city, store_location, opened_date

drop table stores
create table stores(
	store_id smallint,
	store_name text,
	store_city text,
	store_location text,
	opened_date text
)

-- Upload date into the stores table

copy stores(store_id, store_name, store_city, store_location, opened_date)
from 'C:\Users\KEVIN\Desktop\Learning_Vision\Structured Query Language\Toy_sales\stores.csv'
with(format csv, header)

select * from stores
limit 5
-- open data was imported as text due to incompatibility issues, we shall work with it as it is

-- Quick scan of the store data

select 
	count(distinct store_name) as stores
from stores -- we have 50 unique outlets across

select
	count( distinct store_city) as city
from stores -- The stores are spread across 29 cities

select 
	count(distinct store_location) as location
from stores -- Stores are spread across 4 locations


-- Store distribution across the city
select 
	store_city as city,
	count (distinct store_name) as stores
from stores
group by city
order by stores desc
	-- the stores range from 1 to 4 in the 29 cities

-- Store distribution across the locations
select
	store_location as location,
	count(distinct store_name) as count_of_stores
from stores
group by location
order by count_of_stores desc
	-- In the 4 locations the distribution is as below
		-- Downtown - 29, with more store its ideal to suggest its a busy location but the same will confirmed with sales that come through
		-- Commercial -- 12
		-- Residential -- 6
		-- Airport -- 3 -- Less stores in this region suggesting less activity
		
-- Year aspect(Which year had most outlets opened)

select
	right(opened_date, 4) as operational_date,
	count(distinct store_name)
from stores
group by operational_date
order by operational_date
	-- there has been a gradual growth for the business that started back in 1992 with one store, as of 2016 there were in 50 stores
	-- The business has been making an additional of listing again based on various facts, one being the availability of sales to back the listing
	-- Their best years of listing were 2014, 2013 where they they listed in 7 and 5 outlets respectively
	-- Between 2003 and 2016 with the exception of 2014 and 2013 the business has been listing in 2 or 3 outlets

-- Create table for invetory -- has store _id, product_id and stock_at_hand

create table inventory(
	stock_id smallint, -- best way to capture id that dont go above 2btyes
	product_id smallint,
	stock_at_hand int
)

-- Upload date into the invetory table
copy inventory
from 'C:\Users\KEVIN\Desktop\Learning_Vision\Structured Query Language\Toy_sales\inventory.csv'
with(format csv, header)

select * from inventory
limit 5 -- high level view of the inventory table


select
	count(distinct product_id) as products_count
from inventory
		-- The business has 35 unique products

-- Checking which stock have more stock hold
select
	product_id,
	sum(stock_at_hand) as stock
from inventory
group by product_id
order by stock desc
	-- Products id (8, 10, 25) have a stock hold of over 2000 which can be translated either way as being top seller or poor seller
	-- This will be investigated further from the sales table
	
-- Create product table - the table has product_id, product_name, product_category, product_cost, product_price
	-- the cost and price have dollar sign in them, which we will at this time import as text and alter the table to make the 2 columns numeric data type
	
drop table products

create table products(
	product_id smallint,
	product_name text,
	product_category text,
	product_cost text,
	product_price text
)
-- Upload the products table
copy products
from 'C:\Users\KEVIN\Desktop\Learning_Vision\Structured Query Language\Toy_sales\products.csv'
with(format csv, header)

select * from products
limit 5

-- creating a back up table - this will be a fall back plan should the intended activity dont work as expected


create table products_back_up as 
select * from products

-- creating columns of interest

alter table products add column copy_cost decimal
alter table products add column copy_price decimal

-- updating the columns with numeric values

update products
set copy_cost = right(product_cost, length(product_cost)-1):: decimal

update products
set copy_price = right(product_price, length(product_price)-1):: decimal


alter table products drop column product_cost -- drop the product_cost column that has the text values	
alter table products drop column product_price -- drop the product_cost column that has the text values


select * from products 
limit 5

-- Expensive products to produce and their sale price

select 
	product_name,
	round(avg (copy_cost),2) as average_price
from products
group by product_name
order by average_price desc

	-- Lego brick average cost price is 34.99 making them the most expensive product
	-- Barrel O' Slime average cost price is 1.99 making them the least expensive


-- Most expensive products
select 
	product_name,
	copy_price as pricing
from products
order by pricing desc

	-- Lego brick are the most expensive retailing at 34.99
	-- "PlayDoh Can" are the least expensive retailing at 2.99


-- Create sales table

drop table sales

create table sales(
	sales_id bigint, -- the values are about 1M
	order_date text,
	store_id smallint,
	product_id smallint,
	units int
)

--Upload sales to the table
copy sales
from 'C:\Users\KEVIN\Desktop\Learning_Vision\Structured Query Language\Toy_sales\sales.csv'
with(format csv, header)

select * from sales
limit 5


-- All the tables have been load to the database, we move on to joing relevant tables to allow for analysis
	-- Inventory
	-- Sales
	-- Products
	-- Stores

-- join products to sales they both have related column names(product_id)
-- Also join inventory and Stores both have related columns(product id)
-- Create a separate table that joins the 4 tables together
-- To improve on performance we create index - currently postgres is taking long to load the table, this has been caused by the additional features that hasmade the bale longer


create table sales_products as
select 
	s.sales_id,
	s.order_date,
	s.product_id,
	pr.product_name,
	pr.product_category,
	st.store_name,
	st.store_city,
	st.store_location,
	pr.copy_cost,
	pr.copy_price,
	s.units
from sales s
left join
products pr
using(product_id)
left join stores st
on s.store_id = st.store_id

create index sales_idx on sales_products (sales_id) -- improves the database performance

-- Adding columns for total cost and total revenue then substitute the two new columns to generate profit

alter table sales_products add column total_cost decimal;

alter table sales_products add column total_revenue decimal;

alter table sales_products add column profit decimal;


update sales_products
set total_cost = sales_products.copy_cost * sales_products.units

update sales_products
set total_revenue = sales_products.copy_price * sales_products.units

update sales_products
set profit = sales_products.total_revenue - sales_products.total_cost


select *
from sales_products
order by profit desc
limit 5

-- The data has some interesting Categorical and qualitative variables that we shall use to develop a report to even understand the business
	 -- Total revenue generated
	 -- Total cost of products
	 -- Total profit generated
	 -- Total units sold
	 -- Most profitable products
	 -- Most profitable regions

-- The values are in dollars
-- Totals revenue generated

select
 sum(total_revenue) as total_revenue
from sales_products
	-- Total sales generated for the period of time is 14,444,572.35(2022-2023)
	
-- Total cost of products
select 
	sum(total_cost) as total_cost
from sales_products
	-- Total cost for the period of time 10,430,543.35(2022-2023)
	
-- Total profit generated
select
	sum(profit) as profit
from sales_products
	-- Total profit 4,014,029
	
-- Total units sold

select
	sum(units) as units_sold
from sales_products
	-- Total units sold for the period 1,090,565(2022-2023)
	

-- Most profitable products
select
	product_name,
	sum(units) as units_sold,
	sum(profit) as profit
from sales_products
group by product_name
order by profit desc
	-- Color buds are the most profitable products with profit of 834,944 having sold 104,368 units
	-- "Classic Dominoes made profit but the values are low less, they brought profit of 8942 selling 4471 units
	


-- Most profitable categories
select
	product_category,
	sum(units) as units_sold,
	sum(profit) as profit
from sales_products
group by product_category
order by profit desc
	-- Toys are the most profitable category with a profit of 1,097,527 selling 267,200 units

--Filtering the toys category to check on the profitable products
select
	product_name,
	sum(units) as units_sold,
	sum(profit) as profit
from sales_products
where product_category = 'Toys'
group by product_name
order by profit desc
	-- Action figure brought in the most profit of 347,748 selling 57,958 units
	
--Most profitable locations

select
	store_location,
	count(distinct store_name) as stores,
	sum(units) as units_sold,
	sum(profit) as profit
from sales_products
group by store_location
order by profit desc
	-- Downtown has the most profit 2,248,728 but the same can be backed by the number of stores in this area 29 in total


-- Filtering data based on several conditions

select
	store_name
	store_location,
	store_city,
	sum(units) as units_sold,
	sum(profit) as profit
from sales_products
where product_category <>'Toys' and store_location <>'Downtown'
group by store_name, store_location, store_city
order by profit
	-- Checking on other categories that are not 'Toys' and outside 'Downtown' Toluca is the best outlet with a profit of 40,518 selling 11927 units
