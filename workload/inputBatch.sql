
set session batch_enabled=true; set session batch_size = 8;

select count(*) from  memory.default.catalog_sales, memory.default.customer, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk  and cs_sold_date_sk = d_date_sk ;

select count(*)  from  memory.default.catalog_sales, memory.default.customer, memory.default.customer_address, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk ;
select count(*)  from  memory.default.catalog_sales, memory.default.item , memory.default.date_dim where i_item_sk = cs_item_sk and d_date_sk = cs_sold_date_sk ;
select count(*)  from  memory.default.catalog_sales,  memory.default.customer_demographics,  memory.default.customer,  memory.default.customer_address,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and   i_item_sk = cs_item_sk  and cd_demo_sk = cs_bill_cdemo_sk  and  c_customer_sk = cs_bill_customer_sk   and   ca_address_sk =  c_current_addr_sk  ;



select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,   memory.default.customer_demographics,  memory.default.date_dim where  d_date_sk = cs_sold_date_sk  and w_warehouse_sk = cs_warehouse_sk and cd_demo_sk = cs_bill_cdemo_sk and d_year = 2002;
select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.date_dim where  i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk ;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.customer_demographics,  memory.default.household_demographics,  memory.default.date_dim where   i_item_sk = cs_item_sk and cd_demo_sk = cs_bill_cdemo_sk and hd_demo_sk = cs_bill_hdemo_sk and d_date_sk = cs_sold_date_sk and w_warehouse_sk = cs_warehouse_sk ;

select count(*)  from  memory.default.catalog_sales,  memory.default.customer,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and i_item_sk  = cs_item_sk and cs_bill_customer_sk = c_customer_sk    and d_year = 2002;

