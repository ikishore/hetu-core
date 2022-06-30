set session batch_enabled= true;
set session cm_parameter = 2;
set session scheduler_parameter = 4;
set session single_query_cost_threshold = 2;
set session task_concurrency = 16;
set session batch_size = 64;


select count(*) from memory.default.store_sales, memory.default.date_dim where  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item  where  ss_item_sk=i_item_sk  and i_manager_id >= 50;



select count(*) from memory.default.store_sales, memory.default.date_dim, memory.default.item where ss_item_sk=i_item_sk and  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item, memory.default.household_demographics  where ss_item_sk=i_item_sk and ss_hdemo_sk=hd_demo_sk and hd_dep_count >=5 and i_manager_id >=50;


select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.item, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and i_manager_id >=50 ;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and d_year = 2000;



select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.customer  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and ss_customer_sk=c_customer_sk and hd_dep_count >=6 and  d_year = 2000;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.item  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and ss_sold_date_sk=d_date_sk  and hd_dep_count >=6 and i_manager_id >=60 and d_year = 2000;



select count(*)  from  memory.default.catalog_sales, memory.default.customer, memory.default.customer_address, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk  and d_year = 2000;


select count(*)  from  memory.default.catalog_sales,  memory.default.customer,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and i_item_sk  = cs_item_sk and cs_bill_customer_sk = c_customer_sk    and d_year = 2002;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.household_demographics,  memory.default.date_dim where   i_item_sk = cs_item_sk and hd_demo_sk = cs_bill_hdemo_sk and d_date_sk = cs_sold_date_sk and w_warehouse_sk = cs_warehouse_sk  and  hd_dep_count >=7  and d_year = 2000;

select count(*)  from  memory.default.catalog_sales, memory.default.item , memory.default.date_dim where i_item_sk = cs_item_sk and d_date_sk = cs_sold_date_sk and d_year = 2001 and i_manager_id >= 75;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,   memory.default.household_demographics,  memory.default.date_dim where  d_date_sk = cs_sold_date_sk  and w_warehouse_sk = cs_warehouse_sk and  hd_demo_sk = cs_bill_hdemo_sk   and d_year = 2002 and hd_dep_count >=6  ;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.date_dim where  i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and d_year =1999 and i_manager_id >= 85;

select count(*)  from  memory.default.catalog_sales,  memory.default.customer_demographics,  memory.default.customer,  memory.default.customer_address,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and   i_item_sk = cs_item_sk  and cd_demo_sk = cs_bill_cdemo_sk  and  c_customer_sk = cs_bill_customer_sk   and   ca_address_sk =  c_current_addr_sk and d_year = 2000 and i_manager_id >= 80 ;

select count(*) from  memory.default.catalog_sales, memory.default.customer, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk  and cs_sold_date_sk = d_date_sk  and d_year = 2002;




--------end of batch 1-----------------------------------------------------------------------------------------------------------------


select count(*) from memory.default.store_sales, memory.default.date_dim where  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item  where  ss_item_sk=i_item_sk  and i_manager_id >= 50;



select count(*) from memory.default.store_sales, memory.default.date_dim, memory.default.item where ss_item_sk=i_item_sk and  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item, memory.default.household_demographics  where ss_item_sk=i_item_sk and ss_hdemo_sk=hd_demo_sk and hd_dep_count >=5 and i_manager_id >=50;


select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.item, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and i_manager_id >=50 ;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and d_year = 2000;



select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.customer  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and ss_customer_sk=c_customer_sk and hd_dep_count >=6 and  d_year = 2000;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.item  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and ss_sold_date_sk=d_date_sk  and hd_dep_count >=6 and i_manager_id >=60 and d_year = 2000;



select count(*)  from  memory.default.catalog_sales, memory.default.customer, memory.default.customer_address, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk  and d_year = 2000;


select count(*)  from  memory.default.catalog_sales,  memory.default.customer,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and i_item_sk  = cs_item_sk and cs_bill_customer_sk = c_customer_sk    and d_year = 2002;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.household_demographics,  memory.default.date_dim where   i_item_sk = cs_item_sk and hd_demo_sk = cs_bill_hdemo_sk and d_date_sk = cs_sold_date_sk and w_warehouse_sk = cs_warehouse_sk  and  hd_dep_count >=7  and d_year = 2000;

select count(*)  from  memory.default.catalog_sales, memory.default.item , memory.default.date_dim where i_item_sk = cs_item_sk and d_date_sk = cs_sold_date_sk and d_year = 2001 and i_manager_id >= 75;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,   memory.default.household_demographics,  memory.default.date_dim where  d_date_sk = cs_sold_date_sk  and w_warehouse_sk = cs_warehouse_sk and  hd_demo_sk = cs_bill_hdemo_sk   and d_year = 2002 and hd_dep_count >=6  ;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.date_dim where  i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and d_year =1999 and i_manager_id >= 85;

select count(*)  from  memory.default.catalog_sales,  memory.default.customer_demographics,  memory.default.customer,  memory.default.customer_address,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and   i_item_sk = cs_item_sk  and cd_demo_sk = cs_bill_cdemo_sk  and  c_customer_sk = cs_bill_customer_sk   and   ca_address_sk =  c_current_addr_sk and d_year = 2000 and i_manager_id >= 80 ;

select count(*) from  memory.default.catalog_sales, memory.default.customer, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk  and cs_sold_date_sk = d_date_sk  and d_year = 2002;




--------end of batch 32-----------------------------------------------------------------------------------------------------------------

select count(*) from memory.default.store_sales, memory.default.date_dim where  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item  where  ss_item_sk=i_item_sk  and i_manager_id >= 50;



select count(*) from memory.default.store_sales, memory.default.date_dim, memory.default.item where ss_item_sk=i_item_sk and  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item, memory.default.household_demographics  where ss_item_sk=i_item_sk and ss_hdemo_sk=hd_demo_sk and hd_dep_count >=5 and i_manager_id >=50;


select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.item, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and i_manager_id >=50 ;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and d_year = 2000;



select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.customer  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and ss_customer_sk=c_customer_sk and hd_dep_count >=6 and  d_year = 2000;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.item  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and ss_sold_date_sk=d_date_sk  and hd_dep_count >=6 and i_manager_id >=60 and d_year = 2000;



select count(*)  from  memory.default.catalog_sales, memory.default.customer, memory.default.customer_address, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk  and d_year = 2000;


select count(*)  from  memory.default.catalog_sales,  memory.default.customer,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and i_item_sk  = cs_item_sk and cs_bill_customer_sk = c_customer_sk    and d_year = 2002;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.household_demographics,  memory.default.date_dim where   i_item_sk = cs_item_sk and hd_demo_sk = cs_bill_hdemo_sk and d_date_sk = cs_sold_date_sk and w_warehouse_sk = cs_warehouse_sk  and  hd_dep_count >=7  and d_year = 2000;

select count(*)  from  memory.default.catalog_sales, memory.default.item , memory.default.date_dim where i_item_sk = cs_item_sk and d_date_sk = cs_sold_date_sk and d_year = 2001 and i_manager_id >= 75;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,   memory.default.household_demographics,  memory.default.date_dim where  d_date_sk = cs_sold_date_sk  and w_warehouse_sk = cs_warehouse_sk and  hd_demo_sk = cs_bill_hdemo_sk   and d_year = 2002 and hd_dep_count >=6  ;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.date_dim where  i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and d_year =1999 and i_manager_id >= 85;

select count(*)  from  memory.default.catalog_sales,  memory.default.customer_demographics,  memory.default.customer,  memory.default.customer_address,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and   i_item_sk = cs_item_sk  and cd_demo_sk = cs_bill_cdemo_sk  and  c_customer_sk = cs_bill_customer_sk   and   ca_address_sk =  c_current_addr_sk and d_year = 2000 and i_manager_id >= 80 ;

select count(*) from  memory.default.catalog_sales, memory.default.customer, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk  and cs_sold_date_sk = d_date_sk  and d_year = 2002;




--------end of batch 48-----------------------------------------------------------------------------------------------------------------

select count(*) from memory.default.store_sales, memory.default.date_dim where  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item  where  ss_item_sk=i_item_sk  and i_manager_id >= 50;



select count(*) from memory.default.store_sales, memory.default.date_dim, memory.default.item where ss_item_sk=i_item_sk and  ss_sold_date_sk=d_date_sk and d_year = 2000;


select count(*) from memory.default.store_sales, memory.default.item, memory.default.household_demographics  where ss_item_sk=i_item_sk and ss_hdemo_sk=hd_demo_sk and hd_dep_count >=5 and i_manager_id >=50;


select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.item, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and i_manager_id >=50 ;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and hd_dep_count >=6 and d_year = 2000;



select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.customer  where ss_hdemo_sk=hd_demo_sk and ss_sold_date_sk=d_date_sk  and ib_income_band_sk = hd_income_band_sk and ss_customer_sk=c_customer_sk and hd_dep_count >=6 and  d_year = 2000;

select  count(*)  from memory.default.store_sales, memory.default.household_demographics, memory.default.date_dim, memory.default.income_band, memory.default.item  where ss_hdemo_sk=hd_demo_sk and ss_item_sk=i_item_sk  and ib_income_band_sk = hd_income_band_sk and ss_sold_date_sk=d_date_sk  and hd_dep_count >=6 and i_manager_id >=60 and d_year = 2000;



select count(*)  from  memory.default.catalog_sales, memory.default.customer, memory.default.customer_address, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk  and d_year = 2000;


select count(*)  from  memory.default.catalog_sales,  memory.default.customer,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and i_item_sk  = cs_item_sk and cs_bill_customer_sk = c_customer_sk    and d_year = 2002;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.household_demographics,  memory.default.date_dim where   i_item_sk = cs_item_sk and hd_demo_sk = cs_bill_hdemo_sk and d_date_sk = cs_sold_date_sk and w_warehouse_sk = cs_warehouse_sk  and  hd_dep_count >=7  and d_year = 2000;

select count(*)  from  memory.default.catalog_sales, memory.default.item , memory.default.date_dim where i_item_sk = cs_item_sk and d_date_sk = cs_sold_date_sk and d_year = 2001 and i_manager_id >= 75;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,   memory.default.household_demographics,  memory.default.date_dim where  d_date_sk = cs_sold_date_sk  and w_warehouse_sk = cs_warehouse_sk and  hd_demo_sk = cs_bill_hdemo_sk   and d_year = 2002 and hd_dep_count >=6  ;

select count(*)  from  memory.default.catalog_sales,  memory.default.warehouse,  memory.default.item,  memory.default.date_dim where  i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and d_year =1999 and i_manager_id >= 85;

select count(*)  from  memory.default.catalog_sales,  memory.default.customer_demographics,  memory.default.customer,  memory.default.customer_address,  memory.default.date_dim,  memory.default.item where d_date_sk = cs_sold_date_sk and   i_item_sk = cs_item_sk  and cd_demo_sk = cs_bill_cdemo_sk  and  c_customer_sk = cs_bill_customer_sk   and   ca_address_sk =  c_current_addr_sk and d_year = 2000 and i_manager_id >= 80 ;

select count(*) from  memory.default.catalog_sales, memory.default.customer, memory.default.date_dim where cs_bill_customer_sk = c_customer_sk  and cs_sold_date_sk = d_date_sk  and d_year = 2002;



---------enf of 64--------------------------------------------------------------------------
