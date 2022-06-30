
select * from (VALUES (' '));


CREATE TABLE memory.default.store AS
SELECT * from tpcds.tiny.store;

CREATE TABLE memory.default.store_sales AS
SELECT * from tpcds.tiny.store_sales;

CREATE TABLE memory.default.catalog_sales AS
SELECT * from tpcds.tiny.catalog_sales;

CREATE TABLE memory.default.warehouse AS
SELECT * from tpcds.tiny.warehouse;

CREATE TABLE memory.default.promotion AS
SELECT * from tpcds.tiny.promotion;

CREATE TABLE memory.default.ship_mode AS
SELECT * from tpcds.tiny.ship_mode;

CREATE TABLE memory.default.call_center AS
SELECT * from tpcds.tiny.call_center;


CREATE TABLE memory.default.inventory AS
SELECT * from tpcds.tiny.inventory;


CREATE TABLE memory.default.item AS
SELECT * from tpcds.tiny.item;


CREATE TABLE memory.default.date_dim AS
SELECT * from tpcds.tiny.date_dim;

CREATE TABLE memory.default.customer AS
SELECT * from tpcds.tiny.customer;

CREATE TABLE memory.default.household_demographics AS
SELECT * from tpcds.tiny.household_demographics;

CREATE TABLE memory.default.customer_demographics AS
SELECT * from tpcds.tiny.customer_demographics;


CREATE TABLE memory.default.income_band AS
SELECT * from tpcds.tiny.income_band;


CREATE TABLE memory.default.time_dim AS
SELECT * from tpcds.tiny.time_dim;


CREATE TABLE memory.default.customer_address AS
SELECT * from tpcds.tiny.customer_address;


