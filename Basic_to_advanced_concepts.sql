--creating database
 CREATE DATABASE NorthwindTraders

--categories table
 CREATE TABLE categories(
	categoryID integer PRIMARY KEY, --contains uniquely identified values
	catgoryName varchar(250) UNIQUE NOT NULL, --contains unique and non null values
	description text UNIQUE NOT NULL --contains unique and non null values
 ) 

 DROP TABLE categories

--customers table
 CREATE TABLE customers(
	customerID varchar(250) PRIMARY KEY, --contains uniquely identified values
	companyName varchar(250) UNIQUE NOT NULL, --contains unique and non null values
	contactName varchar(250) UNIQUE NOT NULL, --contains unique and non null values
	contactTitle varchar(250) NOT NULL, --contains non null values
	city varchar(250) NOT NULL, --contains non null values
	country varchar(250) NOT NULL --contains non null values
 ) 

 DROP TABLE customers

--employees table
 CREATE TABLE employees(
	employeeID integer PRIMARY KEY, --contains uniquely identified values
	employeeName varchar(250) UNIQUE NOT NULL, --contains unique and non null values
	title varchar (250) NOT NULL, --contains unique and non null values
	city varchar(250) NOT NULL, --contains non null values
	country varchar(250) NOT NULL, --contains non null values
	reportsTo integer
 ) 


DROP TABLE employees

--shippers table
 CREATE TABLE shippers(
	shipperID integer PRIMARY KEY, --contains uniquely identified values
	companyName varchar(250) UNIQUE NOT NULL --contains unique and non null values
 )

 DROP TABLE shippers

--products table
CREATE TABLE products(
	productID integer PRIMARY KEY, --contains uniquely identified values
	productName varchar(250) UNIQUE NOT NULL, --contains unique and non null values
	qantityPerUnit varchar(250) NOT NULL, --contains non null values
	unitPrice numeric NOT NULL, --contains non null values
	discontinued numeric NOT NULL,--contains non null values
	categoryID integer,
	FOREIGN KEY (categoryID) REFERENCES categories(categoryID)
 )

 DROP TABLE products

--orders table
CREATE TABLE orders(
	orderID integer PRIMARY KEY, --contains uniquely identified values
	customerID varchar(250) NOT NULL, --contains non null values
	employeeID integer NOT NULL, --contains non null values
	orderDate Date NOT NULL, --contains non null values
	requiredDate Date NOT NULL, --contains non null values
	shippedDate Date,
	shipperID integer NOT NULL, --contains non null values
	freight numeric NOT NULL, --contains non null values
	FOREIGN KEY (customerID) REFERENCES customers(customerID), --Primary key of customers table referenced here
	FOREIGN KEY (employeeID) REFERENCES employees(employeeID), --Primary key of employees table referenced here
	FOREIGN KEY (shipperID) REFERENCES shippers(shipperID) --Primary key of shippers table referenced here
)

DROP TABLE orders

--order_details table
CREATE TABLE order_details(
	orderID integer NOT NULL, --contains non null values
	productID integer NOT NULL, --contains non null values
	unitPrice numeric NOT NULL, --contains non null values
	quantity integer NOT NULL, --contains non null values
	discount numeric NOT NULL, --contains non null values
	PRIMARY KEY (orderID, productID), --combined values are uniquely identified
	FOREIGN KEY (orderID) REFERENCES orders(orderID), --Primary key of orders table referenced here
	FOREIGN KEY (productID) REFERENCES products(productID) --Primary key of products table referenced here
)

DROP TABLE order_details
--************************************************************************************************************

/* 1)      Alter Table:
 Add a new column linkedin_profile to employees table to store LinkedIn URLs as varchar.
Change the linkedin_profile column data type from VARCHAR to TEXT.
 Add unique, not null constraint to linkedin_profile
Drop column linkedin_profile
*/

ALTER TABLE employees
ADD COLUMN linkedin_profile VARCHAR

ALTER TABLE employees
ALTER COLUMN linkedin_profile
SET DATA TYPE TEXT

ALTER TABLE employees
ADD UNIQUE(linkedin_profile)

ALTER TABLE employees
ALTER COLUMN linkedin_profile SET NOT NULL

ALTER TABLE employees
DROP COLUMN linkedin_profile

/* 2)      Querying (Select)
 Retrieve the first name, last name, and title of all employees
 Find all unique unit prices of products
 List all customers sorted by company name in ascending order
 Display product name and unit price, but rename the unit_price column as price_in_usd
*/

SELECT split_part(employeename,' ',1) AS "first_name",
split_part(employeename,' ',2) AS "last_name"
FROM employees

SELECT DISTINCT unitprice FROM products

SELECT contactname, companyname FROM customers
ORDER BY companyname ASC

SELECT productname, unitprice AS price_in_usd
FROM products

/* 3)      Filtering
Get all customers from Germany.
Find all customers from France or Spain
Retrieve all orders placed in 2014(based on order_date), and either have freight greater than 50 or the shipped date available (i.e., non-NULL)  (Hint: EXTRACT(YEAR FROM order_date))
*/

SELECT contactname, country FROM customers
WHERE country='Germany'

SELECT contactname, country FROM customers
WHERE country='France' OR country='Spain'

SELECT orderid FROM orders 
WHERE (EXTRACT(YEAR FROM orderdate)=2014) AND (freight>50 OR shippeddate IS NOT NULL)



/* 4)  Filtering
 Retrieve the product_id, product_name, and unit_price of products where the unit_price is greater than 15.
List all employees who are located in the USA and have the title "Sales Representative".
Retrieve all products that are not discontinued and priced greater than 30.
*/

SELECT productid, productname, unitprice FROM products
WHERE unitprice > 15

SELECT employeename FROM employees 
WHERE country = 'USA' AND title='Sales Representative'

SELECT productname FROM products
WHERE discontinued = 0 AND unitprice>30

/* 5)  LIMIT/FETCH
 Retrieve the first 10 orders from the orders table.
 Retrieve orders starting from the 11th order, fetching 10 rows (i.e., fetch rows 11-20).
*/

SELECT * FROM orders
LIMIT 10

SELECT * FROM orders
OFFSET 10
FETCH NEXT 10 ROWS ONLY

/* 6)      Filtering (IN, BETWEEN)
List all customers who are either Sales Representative, Owner
Retrieve orders placed between January 1, 2013, and December 31, 2013.
*/

SELECT contactname, contacttitle FROM customers
WHERE contacttitle IN ('Sales Representative','Owner')

SELECT orderid, orderdate FROM orders
WHERE orderdate BETWEEN '01/01/2013' AND '12/31/2013'

/*

7)      Filtering
List all products whose category_id is not 1, 2, or 3.
Find customers whose company name starts with "A".
*/

SELECT productname, categoryid FROM products
WHERE categoryid NOT IN ('1','2','3')

SELECT contactname, companyname FROM customers
WHERE companyname LIKE 'A%'

/*
8)    INSERT into orders table:
 Task: Add a new order to the orders table with the following details:
Order ID: 11078
Customer ID: ALFKI
Employee ID: 5
Order Date: 2025-04-23
Required Date: 2025-04-30
Shipped Date: 2025-04-25
shipperID:2
Freight: 45.50
*/

INSERT INTO orders
VALUES (11078, 'ALFKI', 5, '04/23/2025', '04/30/2025','04/25/2025', 2, 45.50)

SELECT * FROM orders
WHERE orderid=11078

/*
9) Increase(Update)  the unit price of all products in category_id =2 by 10%.
(HINT: unit_price =unit_price * 1.10)
*/

UPDATE products
SET unitprice = unitprice*1.10
WHERE categoryid=2

SELECT productname, unitprice FROM products
WHERE categoryid=2

--***********************************************************************************************
/*
1. Update the categoryName From “Beverages” to "Drinks" in the categories table.
*/

SELECT * FROM categories

UPDATE categories
SET categoryname = 'Drinks'
WHERE categoryname = 'Beverages'

--SELECT * FROM employees
--SELECT * FROM customers
--SELECT * FROM products
--SELECT * FROM shippers
--SELECT * FROM orders
--SELECT * FROM order_details

/*
2. Insert into shipper new record (give any values) Delete that new record from shippers table.
*/

INSERT INTO shippers
VALUES (4, 'Local Shipping')

SELECT * FROM shippers

DELETE FROM shippers
WHERE shipperid = 4
RETURNING *

/*
3. Update categoryID=1 to categoryID=1001. Make sure related products update their categoryID too. Display the both category and products table to show the cascade.
 Delete the categoryID= “3”  from categories. Verify that the corresponding records are deleted automatically from products.
*/

ALTER TABLE products
DROP CONSTRAINT IF EXISTS products_categoryid_fkey

ALTER TABLE products
ADD CONSTRAINT products_categoryid_fkey
FOREIGN KEY (categoryID) REFERENCES categories(categoryid)
ON UPDATE CASCADE
ON DELETE CASCADE

UPDATE categories
SET categoryid = 1001
WHERE categoryid = 1

SELECT * FROM categories 
WHERE categoryid = 1001

SELECT * FROM products 
WHERE categoryid = 1001

DELETE FROM categories
WHERE categoryid = 3
RETURNING *

SELECT * FROM products
WHERE categoryid=3

-- ALtering order_details as it throws error if we delete one categoryid from categories which in turn deletes 
--corresponding rows in products with same categoyid, violating the referening rule of productid in order details
ALTER TABLE order_details
DROP CONSTRAINT IF EXISTS order_details_productid_fkey

ALTER TABLE order_details
ADD CONSTRAINT order_details_productid_fkey
FOREIGN KEY (productid) REFERENCES products(productid)
ON UPDATE CASCADE
ON DELETE CASCADE


/*
4.  Delete the customer = “VINET”  from customers. Corresponding customers in orders table should be set to null 
(HINT: Alter the foreign key on orders(customerID) to use ON DELETE SET NULL)
*/

ALTER TABLE orders
DROP CONSTRAINT IF EXISTS orders_customerid_fkey;

ALTER TABLE orders
ADD CONSTRAINT orders_customerid_fkey
FOREIGN KEY (customerid) REFERENCES customers(customerid)
ON UPDATE CASCADE
ON DELETE SET NULL;

ALTER TABLE orders
ALTER COLUMN customerid
DROP NOT NULL;

DELETE FROM customers
WHERE customerid = 'VINET'
RETURNING *;

SELECT * FROM orders
WHERE customerid IS NULL;

/*
5. Insert the following data to Products using UPSERT:
product_id = 100, product_name = Wheat bread, quantityperunit=1,unitprice = 13, discontinued = 0, categoryID=5
product_id = 101, product_name = White bread, quantityperunit=5 boxes,unitprice = 13, discontinued = 0, categoryID=5
product_id = 100, product_name = Wheat bread, quantityperunit=10 boxes,unitprice = 13, discontinued = 0, categoryID=5
(this should update the quantityperunit for product_id = 100)
*/

INSERT INTO products(productid, productname, qantityperunit, unitprice, discontinued, categoryid)
VALUES (100, 'Wheat bread', '1', 13, 0, 5),
		(101, 'White Bread', '5 boxes', 13, 0, 5)		
ON CONFLICT (productid)
DO UPDATE 
SET productname = EXCLUDED.productname,
	qantityperunit = EXCLUDED.qantityperunit,
	unitprice = EXCLUDED.unitprice,
	discontinued = EXCLUDED.discontinued,
	categoryid = EXCLUDED.categoryid;


INSERT INTO products(productid, productname, qantityperunit, unitprice, discontinued, categoryid)
VALUES (100, 'Wheat Bread', '10 boxes', 13, 0, 5)		
ON CONFLICT (productid)
DO UPDATE 
SET productname = EXCLUDED.productname,
	qantityperunit = EXCLUDED.qantityperunit,
	unitprice = EXCLUDED.unitprice,
	discontinued = EXCLUDED.discontinued,
	categoryid = EXCLUDED.categoryid;


SELECT * FROM products
WHERE productid=100 OR productid=101;

/*
  Write a MERGE query:
 Update the price and discontinued status for from below table ‘updated_products’ 
 only if there are matching products and updated_products .discontinued =0 

If there are matching products and 
updated_products .discontinued =1 then delete 
 
 Insert any new products from updated_products that don’t 
 exist in products only if updated_products .discontinued =0.
*/

CREATE TEMP TABLE updates_products(productid INT, 
productname VARCHAR(250), 
qantityperunit VARCHAR(250), 
unitprice NUMERIC, 
discontinued INT, 
categoryid INT );

DROP TABLE updates_products

INSERT INTO updates_products (
productid, 
productname, 
qantityperunit, 
unitprice, 
discontinued, 
categoryid 
)
VALUES (100, 'Wheat Bread', '10', 20, 1, 5),
		(101, 'White Bread', '5 boxes', 19.99, 0, 5),
		(102, 'Midnight Mango Fizz', '24 - 12 oz bottles', 19, 0, 1001),
		(103, 'Savory Fire Sauce','12 - 550 ml bottles',10,0,2);

SELECT * FROM updates_products

SELECT * FROM products
WHERE productid IN (100, 101, 102, 103)

MERGE INTO products p
USING updates_products up
ON p.productid = up.productid
WHEN MATCHED AND up.discontinued=1 THEN
	DELETE
WHEN MATCHED AND up.discontinued=0 THEN
	UPDATE SET 
	unitprice=up.unitprice,
	discontinued=up.discontinued
WHEN NOT MATCHED AND up.discontinued=0 THEN
	INSERT (productid, productname, qantityperunit, unitprice, discontinued, categoryid )
	VALUES (up.productid, up.productname, up.qantityperunit, up.unitprice, up.discontinued, up.categoryid)

/*
7. List all orders with employee full names. (Inner join)
*/

SELECT e.employeename, o.orderid FROM orders o
INNER JOIN employees e
ON e.employeeid=o.employeeid

--********************************************************************************************************************

/*
1.     List all customers and the products they ordered with the order date. (Inner join)
Tables used: customers, orders, order_details, products
Output should have below columns:
    companyname AS customer,
    orderid,
    productname,
    quantity,
    orderdate
*/
SELECT c.company_name AS customer,
		o.order_id,
		p.product_name,
		od.quantity,
		o.order_date
FROM customers c
INNER JOIN orders o
USING (customer_id)
INNER JOIN order_details od
USING (order_id)
INNER JOIN products p
USING (product_id)


/*
2. Show each order with customer, employee, shipper, and product info — even if some parts are missing. (Left Join)
Tables used: orders, customers, employees, shippers, order_details, products
*/
SELECT o.order_id,
		c.company_name AS customer,
		e.first_name || ' ' || e.last_name AS employee,
		s.company_name,
		p.product_name
FROM orders o
LEFT JOIN customers c
USING (customer_id)
LEFT JOIN shippers s
ON o.ship_via = s.shipper_id
LEFT JOIN employees e
USING (employee_id)
LEFT JOIN order_details od
USING (order_id)
LEFT JOIN products p
USING (product_id)

/*
3. Show all order details and products (include all products even if they were never ordered). (Right Join)
Tables used: order_details, products
Output should have below columns:
    orderid,
    productid,
    quantity,
    productname
*/
SELECT od.order_id,
		p.product_id,
		od.quantity,
		p.product_name
FROM order_details od
RIGHT JOIN products p
USING (product_id)

/*
4. 	List all product categories and their products — including categories that have no products, and 
products that are not assigned to any category.(Outer Join)
Tables used: categories, products
*/
SELECT c.category_name,
		p.product_name
FROM categories c
FULL OUTER JOIN products p
USING (category_id)

/*
5. 	Show all possible product and category combinations (Cross join).
*/
SELECT p.product_name,
		c.category_name
FROM products p
CROSS JOIN categories c

/*
Show all employees and their manager(Self join(left join))
*/
SELECT e.first_name || ' ' || e.last_name AS employee,
		m.first_name || ' ' || m.last_name AS manager
FROM employees e
LEFT JOIN employees m
ON e.reports_to = m.employee_id

/*
List all customers who have not selected a shipping method.
Tables used: customers, orders
(Left Join, WHERE o.shipvia IS NULL)
*/
SELECT c.company_name AS customer,
		o.ship_via
FROM customers c
LEFT JOIN orders o
USING (customer_id)
WHERE o.ship_via IS NULL


--**************************************************************************************************************

/*
1. GROUP BY with WHERE - Orders by Year and Quarter
Display, order year, quarter, order count, avg freight cost only for those orders where freight cost > 100
*/
SELECT EXTRACT(YEAR FROM order_date) AS order_year,
		EXTRACT(QUARTER FROM order_date) AS order_quarter,
		COUNT(order_id) AS count_of_orders,
		ROUND(AVG(freight)::NUMERIC,2) AS avg_freight
FROM orders
WHERE freight > 100
GROUP BY order_year,order_quarter

/*
2. GROUP BY with HAVING - High Volume Ship Regions
Display, ship region, no of orders in each region, min and max freight cost
 Filter regions where no of orders >= 5
*/
SELECT * FROM orders

SELECT ship_region,
		COUNT(order_id) AS order_count,
		MAX(freight) AS max_freight,
		MIN(freight) AS min_freight
FROM orders
GROUP BY ship_region
HAVING COUNT(order_id) >= 5

/* 
3. Get all title designations across employees and customers ( Try UNION & UNION ALL)
*/
SELECT * from customers

SELECT title AS designation
FROM employees

UNION

SELECT contact_title AS designation
FROM customers
ORDER BY designation

SELECT title AS designation
FROM employees

UNION ALL

SELECT contact_title AS designation
FROM customers
ORDER BY designation

/*
   Find categories that have both discontinued and in-stock products
(Display category_id, instock means units_in_stock > 0, Intersect)
*/
SELECT * FROM products
SELECT * FROM categories 

SELECT category_id
FROM products
WHERE units_in_stock >0

INTERSECT

SELECT category_id
FROM products
WHERE discontinued = 1

/*
5. Find orders that have no discounted items (Display the  order_id, EXCEPT)
*/

SELECT * FROM order_details

SELECT order_id
FROM order_details 
WHERE discount = 0

EXCEPT 

SELECT order_id
FROM order_details 
WHERE discount >0

--*************************************************************************************************************


/*
1.   Categorize products by stock status
(Display product_name, a new column stock_status whose values are based on below condition
 units_in_stock = 0  is 'Out of Stock'
       units_in_stock < 20  is 'Low Stock')
*/
SELECT product_name,
CASE 
	WHEN units_in_stock = 0 THEN 'Out of Stcok'
	WHEN units_in_stock < 20 THEN 'Low Stock'
ELSE
	'In Stock'
END AS stock_status
FROM products

/*
2. Find All Products in Beverages Category
(Subquery, Display product_name,unitprice)
*/

SELECT * FROM categories

SELECT product_name,
		unit_price
FROM products
WHERE category_id IN (
						SELECT category_id
						FROM categories
						WHERE category_name = 'Beverages'
)

/*
3.   Find Orders by Employee with Most Sales
(Display order_id,   order_date,  freight, employee_id.
Employee with Most Sales=Get the total no.of of orders for each employee then order by DESC and limit 1. Use Subquery)
*/
SELECT order_id,
		order_date,
		freight,
		employee_id
FROM orders
WHERE employee_id = (
					SELECT employee_id
					FROM orders
					GROUP BY employee_id
					ORDER BY COUNT(order_id) DESC
					LIMIT 1
					)

/*
 Find orders  where for country!= ‘USA’ with freight costs 
 higher than any order from USA. (Subquery, Try with ANY, ALL operators)
*/

SELECT * FROM orders

--ANY operator
SELECT order_id,
		ship_country,
		freight
FROM orders
WHERE ship_country != 'USA'
		AND freight > ANY (
							SELECT freight
							FROM orders
							WHERE ship_country = 'USA'
							)

--ALL operator
SELECT order_id,
		ship_country,
		freight
FROM orders
WHERE ship_country != 'USA'
		AND freight > ALL (
							SELECT freight
							FROM orders
							WHERE ship_country = 'USA'
							)

--*****************************************************************************************

/*
1. Rank employees by their total sales
(Total sales = Total no of orders handled, JOIN employees and orders table)
*/
SELECT * FROM orders
SELECT * FROM employees

WITH employee_total_sales AS(
	SELECT employee_id,
			COUNT(order_id) AS order_count
	FROM orders
	GROUP BY employee_id
)
SELECT first_name || ' ' || last_name AS employee_name,
		order_count,
		RANK() OVER(ORDER BY order_count DESC) AS employee_rank
FROM employees
JOIN employee_total_sales
USING (employee_id)

/*
2. Compare current order's freight with previous and next order for each customer.
(Display order_id,  customer_id,  order_date,  freight,
Use lead(freight) and lag(freight).
*/
--lag
SELECT order_id,
		customer_id,
		order_date,
		freight,
		LAG(freight,1, freight) OVER( PARTITION BY customer_id ORDER BY order_date) AS previous_order_freight,
		ROUND((freight - LAG(freight,1,freight) OVER( PARTITION BY customer_id ORDER BY order_date))::NUMERIC,2) AS freight_difference_last_order
FROM orders

--lead
SELECT order_id,
		customer_id,
		order_date,
		freight,
		LEAD(freight, 1, freight) OVER(PARTITION BY customer_id ORDER BY order_date) AS next_order_freight,
		ROUND((freight - LEAD(freight,1,freight) OVER(PARTITION BY customer_id ORDER BY order_date))::NUMERIC, 2) AS freight_difference_next_order
FROM orders

/*
3. Show products and their price categories, product count in each category, avg price:
        	(HINT:
·  	Create a CTE which should have price_category definition:
        	WHEN unit_price < 20 THEN 'Low Price'
            WHEN unit_price < 50 THEN 'Medium Price'
            ELSE 'High Price'
·  	In the main query display: price_category,  product_count in each price_category,  ROUND(AVG(unit_price)::numeric, 2) as avg_price)
*/

SELECT * FROM products

WITH products_price_category AS(
	SELECT product_id,
	product_name,
	CASE 
		WHEN unit_price < 20 THEN 'Low Price'
		WHEN unit_price < 50 THEN 'Medium Price'
	ELSE 
		'High Price'
	END AS price_category
	FROM products
)
SELECT price_category,
		COUNT(product_id) AS product_count,
		ROUND(AVG(unit_price)::NUMERIC,2) AS avg_price
FROM products
JOIN products_price_category
USING (product_id)
GROUP BY price_category
ORDER BY product_count

--**************************************************************************************************

/*
1.     Create view vw_updatable_products (use same query whatever I used in the training)
Try updating view with below query and see if the product table also gets updated.
Update query:
UPDATE updatable_products SET unit_price = unit_price * 1.1 WHERE units_in_stock < 10;
*/

CREATE VIEW vw_updatable_products AS
SELECT product_id,
		product_name,
		unit_price,
		units_in_stock,
		discontinued
FROM products
WHERE discontinued = 0
WITH CHECK OPTION;

UPDATE vw_updatable_products
SET unit_price = unit_price * 1.1
WHERE units_in_stock < 10

SELECT * FROM products
WHERE units_in_stock < 10

/*
2. Transaction:
Update the product price for products by 10% in category id=1
Try COMMIT and ROLLBACK and observe what happens.
*/
BEGIN;

UPDATE products
SET unit_price = unit_price * 1.1
WHERE category_id = 1;

DO $$
BEGIN 
	IF EXISTS(
			SELECT 1
			FROM products
			WHERE category_id=1 AND unit_price > 50
	)
	THEN 
		RAISE EXCEPTION 'Some prices exceed 50';
	ELSE
		RAISE NOTICE 'Unit price updated successfully';
	END IF;
END $$;

COMMIT;

ROLLBACK;

SELECT * FROM products 
WHERE category_id=1 AND unit_price > 40

/*
3. Create a regular view which will have below details (Need to do joins):
Employee_id,
Employee_full_name,
Title,
Territory_id,
territory_description,
region_description
*/
SELECT * FROM employees 
SELECT * FROM employee_territories 
SELECT * FROM territories
SELECT * FROM region

CREATE OR REPLACE VIEW employee_territory_region AS
SELECT employee_id,
		first_name || ' ' || last_name AS employee_name,
		title,
		et.territory_id,
		territory_description,
		region_description
FROM employees
JOIN employee_territories et
USING (employee_id)
JOIN territories 
USING (territory_id)
JOIN region
USING (region_id)

SELECT * FROM employee_territory_region

/*
4. Create a recursive CTE based on Employee Hierarchy
*/

WITH RECURSIVE cte_employee_hierarchy AS(
SELECT e.employee_id,
 		e.first_name || ' ' || e.last_name AS employee_name,
		e.reports_to,
		0 AS LEVEL
FROM employees e
WHERE reports_to IS NULL

UNION ALL 

SELECT e.employee_id,
 		e.first_name || ' ' || e.last_name AS employee_name,
		e.reports_to,
		eh.level+1
FROM employees e
JOIN cte_employee_hierarchy eh
ON eh.employee_id = e.reports_to
)

SELECT level,
		employee_id,
		first_name ||  ' ' || last_name
FROM cte_employee_hierarchy
ORDER BY level, employee_id


--*************************************************************************************

--1.  Create AFTER UPDATE trigger to track product price changes
CREATE TABLE product_price_audit (
audit_id SERIAL PRIMARY KEY,
product_id INT,
product_name VARCHAR(40),
old_price DECIMAL(10,2),
new_price DECIMAL(10,2),
change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
user_name VARCHAR(50) DEFAULT CURRENT_USER
)

CREATE OR REPLACE FUNCTION new_unit_price()
RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO product_price_audit(
	product_id,
	product_name,
	old_price,
	new_price
	)
	VALUES (
	old.product_id,
	old.product_name,
	old.unit_price,
	new.unit_price
	);
	RETURN NEW; 
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER change_unit_price
AFTER UPDATE ON products
FOR EACH ROW 
EXECUTE FUNCTION new_unit_price();

UPDATE products
SET unit_price = unit_price * 1.1
WHERE product_id = 1 

SELECT * FROM products
WHERE product_id = 1

/*
2.  Create stored procedure  using IN and INOUT parameters to assign tasks to employees
Parameters:
IN p_employee_id INT,
IN p_task_name VARCHAR(50),
INOUT p_task_count INT DEFAULT 0
*/

 CREATE TABLE IF NOT EXISTS employee_tasks (
        task_id SERIAL PRIMARY KEY,
        employee_id INT,
        task_name VARCHAR(50),
        assigned_date DATE DEFAULT CURRENT_DATE)

CREATE OR REPLACE PROCEDURE assign_task(p_employee_id INT, p_task_name VARCHAR(50), INOUT p_task_count INT DEFAULT 0)
LANGUAGE plpgsql
AS $$
BEGIN
	--inserting the values in employee_tasks table
	INSERT INTO employee_tasks(employee_id, task_name)
	VALUES(p_employee_id,p_task_name);
	
	--calculating total number of task for employee
	SELECT COUNT(*)
	INTO p_task_count
	FROM employee_tasks
	WHERE employee_id = p_employee_id;

	RAISE NOTICE 'Task "%" assigned is to employee "%". Total tasks: "%"',p_task_name, p_employee_id, p_task_count;
	
END;
$$;

--calling the stored procedure
CALL assign_task(1,'Review Reports');

SELECT * FROM employee_tasks


--**********************************************************************************************************

/*
1. Write  a function to Calculate the total stock value for a given category:
(Stock value=ROUND(SUM(unit_price * units_in_stock)::DECIMAL, 2)
Return data type is DECIMAL(10,2)
*/
SELECT * FROM products
SELECT * FROM categories

CREATE OR REPLACE FUNCTION total_stock_value_of_category(c_category_id INT)
RETURNS DECIMAL(10,2)
LANGUAGE plpgsql
AS $$
DECLARE
	total_stock_value DECIMAL(10,2);
BEGIN
	--validate category exists
	IF NOT EXISTS (SELECT 1 FROM categories WHERE category_id = c_category_id) THEN
		RAISE EXCEPTION 'Category ID % does nto exist', c_category_id;
		RETURN 0;
	END IF;

	--calculate total stock value
	SELECT
		ROUND(SUM(unit_price * units_in_stock)::DECIMAL,2)
		INTO total_stock_value
	FROM products p
	JOIN categories c
	USING (category_id)
	WHERE p.category_id = c_category_id;

	RETURN total_stock_value;
END;
$$;

--execute the function
SELECT total_stock_value_of_category(1);

/*
2. Write an example of cursor query.
*/

CREATE OR REPLACE PROCEDURE update_prices_with_cursor()
LANGUAGE plpgsql
AS $$
DECLARE 
	product_cursor CURSOR FOR
		SELECT product_id, product_name, unit_price, units_in_stock
		FROM products
		WHERE discontinued = 0;
	product_record RECORD;
	v_new_price DECIMAL(10,2);
BEGIN
	--open the cursor
	OPEN product_cursor;
	LOOP
		--fetch the next row
		FETCH product_cursor INTO product_record;

		--exit when nor more rows to fetch
		EXIT WHEN NOT FOUND;

		--calculate the new price
		IF product_record.unit_price < 10 THEN	
			v_new_price = product_record.unit_price * 1.1; --10% increase
		ELSE
			v_new_price = product_record.unit_price * 0.95; --5% decrease
		END IF; 

		--update the price
		UPDATE products
		SET unit_price = ROUND(v_new_price,2)
		WHERE product_id = product_record.product_id;

		--log the change
		RAISE NOTICE 'Update % price from % to %',
			product_record.product_id,
			product_record.unit_price,
			v_new_price;
		END LOOP;

		--close the cursor
		CLOSE product_cursor;
END;
$$;

--exceute the procedure
CALL update_prices_with_cursor();        	
	