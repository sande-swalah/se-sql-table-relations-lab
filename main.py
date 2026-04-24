# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
df_boston = pd.read_sql(
	"""
	SELECT e.firstName, e.lastName
	FROM employees AS e
	JOIN offices AS o
		ON e.officeCode = o.officeCode
	WHERE o.city = 'Boston'
	ORDER BY e.firstName
	""",
	conn,
)

# STEP 2
# Replace None with your code
df_zero_emp = pd.read_sql(
	"""
	SELECT o.officeCode, o.city
	FROM offices AS o
	LEFT JOIN employees AS e
		ON o.officeCode = e.officeCode
	GROUP BY o.officeCode, o.city
	HAVING COUNT(e.employeeNumber) = 0
	""",
	conn,
)

# STEP 3
# Replace None with your code
df_employee = pd.read_sql(
	"""
	SELECT e.firstName, e.lastName, o.city, o.state
	FROM employees AS e
	LEFT JOIN offices AS o
		ON e.officeCode = o.officeCode
	ORDER BY e.firstName, e.lastName
	""",
	conn,
)

# STEP 4
# Replace None with your code
df_contacts = pd.read_sql(
	"""
	SELECT
		c.contactFirstName,
		c.contactLastName,
		c.phone,
		c.salesRepEmployeeNumber
	FROM customers AS c
	LEFT JOIN orders AS o
		ON c.customerNumber = o.customerNumber
	WHERE o.orderNumber IS NULL
	ORDER BY c.contactLastName, c.contactFirstName
	""",
	conn,
)

# STEP 5
# Replace None with your code
df_payment = pd.read_sql(
	"""
	SELECT
		c.contactFirstName,
		c.contactLastName,
		p.amount,
		p.paymentDate
	FROM customers AS c
	JOIN payments AS p
		ON c.customerNumber = p.customerNumber
	ORDER BY CAST(p.amount AS REAL) DESC
	""",
	conn,
)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql(
	"""
	SELECT
		e.employeeNumber,
		e.firstName,
		e.lastName,
		COUNT(c.customerNumber) AS n_customers
	FROM employees AS e
	JOIN customers AS c
		ON e.employeeNumber = c.salesRepEmployeeNumber
	GROUP BY e.employeeNumber, e.firstName, e.lastName
	HAVING AVG(c.creditLimit) > 90000
	ORDER BY n_customers DESC
	""",
	conn,
)

# STEP 7
# Replace None with your code
df_product_sold = pd.read_sql(
	"""
	SELECT
		p.productName,
		COUNT(od.orderNumber) AS numorders,
		SUM(od.quantityOrdered) AS totalunits
	FROM products AS p
	JOIN orderdetails AS od
		ON p.productCode = od.productCode
	GROUP BY p.productCode, p.productName
	ORDER BY totalunits DESC
	""",
	conn,
)

# STEP 8
# Replace None with your code
df_total_customers = pd.read_sql(
	"""
	SELECT
		p.productName,
		p.productCode,
		COUNT(DISTINCT o.customerNumber) AS numpurchasers
	FROM products AS p
	JOIN orderdetails AS od
		ON p.productCode = od.productCode
	JOIN orders AS o
		ON od.orderNumber = o.orderNumber
	GROUP BY p.productCode, p.productName
	ORDER BY numpurchasers DESC
	""",
	conn,
)

# STEP 9
# Replace None with your code
df_customers = pd.read_sql(
	"""
	SELECT
		COUNT(c.customerNumber) AS n_customers,
		o.officeCode,
		o.city
	FROM offices AS o
	LEFT JOIN employees AS e
		ON o.officeCode = e.officeCode
	LEFT JOIN customers AS c
		ON e.employeeNumber = c.salesRepEmployeeNumber
	GROUP BY o.officeCode, o.city
	ORDER BY o.officeCode
	""",
	conn,
)

# STEP 10
# Replace None with your code
df_under_20 = pd.read_sql(
	"""
	SELECT DISTINCT
		e.employeeNumber,
		e.firstName,
		e.lastName,
		of.city,
		of.officeCode
	FROM employees AS e
	JOIN customers AS c
		ON e.employeeNumber = c.salesRepEmployeeNumber
	JOIN orders AS o
		ON c.customerNumber = o.customerNumber
	JOIN orderdetails AS od
		ON o.orderNumber = od.orderNumber
	JOIN offices AS of
		ON e.officeCode = of.officeCode
	WHERE od.productCode IN (
		SELECT od2.productCode
		FROM orderdetails AS od2
		JOIN orders AS o2
			ON od2.orderNumber = o2.orderNumber
		GROUP BY od2.productCode
		HAVING COUNT(DISTINCT o2.customerNumber) < 20
	)
	ORDER BY e.lastName
	""",
	conn,
)

conn.close()