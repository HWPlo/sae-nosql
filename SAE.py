
# SAE MIGRATION SQL --> NoSQL
# IMPORTATION DES MODULES :
import sqlite3
import pandas


# CONNEXION AVEC LA BASE SQL :
conn = sqlite3.connect("ClassicModel.sqlite")


# TEST D'UNE REQUÊTE SQL :
pandas.read_sql_query("SELECT * FROM Customers;", conn)


# REQUÊTES A FAIRE :
# 1) Lister les clients n’ayant jamais effecuté une commande ;

q1 = pandas.read_sql_query(
    """
    SELECT c.customerName, c.customerNumber
    FROM Customers c
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL;""", conn)
                           
                           
# 2) Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci ;

q2 = pandas.read_sql_query(
    """
    SELECT e.employeeNumber,
    e.firstName,
    e.lastName,
    COUNT(DISTINCT c.customerNumber) AS numberOfCustomers,
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
    SUM(pa.amount) AS totalOrderAmount
    FROM Employees e
    LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber 
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN Payments pa ON o.customerNumber = pa.customerNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName;
    """, conn
    )


# 3) Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a ;

q3 = pandas.read_sql_query(
    """
    SELECT off.officeCode,
    off.country,
    COUNT(DISTINCT c.customerNumber) AS numberOfCustomers,
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
    SUM(pa.amount) AS totalOrderAmount,
    COUNT(DISTINCT CASE WHEN off.country != c.country THEN c.customerNumber ELSE NULL END)
    FROM Offices off
    LEFT JOIN Employees e ON off.officeCode = e.officeCode
    LEFT JOIN Customers c ON e.employeeNumber = c.salesRepEmployeeNumber 
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN Payments pa ON c.customerNumber = pa.customerNumber
    GROUP BY off.officeCode, off.country;
    """, conn
    )


# 4) Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents ;

q4 = pandas.read_sql_query(
    """
    SELECT p.productCode,
    p.productName,
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
    SUM(od.quantityOrdered) AS quantityOfOrders,
    COUNT(DISTINCT o.customerNumber) AS numberOfCustomers
    FROM Products p
    LEFT JOIN OrderDetails od ON p.productCode = od.productCode
    LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode;
    """, conn
    )


# 5) Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final ;

q5 = pandas.read_sql_query(
    """
    SELECT 
    c.country,
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders,
    SUM(od.quantityOrdered * od.priceEach) AS totalOrderAmount,
    SUM(pa.amount) AS totalPaidAmount
    FROM Customers c
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
    LEFT JOIN Payments pa ON c.customerNumber = pa.customerNumber
    GROUP BY c.country;
    """, conn
    )
    

# 6) On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client ;

q6 = pandas.read_sql_query(
    """
    SELECT 
    p.productLine,
    c.country,
    COUNT(DISTINCT o.orderNumber) AS numberOfOrders
    FROM Orders o
    LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
    LEFT JOIN Products p ON od.productCode = p.productCode
    LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
    GROUP BY p.productLine, c.country
    ORDER BY p.productLine, c.country;
    """, conn
    )


# 7) On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule ;

q7 = pandas.read_sql_query(
    """
    SELECT 
    p.productLine,
    c.country,
    SUM(pa.amount) AS totalPaidAmount
    FROM Orders o
    LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
    LEFT JOIN Products p ON od.productCode = p.productCode
    LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
    LEFT JOIN Payments pa ON c.customerNumber = pa.customerNumber
    GROUP BY p.productLine, c.country
    ORDER BY p.productLine, c.country;
    """, conn
    )


# 8) Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach) ;

q8 = pandas.read_sql_query(
    """
    SELECT
    p.productName,
    AVG(od.priceEach-p.buyPrice) as AverageMargin
    FROM Products p
    LEFT JOIN OrderDetails od ON p.productCode = od.productCode
    GROUP BY p.productName
    ORDER BY AverageMargin DESC
    LIMIT 10;
    """, conn
    )


# 9) Lister les produits (avec le nom et le code du client) qui ont été vendus à perte :
#        Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois, 
#        Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;

q9 = pandas.read_sql_query(
    """
    SELECT
    p.productName,
    c.customerNumber,
    od.priceEach,
    p.buyPrice
    FROM Products p
    LEFT JOIN OrderDetails od ON p.productCode = od.productCode
    LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
    LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
    WHERE od.priceEach < p.buyPrice;
    """, conn
    )


# 10) Lister les clients pour lesquels le montant total payé est inférieur aux montants totals des achats ;

q10 = pandas.read_sql_query(
    """
    SELECT 
    c.customerName,
    c.customerNumber,
    SUM(od.quantityOrdered * od.priceEach) AS totalOrderAmount,
    SUM(pa.amount) AS totalPaidAmount
    FROM Customers c
    LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
    LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
    LEFT JOIN Payments pa ON c.customerNumber = pa.customerNumber
    GROUP BY c.customerName, c.customerNumber
    HAVING SUM(pa.amount) < SUM(od.quantityOrdered * od.priceEach);
    """, conn
    )


# FERMER LA BASE SQL :
conn.close()


