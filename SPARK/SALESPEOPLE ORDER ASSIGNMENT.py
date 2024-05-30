from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, count, col, lit, trim, sum, collect_list, avg, desc, asc, countDistinct,exists,concat,row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,DecimalType,StructType,StructField,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    a = StructType([
        StructField("snum",IntegerType()),
        StructField("sname",StringType()),
        StructField("city", StringType()),
        StructField("comm", DecimalType(6,2))
    ])

    b = StructType([
        StructField("cnum", IntegerType()),
        StructField("cname", StringType()),
        StructField("city", StringType()),
        StructField("rating", IntegerType()),
        StructField("snum", IntegerType()),
    ])


    c = StructType([
        StructField("onum", IntegerType()),
        StructField("amt", DecimalType(10,2)),
        StructField("odate", StringType()),
        StructField("cnum", IntegerType()),
        StructField("snum", IntegerType()),
    ])


    df1 = spark.read.csv(r"D:\SPARK\salespeople.csv",schema=a)
    print("SALESPEOPLE TABLE")
    df1.show()

    df2 = spark.read.csv(r"D:\SPARK\customer.csv",schema=b)
    print("CUSTOMERS TABLE")
    df2.show()

    df3 = spark.read.csv(r"D:\SPARK\orders.csv", schema=c)
    print("ORDERS TABLE")
    df3.show()



    print("########### ASIIGNMENT NO 01 #############")

    print("## Q1. List all customers with a rating of 100. ##")

    ans = df2.filter(df2.rating == 100).select("*")
    ans.show()

    print("## Q2. Find all records in the Customer table with NULL values in the city column. ##")

    ans = df2.filter(df2.city.isNull()).select("*")
    ans.show()

    print("## Q3. Find the largest order taken by each salesperson on each date. ##")

    ans = df1.join(df3,df1.snum == df3.snum,"inner")

    result = ans.groupBy(df1.snum, df1.sname, df3.odate).agg(max(df3.amt).alias("max_order_amount"))

    result = result.select(df1.snum, df1.sname, df3.odate, "max_order_amount").orderBy(df3.odate)

    result.show()

    print("## Q4. Arrange the Orders table by descending customer number. ##")

    ans = df3.orderBy(df3.cnum.desc()).select("*")

    ans.show()

    print("## Q5. Find which salespeople currently have orders in the Orders table. ##")


    # Join salespeople (df1) with orders (df3)
    ans = df1.join(df3, df1.snum == df3.snum, "inner")

    result = ans.select(df1.snum, df1.sname, df3.onum)

    result = result.dropDuplicates()

    result = result.orderBy("snum")

    result.show()

    ans = df1.join(df3, df1.snum == df3.snum, "right")

    result = ans.select(df1.snum, df1.sname, df3.onum)

    result = result.orderBy("snum")

    result.show()

    print("## Q6. List names of all customers matched with the salespeople serving them. ##")
    ans1 = df1.join(df2,"snum","inner")
    ans = ans1.join(df3, df2.cnum == df3.cnum, "inner")

    result = ans.select(df2.cnum, df2.cname, df3.snum,df1.sname)

    result = result.distinct()

    result = result.orderBy(df2.cnum)

    result.show()

    print("## Q7. Find the names and numbers of all salespeople who had more than one customer. ##")

    a = df2.join(df1, df2.snum == df1.snum, "inner")

    a2 = a.groupBy(df1.snum, df1.sname)

    a3 = a2.count()

    a4 = a3.filter(a3["count"] > 1)

    a5 = a4.select(df1.snum, df1.sname,"count")

    a5.show()


    print("## Q8. Count the orders of each of the salespeople and output the results in descending order. ##")

    ans = df1.join(df3, df1.snum == df3.snum, "inner")

    a2 = ans.groupBy(df1.snum, df1.sname)

    a3 = a2.count()

    a4 = a3.orderBy(a3["count"].desc())

    a4.show()

    print("## Q9. List the Customer table if and only if one or more of the customers in the Customer tables are located in San Jose. ##")

    result = df2.filter(trim(df2.city) == "San Jose").groupBy("city", "cnum", "cname").count().filter(col("count") > 1).select("city", "cnum", "cname", "count")
    result.show()

    a = df2.filter(trim(df2.city) == "San Jose")
    a.show()

    print("## Q10. Match salespeople to customers according to what city they lived in.. ##")

    ans = df1.join(df2,"city","inner")
    a = ans.filter(df1.city == df2.city)
    a.select(df1.sname,df1.city,df2.cname,df2.city).orderBy(df1.sname).show()

    print("## Q11. Find the largest order taken by each salesperson. ##")

    a = df1.join(df3,"snum","inner")
    b = a.groupBy(df1.snum,df1.sname).agg(max(df3.amt).alias("Largest Order")).select(df1.snum,df1.sname,"Largest Order")
    b.show()

    print("## Q12. Find customers in San Jose who has a rating above 200. ##")

    a = df2.filter(df2.city == "San Jose").filter(df2.rating >200)
    a.show()

    print("## Q13. List the names and commissions of all salespeople in London ##")

    a = df1.filter(df1.city == "London").select(df1.sname,df1.comm,df1.city)
    a.show()

    print("## Q14. List all the orders of salesperson Monika from the Orders table. ##")

    a = df1.join(df3,"snum","inner")
    b = a.filter(df1.sname=="Monika").select(df1.sname,df3.cnum,df3.onum)
    b.show()

    print("## Q15. Find all customers with orders on October 3. ##")

    a = df2.join(df3,"cnum","inner")
    b = a.filter(df3.odate == "03-Oct-96").orderBy(df3.cnum).select(df2.cname,df2.cnum,df3.odate)
    b.show()

    print("## Q16. Give the sums of the amounts from the orders table, grouped by date ##")

    a = df3.groupBy(df3.odate).agg(sum(df3.amt).alias("Sum of Amount"))
    a.orderBy(df3.odate).select(df3.odate,"Sum of Amount").show()

    print("## Q17. Eliminating all those dates where the SUM was not at least 2000.00 above the MAX amount. ##")

    a = (df3.groupBy(df3.odate).agg(sum(df3.amt).alias("sum of amt"),max(df3.amt).alias("Max amt on date")))
    a.filter((sum(df3.amt) > max(df3.amt)+2000)).orderBy(df3.odate).select(df3.odate,"Max amt on date","sum of amt").show()

    print("## Q18. Select all orders that had amounts that were greater than at least one of the orders from October 6. ##")

    # a = df3.filter(df3.amt > (df3.agg(min(df3.amt)).filter(df3.odate == "03-Oct-96")))
    # a.show()

    a = df3.filter(df3.amt > df3.filter(df3.odate == "06-Oct-96").agg(min(df3.amt)).collect()[0]["min(amt)"])
    a.show()

    print("## Q20. Find all pairs of customers having the same rating. ##")

    a = df2.alias("A").join(df2.alias("B"),
                                (col("A.rating") == col("B.rating")) & (col("A.cname") < col("B.cname")))


    b = a.select(col("A.cname").alias("customer1"), col("B.cname").alias("customer2"),
                           col("A.rating")).orderBy(col("A.rating"))

    a.show()

    print("## Q21. Find all customers with CNUM, 1000 above the SNUM of Serres. ##")
    a = df1.join(df2, "snum", "inner")
    b = a.filter(df1.sname == "Serres").select("snum").first()[0]
    c = df2.filter(df2.cnum == b + 1000)
    c.show()

    print("## Q22. Give the salespeople’s commissions as percentage instead of decimal numbers. ##")

    df1.select("snum","sname","city","comm",(df1.comm*100).alias("Commission %")).show()

    print("## Q23. Find the largest order taken by each salesperson on each date, eliminating those MAX orders, which are less than $3000.00 in value. ##")

    a = df1.join(df3,"snum","inner")
    b = a.groupBy(df1.snum,df1.sname,df3.odate)
    c = b.agg(max(df3.amt).alias("Largest Order"))
    d = c.filter(max(df3.amt)>3000)
    e = d.select(df1.snum,df1.sname,df3.odate,"Largest Order").orderBy(df3.odate)
    e.show()

    print("## Q24. List the largest orders on October 3, for each salesperson. ##")

    a = df1.join(df3,"snum","inner")
    b = a.groupBy(df1.snum,df1.sname,df3.odate)
    c = b.agg(max(df3.amt).alias("Largest Order"))
    d = c.filter(df3.odate == "03-Oct-96")
    e = d.select(df1.snum,df1.sname,df3.odate,"Largest Order").orderBy(df1.snum)
    e.show()


    print("## Q25. Find all customers located in cities where Serres (SNUM 1002) has customers. ##")

    a = df2.filter(df2.snum == 1002).select(df2.city).distinct()

    b = df2.join(a, "city", "inner").select(df2.cnum, df2.cname, df2.city)

    b.show()

    print("## Q26. Select all customers with a rating above 200.00. ##")

    df2.filter(df2.rating>200).show()

    print("## Q27. Count the number of salespeople currently listing orders in the Orders table. ##")

    a = df3.select(df3.snum).distinct().count()

    print(f" ** Number of salespeople currently listing orders ** :-  {a}")

    print("## Q28. Write a query that produces all customers serviced by salespeople with a commission above 12%. Output the customer’s name and the salesperson‘s rate of commission. ##")

    a = df1.join(df2,"snum","inner")

    b = a.filter(df1.comm > 0.12)

    c = b.select(df1.snum,df1.sname,df2.cname,df1.comm).orderBy(df1.snum)

    c.show()

    print("## Q29. Find salespeople who have multiple customers. ##")

    a1 = df1.join(df2,"snum","inner")

    a = a1.join(df3,"cnum","inner")

    b = a.groupBy(df1.snum,df1.sname)

    c = b.agg(count(df1.snum).alias("Number of Customers"), collect_list(df3.cnum).alias("Customers"))

    d = c.filter(count(df1.snum)>1).select(df1.snum,df1.sname,"Number of Customers","Customers").orderBy(df1.snum)

    d.show()



    print("## Q30. Find salespeople with customers located in their city. ##")

    a = df1.join(df2,"city","inner")

    b = a.filter(df1.city == df2.city).select(df1.sname,df1.city,df2.cname,df2.city).orderBy(df1.sname)

    b.show()


    print("## Q31. Find all salespeople whose name starts with ‘P’ and the fourth character is ‘I’. ##")

    a = df1.filter((col("sname").startswith("P")) & (col("sname").substr(4, 1) == "i"))

    a.show()

    a = df1.filter((col("sname").startswith("M")) & (col("sname").substr(4, 1) == "i"))

    a.show()


    print("## Q32. Write a query that uses a sub query to obtain all orders for the customer named ‘Cisneros’. Assume you do not know his customer number. ##")

    a = df2.join(df3,"cnum","inner")

    b = df2.filter(df2.cname == "Cisneros").select(df2.cnum).first()[0]

    c = a.filter(df3.cnum == b).select(df3.cnum,df2.cname,df3.onum).orderBy(df3.cnum)

    c.show()

    print("## Q33. Find the largest orders for ‘Serres’ and ‘Rifkin’. ##")

    a = df1.join(df3, "snum", "inner")

    b = a.filter((df1.sname == "Serres") | (df1.sname == "Rifkin")).select(df1.snum)

    c = a.join(b,"snum","inner").groupBy(df1.snum,df1.sname).agg(max(df3.amt).alias("max amt")).select(df1.snum,df1.sname,"max amt").show()



    print("## Q34. # Extract the Salespeople table in the following order: SNUM, SNAME, COMMISSION, CITY. ##")

    df1.select("snum","sname","comm","city").show()


    print("## Q35. # Select all customers whose names fall in between ‘A’ and ‘G’ alphabetical range. ##")

    a = df2.filter((df2.cname >= "A") & (df2.cname <= "G"))

    a.show()

    print("## Q36.  Select all the possible combinations of customers that you can assign. ##")

    df2.crossJoin(df2).show()

    print("## Q37. Select all orders that are greater than the average for October 4. ##")

    a = df3.filter(df3.odate == "04-Oct-96")
    b = a.agg(avg(df3.amt)).first()[0]

    df3.filter(df3.amt>b).select(df3.snum,df3.odate,df3.amt).orderBy(df3.snum).show()


    print("## Q38. Write a select command using a correlated sub query that selects the names and numbers of all customers with ratings equal to the maximum for their city. ##")

    a = df2.groupby(df2.city).agg(max(df2.rating).alias("rating")).select(df2.city.alias("city_max"), "rating")

    b = df2.join(a, (df2.city == a.city_max) & (df2.rating == a.rating), "inner").select(df2.cnum, df2.cname, df2.city,df2.rating).orderBy(df2.rating)

    b.show()


    print("## Q39. Write a query that totals the orders for each day and places the results in descending order. ##")

    df3.groupBy(df3.odate).agg(sum(df3.amt).alias("Total Amt")).select(df3.odate,"Total Amt").orderBy(desc(df3.odate)).show()


    print("## Q40. Write a select command that produces the rating followed by the name of each customer in San Jose. ##")

    df2.filter(df2.city == "San Jose").select(df2.cname,df2.rating).show()


    print("## Q41. Find all orders with amounts smaller than any amount for a customer in San Jose. ##")

    a = df2.join(df3, "snum", "inner")

    b = a.filter(df2.city == "San Jose").select(max(df3.amt)).first()[0]


    c = a.filter(df3.amt<b).select(df3.onum, df3.odate, df3.cnum, df3.amt).distinct().orderBy(df3.odate)

    c.show()


    print("## Q42. Write a query that calculates the amount of the salesperson’s commission on order by a customer with a rating above 100.00. ##")

    a = df1.join(df2,"snum","inner")
    b = a.join(df3,"snum","inner")

    c = b.filter(df2.rating > 100).select(df1.snum,df1.sname,df3.cnum,df3.odate,(df3.amt*df1.comm).alias("Commision Amt")).orderBy(df1.snum)

    c.show()


    print("## Q43. Write a query that produces all pairs of salespeople with themselves as well as duplicate rows with the order reversed. ##")

    a = df1.alias("a").join(df1.alias("b"), "snum").select("a.snum", "a.sname", "b.snum", "b.sname")

    a.show()

    print("## Q44. Find all salespeople that are located in either Barcelona or London. ##")

    df1.filter((df1.city == "Barcelona") | (df1.city == "London")).show()


    print("## Q45. Find all salespeople with only one customer. ##")

    a = df1.join(df2,"snum","inner")

    b = a.groupBy("snum", "sname", "comm").agg(count("cnum").alias("customer_count")).filter(col("customer_count") == 1)

    b.show()


    print("## Q46. Write a query that joins the Customer table to itself to find all pairs of customers served by a single salesperson.. ##")

    a = df2.alias("a").join(df2.alias("b"), col("a.snum") == col("b.snum"), "inner")

    b = a.filter(col("a.cnum") < col("b.cnum"))
    b.select(col("a.cnum").alias("customer1"), col("a.cname").alias("customer1_name"),
                col("b.cnum").alias("customer2"), col("b.cname").alias("customer2_name"),
                col("a.snum").alias("salesperson"))

    b.show()


    print("## Q47. Write a query that will give you all orders for more than $1000.00. ##")

    df3.filter(col("amt") > 1000.00).show()


    print("## Q48. Write a query that lists each order number followed by the name of the customer who made that order. ##")

    df3.join(df2, "cnum").select("onum", "cname")



    print("## Q49. Write 2 queries that select all salespeople (by name and number) who have customers in their cities who they do not service, one using a join and one a correlated subquery. Which solution is more elegant?. ##")

    df = df1.join(df2,'city','inner').filter(df1.snum != df2.snum)

    df.show()

    print("## Q50. Write a query that selects all customers whose ratings are equal than ANY (in the SQL sense) of Serres. ##")

    a = df2.where(col("rating") == any(df2.alias("c").select("rating").where(col("cname") == "Serres").collect()))

    a.show()

    print("## Q51. Write2 queries that will produce all orders taken on October3 or October 4. ##")

    a = df3.where((col("odate") == "03-Oct-96") | (col("odate") == "04-Oct-96"))

    a.show()

    print("## Q52. Write a query that produces all pairs of orders by a given customer. Name that customer and eliminate duplicates. ##")

    a = df3.join(df2, "cnum", "inner").groupBy("cname", "onum").agg(count("onum"))

    a.show()

    print("## Q53. Find only those customers whose ratings are higher than every customer in Rome. ##")

    a = df2.filter(df2.city == "Rome").groupBy(df2.cnum, df2.rating).agg(max(df2.rating)).select(df2.rating).collect()[0][0]
    print(a)

    df2.filter(df2.rating > a).show()

    print("## Q54. Write a query on the customers table whose output will exclude all customers with a rating < = 100.00, unless they are located in Rome.. ##")

    a = df2.where(~((col("rating") <= 100) & (col("city") == "Rome")))

    a.show()

    print("## Q55. Find all rows from the Customer table for which the salesperson number is 1001. ##")


    a = df2.filter('SNUM == 1001')

    a.show()


    print("## Q56. Find the total amount in Orders for each salesperson for which this total is greater than the amount of the largest order in the table.. ##")

    a = df3.agg(max('amt').alias('max_amt')).collect()[0][0]

    b = df3.groupBy('snum').sum('amt').filter(col('sum(amt)') > a)

    b.show()


    print("## Q57. Write a query that selects all orders that have Zeroes or NULL in the Amount field. ##")

    a = df3.filter((df3.amt.isNull()) | (df3.amt == '0'))

    a.show()

    print("## Q58. Produce all combinations of salespeople and customer names such that the former precedes the latter alphabetically, and the latter has a rating of less than 200.  ##")

    a = df1.crossJoin(df2).filter((col('sname') < col('cname')) & (col('rating') < 200))

    a.show()


    print("## Q59. List all salespeople’s names and the commission they have earned. ##")

    a = df1.join(df3, "snum", "inner")

    b = a.groupby(df1.sname, df1.comm).agg(sum(df3.amt).alias("Total amt"))

    b.select(df1.sname, "Total amt", df1.comm, (col("Total amt") * df1.comm).alias("Commision earned")).show()


    print("## Q60. Write a query that produces the names and cities of all customers with the same rating as Hoffman. Write the query using Hoffman’s CNUM rather than his rating, so that it would still be usable if his rating changed. ##")

    a = df2.select('rating').filter(col('CNAME') =='Hoffman').collect()[0][0]

    b = df2.select('CNAME','CITY').filter(col('rating')== a)

    b.show()


    print("## Q61. Find all salespeople for whom there are customers that follow them in alphabetical order. ##")

    a = df2.select('*').filter(df2.cnum.isNotNull()).orderBy(df2.cname)

    a.show()


    print("## Q62. Write a query that produces the names and ratings of all customers of all who have above average orders. ##")

    a = df3.agg(avg('amt')).collect()[0][0]

    b = df2.join(df3,'cnum','inner')

    c = b.filter(col('amt') >a).select('cname','rating')

    c.show()



    print("## Q63. Find the sum of all purchases from the Orders table. ##")

    df3.agg(sum('amt')).show()


    print("## Q64. Write a select command that produces the order number, amount, and date for all rows in the Order table. ##")

    df3.select("onum","amt","odate").show()


    print("## Q65. Count the number of not null rating fields in the Customer table including duplicates. ##")

    df2.select(count(col("rating"))).show()


    print("## Q66. Write a query that gives the names of both the salesperson and the customer for each order after the order number. ##")

    df3.join(df1, "snum").join(df2, "cnum").select("onum", "sname", "cname").show()


    print("## Q67 List the commissions of all salespeople servicing customers in London.")

    df1.join(df2,'SNUM','leftsemi').filter(col('city')== 'London').show()


    print("## Q68 Write a query using ANY or ALL that will find all salespeople who have no customers located in their city.")

    # df1.filter(~any(df2.filter(col("city") == col("sname")))).show()
    #
    # df1.filter(all(df2.filter(col("city") != col("sname")))).show()


    print("## Q69 Write a query using the EXISTS operator that selects all salespeople with customers located in their cities who are not assigned to them.")

    # df1.filter(exists(df2.filter((col("city") == col("sname")) & (col("snum") != col("snum"))))).show()

    print("## Q70 Write a query that selects all customers serviced by Peel or Motika.")

    df1.join(df2,'Snum','inner').filter((col('SNAME') == 'Peel') |( col('Sname') == 'Monika')).show()

    print("## Q71 Count the number of salespeople registering orders for each day. (If a salesperson has more than one order on a given day, he or she should be counted only once.).")

    a = df3.groupBy('Snum').count()

    b = df1.join(a,'snum','leftsemi')

    b.show()


    print("## Q72 Find all orders attributed to salespeople in London.")

    df1.join(df3, 'snum', 'inner').filter(col('city') == 'London').show()


    print("## Q73 Find all orders by customers not located in the same cities as their salespeople.")

    a = df3.join(df2, "cnum").join(df1, "snum")

    b = a.filter(df2.city != df1.city)

    b.show()

    print("## Q74 Find all salespeople who have customers with more than one current order.")

    df3.groupBy('SNUM').agg(count('CNUM').alias('total_cust')).filter(col('total_cust') > 1).show()

    print("## Q75 Write a query that extracts from the Customer table every customer assigned to a salesperson who currently has at least one other customer (besides the customer being selected) with orders in the Orders table.")


    print("## Q76 Write a query that selects all customers whose name begins with ‘C’.")

    df2.filter(col('CNAME').like('C%')).show()

    print("## Q77 Write a query on the Customers table that will find the highest rating in each city. Put the output in this form:  for the city (city) the highest rating is : (rating).")

    a = df2.groupBy("city").agg(max("rating").alias("highest_rating"))

    b = a.withColumn("output",concat(lit("For the city "), col("city"),lit(" the highest rating is : "), col("highest_rating")))

    b.select("output").show()

    print("## Q78 Write a query that will produce the Snum values of all salespeople with orders currently in the Orders table without any repeats.")

    df3.select('SNUM').distinct().show()


    print("## Q79 Write a query that lists customers in descending order of rating. Output the rating field first, followed by the customers’ names and numbers.")

    df2.select('RATING', 'CNAME', 'CNUM').orderBy(col('CNUM').desc()).show()

    print("## Q80 Find the average commission for salespeople in London.")

    df1.groupBy('city').avg('comm').filter(col('CITY') == 'London').show()


    print("## Q81 Find all orders credited to the same salesperson that services Hoffman.")

    a = df2.select('SNUM').filter(col('CNAME') == 'Hoffman').collect()[0][0]

    b = df3.filter(col('SNUM') == a)

    b.show()


    print("## Q82 Find all salespeople whose commission is in between 0.10 and 0.12 both inclusive.")

    df1.filter(col('comm').between(0.10, 0.12)).show()


    print("## Q83 Write a query that will give you the names and cities of all salespeople in London with commission above 0.10")

    df1.select('SNAME', 'CITY', ).filter((col('CITY') == 'London') & (col('comm') == 0.10)).show()


    print("## Q84 SELECT * FROM ORDERS WHERE (AMT < 1000 OR NOT (ODATE = 10/03/1996 AND CNUM > 2003));")

    print("## Q85 Write a query that selects each customer’s smallest order.")

    df3.groupBy('SNUM').min('AMT').show()


    print("## Q86 Write a query that selects the first customer in alphabetical order whose name begins with ‘G’.")

    df2.select('CNAME').filter(col('CNAME').like('G%')).orderBy('CNAME').show()


    print("## Q87 Write a query that counts the number of different not NULL city values in the Customers table.")

    a = df2.filter(col('CITY').isNull())
    b = a.count()

    print("Number of NOT NULL city values :-",b)


    print("## Q88 Find the average amount from the Orders table.")

    df3.agg(avg(df3.amt).alias("AVERAGE AMOUNT FROM ORDERS TABLE")).select("AVERAGE AMOUNT FROM ORDERS TABLE").show()

    #
    # print("## Q89 What would be the output from the following query?SELECT*FROM ORDERS WHERE NOT  (Odate =10/03/1996 OR Snum>1006) AND amt. >=1500);")



    print("## Q90 Find all customers who are not located in San Jose & whose rating is above 200.")

    df2.filter((df2.city != "San Jose") &(df2.rating > 200)).show()


    print("## Q91 Give a simpler way to write this query: SELECT Snum, Sname, city, Comm FROM salespeople WHERE (Comm>0.12 and Comm <0.14);")

    df1.select('snum', 'sname', 'city', 'comm').filter(col('comm').between(0.12, 0.14)).show()


    print("## Q92 Evaluate the following query:SELECT * FROM orders WHERE NOT ((Odate = 10/03/1996 AND Snum>1002) OR amt>2000);")


    print("## Q93 Which salespeople attend to customers not in the city they have been assigned to?")

    a = df2.join(df1, ((df1.snum == df2.snum) & (df1.city != df2.city)), 'inner').select(df1.sname, df1.snum, df1.city, df2.cname,df2.city).distinct()

    a.show()


    print("## Q94 Which salespeople get commission greater than 0.11 and serving customers rated less than 250?")

    a = df1.join(df2,"snum","inner")

    b = a.filter((df1.comm > 0.11) & (df2.rating < 250)).select(df1.snum,df1.sname,df1.comm,df2.rating).orderBy(df1.snum)

    b.show()


    print("## Q95 Which salespeople have been assigned to the same city but get different commission percentages?")

    df1.alias('s1').join(df1.alias('s2'), on='SNUM', how='inner').filter((col('s1.city') == col('s2.city')) & (col('s1.comm') != col('s2.comm'))).show()


    print("## Q96 Which salesperson has earned the most by way of commission?")

    a = df1.join(df3, "snum", "inner")

    b = a.groupby(df1.snum, df1.sname, df1.comm).agg(max(df3.amt).alias("max"))

    c = b.select(df1.snum, df1.sname, (df1.comm * col("max")).alias("Commision"))

    windowspec = Window.orderBy(col('Commision').desc())

    c.withColumn('row', row_number().over(windowspec)).filter(col('row') == 1).show()

    print("OR")

    windowspec = Window.orderBy(col('comm').desc())

    df1.withColumn('row',row_number().over (windowspec)).filter(col('row') == 1 ).show()


    print("## Q97 Does the customer who has placed the maximum number of orders have the maximum rating?")

    m = Window.orderBy(col('rating').desc())

    a = df2.withColumn('row',row_number().over(m)).filter(col('row') == 1 ).select('CNUM').collect()[0][0] # we get cnum for max rating

    n = Window.orderBy(col('amt').desc())

    b = df3.withColumn('row',row_number().over(n)).filter(col('row') == 1 ).select('CNUM').collect()[0][0] # cnum for max amount

    if a == b:
        print('The customer who has place the maximun number of orders have  maximum rating')
    else:
        print('The customer who has place the maximum number of orders have not maximum rating ')


    print("## Q98 Has the customer who has spent the largest amount of money been given the highest rating?")

    m = Window.orderBy(col('rating').desc())

    a = df2.withColumn('row',row_number().over(m)).filter(col('row') == 1 ).select('CNUM').collect()[0][0] # we get cnum for max rating

    n = Window.orderBy(col('amt').desc())

    b = df3.withColumn('row',row_number().over(n)).filter(col('row') == 1 ).select('CNUM').collect()[0][0] # cnum for max amount

    if a == b:
        print('The customer who has spent the largest amount of money been given the highest rating')
    else:
        print('The customer who has spent the largest amount of money been NOT given the highest rating ')


    print("## Q99 List all customers in descending order of customer rating.")

    df2.select('*').orderBy(col('rating').desc()).show()



    print("## Q100 On which days has Hoffman placed orders?")

    a = df2.select('Cnum').filter(col('CNAME') == 'Hoffman').collect()[0][0] # also use first()[0] / first() =collect()[0]

    b = df3.select('odate').filter(col('CNUM')== a)

    b.show()



    print("## Q101 Do all salespeople have different commissions?")

    a = df1.select('comm').count()
    b = df1.select('comm').distinct().count()

    if a == b:
        print('All salespeople have same commission')

    else:
        print('Some salespeople have different commission')


    print("## Q102 Which salespeople have no orders between 10/03/1996 and 10/05/1996?")

    df3.select('SNUM').filter((col('ODATE') < '03-10-1996') & (col('odate') > '05-10-1996')).show()


    print("## Q103 How many salespersons have succeeded in getting orders?")

    df3.select('SNUM').distinct().agg(count('SNUM')).show()


    print("## Q104 How many customers have placed orders?")

    df3.select('CNUM').distinct().agg(count('CNUM')).show()


    print("## Q105 On which date has each salesperson booked an order of maximum value?")

    df3.groupBy('SNUM', 'ODate').max('amt').show()


    print("## Q106 Who is the most successful salesperson?")

    a = df1.join(df3, "snum", "inner")

    b = a.groupBy("snum", "sname").agg(max(df3.amt).alias("max"))

    window_spec = Window.orderBy(col("max").desc())

    c = b.withColumn("row", row_number().over(window_spec)).filter(col("row") == 1).select("snum", "sname","max")

    c.show()


    print("## Q107 Who is the worst customer with respect to the company?")

    a = df1.join(df3, "snum", "inner")

    b = a.groupBy("snum", "sname").agg(min("amt").alias("min_amt"))

    window_spec = Window.orderBy(b.min_amt)

    c = b.withColumn("row", row_number().over(window_spec)).filter(col("row") == 1).select("snum","sname","min_amt")

    c.show()


    print("## Q108 Are all customers not having placed orders greater than 200 totally been serviced by salesperson Peel or Serres?")

    a = df1.join(df3,"snum","inner")

    b = a.filter(df3.amt < 200).filter((df1.sname == "Peel") | (df1.sname == "Serres"))

    b.show()

    print("## Q109 Which customers have the same rating?")

    a = df2.alias("a").join(df2.alias("b"),"rating","inner")

    b = a.filter((col("a.cname") < col("b.cname")))

    c = b.select("a.cname", "a.rating","b.cname", "b.rating")

    c.show()



    print("## Q110 Find all orders greater than the average for October 4th.")



    a = df3.groupBy('odate').avg('amt').filter(col('odate') == "04-Oct-96").select('avg(amt)').collect()[0][0]

    df3.select('*').filter(col('amt') > a).show()


    print("## Q111 Which customers have above average orders?")

    a= df3.agg(avg('amt')).collect()[0][0]

    b= df3.filter(col('amt') > a)

    b.show()


    print("## Q112 List all customers with ratings above San Jose’s average.")

    a = df2.groupby("city").avg('rating').filter(col('city') == "San Jose").select('avg(rating)').collect()[0][0]

    df2.select('*').filter(col('rating') > a).show()



    print("## Q113 Select the total amount in orders for each salesperson for which the total is greater than the amount of the largest order in the table.")

    a = df3.agg(max('amt')).collect()[0][0]

    b = df3.groupBy('snum').sum('amt').filter(col('sum(amt)')>a)

    b.show()



    print("## Q114 Give names and numbers of all salesperson that have more than one customer.")

    a = df1.join(df3,"snum","inner")

    b = a.groupby(df1.sname,df3.snum).agg(count(df3.snum).alias("count")).filter(col("count")>1).select(df3.snum,df1.sname,"count").orderBy(df3.snum)

    b.show()



    print("## Q115 Select all salesperson by name and numbers who have customers in their city whom they don’s the service.")


    a = df2.join(df1, ((df1.snum == df2.snum) & (df1.city != df2.city)), 'inner').select(df1.sname, df1.snum, df1.city, df2.cname,df2.city).distinct()
    a.show()

    print("REVERSE OF ABOVE QUE")

    a = df2.join(df1, 'city', 'inner').filter(df2.snum != df1.snum).select(df1.sname, df1.snum, df1.city, df2.cname,df2.city)

    a.show()

    print("## Q116 Which customers’ rating should be lowered?")



    print("## Q117 Is there a case for assigning a salesperson to Berlin?")



    print("## Q118Is there any evidence linking the performance of a salesperson to commission that he or she is being paid?")



    print("## Q119 Dose the total amount in orders by customer in Rome and London exceeds the commission paid to salesperson in London and New York by more than 5 times?")



    print("## Q120 Which is the date, order number, amt and city for each salesperson (byname) for  the maximum order he has obtained?")

    a = df3.join(df1,'SNUM','inner')
    b = a.select(df3.odate,df3.onum,df3.amt,df1.city,df1.sname)
    c = b.groupBy(df3.odate,df3.onum,df3.amt,df1.city,df1.sname).agg(max(df3.amt))
    c.show()


    print("## Q121 Which salesperson(s) should be fired?")

    df3.groupBy('snum').sum('amt').orderBy(col('sum(amt)')).limit(1).select('snum').show()



    print("## Q122 What is the total income for the company?")

    df3.agg(sum('amt').alias("TOTAL INCOME FOR COMPANY")).show()



    








