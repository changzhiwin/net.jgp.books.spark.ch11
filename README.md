# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch11

# Environment
- Java 8
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch11_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
// common jar, need --jars option
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/scala-logging_2.13-3.9.4.jar \
  target/scala-2.13/main-scala-ch11_2.13-1.0.jar
```

## 3, print

### Case: simpleSql
```
root
 |-- geo: string (nullable = true)
 |-- yr1980: double (nullable = true)

+---------------------------------+-------+
|geo                              |yr1980 |
+---------------------------------+-------+
|Falkland Islands (Islas Malvinas)|0.002  |
|Niue                             |0.002  |
|Saint Pierre and Miquelon        |0.00599|
|Saint Helena                     |0.00647|
|Turks and Caicos Islands         |0.00747|
|Nauru                            |0.00771|
|Virgin Islands, British          |0.011  |
|Montserrat                       |0.01177|
|Cayman Islands                   |0.01708|
|Cook Islands                     |0.01801|
+---------------------------------+-------+
```

### Case: sqlAndApi
```
+-------------------------+--------+--------+----------+
|geo                      |yr1980  |yr2010  |evolution |
+-------------------------+--------+--------+----------+
|Bulgaria                 |8.84353 |7.14879 |-1694740.0|
|Hungary                  |10.71112|9.99234 |-718780.0 |
|Romania                  |22.13004|21.95928|-170760.0 |
|Guyana                   |0.75935 |0.74849 |-10860.0  |
|Montserrat               |0.01177 |0.00512 |-6650.0   |
|Cook Islands             |0.01801 |0.01149 |-6520.0   |
|Netherlands Antilles     |0.23244 |0.22869 |-3750.0   |
|Dominica                 |0.07389 |0.07281 |-1080.0   |
|Saint Pierre and Miquelon|0.00599 |0.00594 |-50.0     |
+-------------------------+--------+--------+----------+

+-----------------------+----------+----------+------------+
|geo                    |yr1980    |yr2010    |evolution   |
+-----------------------+----------+----------+------------+
|World                  |4451.32679|6853.01941|2.40169262E9|
|Asia & Oceania         |2469.81743|3799.67028|1.32985285E9|
|Africa                 |478.96479 |1015.47842|5.3651363E8 |
|India                  |684.8877  |1173.10802|4.8822032E8 |
|China                  |984.73646 |1330.14129|3.4540483E8 |
|Central & South America|293.05856 |480.01228 |1.8695372E8 |
|North America          |320.27638 |456.59331 |1.3631693E8 |
|Middle East            |93.78699  |212.33692 |1.1854993E8 |
|Pakistan               |85.21912  |184.40479 |9.918567E7  |
|Indonesia              |151.0244  |242.96834 |9.194394E7  |
|United States          |227.22468 |310.23286 |8.300818E7  |
|Brazil                 |123.01963 |201.10333 |7.80837E7   |
|Nigeria                |74.82127  |152.21734 |7.739607E7  |
|Europe                 |529.50082 |606.00344 |7.650262E7  |
|Bangladesh             |87.93733  |156.11846 |6.818113E7  |
+-----------------------+----------+----------+------------+

22/06/10 11:15:37 INFO SimpleSelect$: Territories in orginal dataset: 232
22/06/10 11:15:38 INFO SimpleSelect$: Territories in cleaned dataset: 224

+-------------+---------+----------+-----------+
|geo          |yr1980   |yr2010    |evolution  |
+-------------+---------+----------+-----------+
|China        |984.73646|1330.14129|3.4540483E8|
|India        |684.8877 |1173.10802|4.8822032E8|
|United States|227.22468|310.23286 |8.300818E7 |
|Indonesia    |151.0244 |242.96834 |9.194394E7 |
|Brazil       |123.01963|201.10333 |7.80837E7  |
|Pakistan     |85.21912 |184.40479 |9.918567E7 |
|Bangladesh   |87.93733 |156.11846 |6.818113E7 |
|Nigeria      |74.82127 |152.21734 |7.739607E7 |
|Russia       |null     |139.39021 |null       |
|Japan        |116.80731|126.80443 |9997120.0  |
|Mexico       |68.34748 |112.46886 |4.412138E7 |
|Philippines  |50.94018 |99.90018  |4.896E7    |
|Vietnam      |53.7152  |89.57113  |3.585593E7 |
|Ethiopia     |38.6052  |88.01349  |4.940829E7 |
|Germany      |null     |82.28299  |null       |
+-------------+---------+----------+-----------+
only showing top 15 rows
```

## 4, Some diffcult case

### What difference between StructType and DataType?
- show many case using StructType, [here](https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/)
```
  def notPractice(df: DataFrame): Unit = {
    val updatedDF = df.withColumn("OtherInfo", 
      struct(
        col("id").as("identifier"),
        col("gender").as("gender"),
        col("salary").as("salary"),
        when( col("salary").cast(IntegerType) < 2000, "Low").
        when( col("salary").cast(IntegerType) < 4000, "Medium").
        otherwise("High").alias("Salary_Grade")
      )
    ).drop("id","gender","salary")

    updatedDF.printSchema()
    updatedDF.show(false)
  }
```

### Download jar
When I use [scala-logging](https://github.com/lightbend/scala-logging), I need download jar file.
> https://mvnrepository.com/