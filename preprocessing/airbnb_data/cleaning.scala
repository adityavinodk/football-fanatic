//This code is run in the Spark shell, which is also interactive and therefore easy to work with

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType}
import org.apache.spark.sql.functions.{col, regexp_replace, udf}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.to_date

val schema = StructType(Array(StructField("id", IntegerType, false),
 StructField("listing", StringType, false),
 StructField("desc", StringType, false),
 StructField("price", StringType, false),
 StructField("city", StringType, false),
 StructField("country", StringType, false),
 StructField("info_date", StringType, false),
 StructField("checkin_date", StringType, false),
 StructField("checkout_date", StringType, false)))

//Reverse the price string to get the last number, and grab only that number
//.map(_.toInt)
val extractPriceUDF = udf((priceStr: String) => {
  "\\$\\s*(\\d+)".r.findAllIn(priceStr).matchData.toList.reverse.headOption.map(_.group(1))})

val df_newline = spark.read.schema(schema).option("header", "false").option("multiline","true").csv("/user/mjd9571_nyu_edu/project/airbnb_listings_new.csv")
val df_newline2 = df_newline.withColumn("price", regexp_replace(col("price"), "\n", ""))
val df = df_newline2.withColumn("desc", regexp_replace(col("desc"), "\n", ""))
df.show(false)
//Old DataFrame that read the data with the newlines pre cleaned out
//val df = spark.read.schema(schema).option("header", "true").csv("/user/mjd9571_nyu_edu/project/airbnb_listings_new.csv")
val df2 = df.na.drop()
val df3 = df2.withColumn("checkout_date", split(col("checkout_date"), " ").getItem(0))
val df4 = df3.withColumn("price", extractPriceUDF(col("price")))
val df5 = df4.withColumn("unique_id", monotonically_increasing_id())
df5.select("unique_id", "id").show()
val id_count = df5.groupBy(col("id")).count()
val duplicate_id = id_count.filter(col("count") > 1)
//duplicate_id.show()
val unique_id_count = df5.groupBy(col("unique_id")).count()
val duplicate_unique_id = unique_id_count.filter(col("count") > 1)
//duplicate_unique_id.show()
val rows = df5.count()
//print(rows)
val columns = df5.columns
val df6 = df5.na.drop()
val df7 = df6.withColumn("checkin_date_trimmed", trim($"checkin_date")).withColumn("checkin_date", to_date($"checkin_date_trimmed", "yyyy-MM-dd")).drop("checkin_date_trimmed")
val df8 = df7.withColumn("checkout_date_trimmed", trim($"checkout_date")).withColumn("checkout_date", to_date($"checkout_date_trimmed", "yyyy-MM-dd")).drop("checkout_date_trimmed")
val df9 = df8.withColumn("city",lower($"city"))
val df10 = df9.drop("id")
df10.printSchema()
df10.show()
val output_path = "/user/mjd9571_nyu_edu/project/airbnb_listings_new_cleaned.csv"
df10.write.format("csv").option("header", "true").save(output_path)