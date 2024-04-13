from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

df = spark.read.option("multiline","true").json("compras_1.json")

# Show the DataFrame schema
df.printSchema()

# Show the first few rows of the DataFrame
df.show(truncate=False)