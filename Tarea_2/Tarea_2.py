from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

df = spark.read.json("compras_1.json")

# Show the DataFrame schema
df.printSchema()

# Show the first few rows of the DataFrame
df.show()