from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Tarea_2").getOrCreate()

schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio_unitario", FloatType(), True)
])

df = spark.read.option("multiline","true").json("compras_1.json",schema=schema)

# Show the DataFrame schema
df.printSchema()

# Show the first few rows of the DataFrame
df.show()