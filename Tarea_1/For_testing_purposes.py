from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)

spark = SparkSession.builder.appName("Read Transactions").getOrCreate()

expected_ds = spark.createDataFrame([(980,1,70.5,'John Lennon','Computacion',3),(325,1,85.4,'John Lennon','Computacion',2),(980,2,55.4,'Paul McCartney','Computacion',3),
                                                  (725,3,95.5,'Ringo Starr', 'Computacion',4),(589,4,45.4,'George Harrison','Fisica',2),(589,4,89.3,'George Harrison','Fisica',2)],\
                                                  ['Codigo de Curso','Numero de Carnet','Nota','Nombre Completo','Carrera','Credito'])
expected_ds.show()