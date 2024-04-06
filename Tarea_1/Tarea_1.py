from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)

spark = SparkSession.builder.appName("Tarea_1").getOrCreate()

estudiantes_schema = StructType([StructField('Numero de Carnet',IntegerType()),
                                StructField('Nombre Completo',StringType()),
                                StructField('Carrera',StringType())])
                                

estudiantes_df = spark.read.csv('estudiante.csv',
                                    schema=estudiantes_schema,
                                    header=False)
                                    
estudiantes_df.printSchema()
estudiantes_df.show()
                                    
curso_schema = StructType([StructField('Codigo de Curso',IntegerType()),
                                StructField('Credito',IntegerType()),
                                StructField('Carrera',StringType())])
                                
curso_df = spark.read.csv('curso.csv',
                                    schema=curso_schema,
                                    header=False)
curso_df.printSchema()
curso_df.show()
                                    
nota_schema = StructType([StructField('Numero de Carnet',IntegerType()),
                                StructField('Codigo de Curso',IntegerType()),
                                StructField('Nota',FloatType())])
                                
nota_df = spark.read.csv('nota.csv',
                                    schema=nota_schema,
                                    header=False)
                                    
nota_df.printSchema()
nota_df.show()

##Joins##

##Primero se hace un left join entre Nota y Estudiante. Esto implica que los estudiantes que aparecen el df 'estudiante' pero no no matricularan curso dentro del periodo de referencia no aparecen en el df 'Nota' y por tanto no son considerados

df_joined_1= nota_df.join(estudiantes_df,on='Numero de Carnet', how='left')
df_joined_1.summary.show()