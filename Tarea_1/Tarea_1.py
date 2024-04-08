from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, udf, rank
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
                                StructField('Carrera_c',StringType())])
                                
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

# df_joined_1= nota_df.join(estudiantes_df,on='Numero de Carnet', how='left')
# #df_joined_1.summary().show()
# df_joined_1.show()

# #Ahora se hace un left join entre el primer join y curso para obtener los creditos

# df_joined_2=df_joined_1.join(curso_df,on='Codigo de Curso',how='left')
# #df_joined_2.summary().show()
# df_joined_2.show()

def unir_datos(nota,estudiantes,curso):
    '''
    La funcion recibe Tres dataframes y devuelve la union de los 3.
    Se asume que los estudiantes listados en el Dataset 'estudiantes_df' pero no en el dataset 'nota_df' no matricularon cursos
    en el periodo de referencia y por ende no se toman en cuenta para la obtencion de los mejores promedios.
    
    Args:
    nota: dataframe
    estudiante: dataframe
    curso: dataframe
    
    Return:
    segundo_join_df: La union de los tres dataframes  
    '''
    
    primer_join_df = nota_df.join(estudiantes_df,on = 'Numero de Carnet', how = 'left')
    segundo_join_df = primer_join_df.join(curso_df,on = 'Codigo de Curso',how = 'left')

    return segundo_join_df
    
df_joined_2 = unir_datos(nota_df,estudiantes_df,curso_df)
df_joined_2.show()



# nota_ponderada_df = df_joined_2.withColumn('nota_ponderada', col('Nota') * col('Credito')).drop('Carrera_c','Codigo de Curso','Nota','Numero de Carnet')
# nota_ponderada_df.show()
# nota_ponderada_df.printSchema()

# agrupar_por_estudiante_df = nota_ponderada_df.groupBy("Nombre Completo", "Carrera").sum()
# agrupar_por_estudiante_df.show()

# agrupar_por_estudiante_sumas_df = \
    # agrupar_por_estudiante_df.select(
        # col('Nombre Completo'),
        # col('Carrera'),
        # col('sum(Credito)').alias('Credito'),col('sum(nota_ponderada)').alias('nota_ponderada'))
# agrupar_por_estudiante_sumas_df.show()

# promedio_poderado_df=agrupar_por_estudiante_sumas_df.withColumn("promedio_ponderado", col('nota_ponderada') / col('Credito')).drop('Credito', 'nota_ponderada')
# promedio_poderado_df.show()

def agregaciones_parciales(df):
    '''La funcion recibe un dataframe creado por la funcion 'unir datos' y devuelve un nuevo dataframe que contiene datos correspondientes a 
    Nombre Completo, Carrera y Promedio Ponderado para cada estudiante
    
    Args:
    df: dataframe
    
    Return:
    
    promedio ponderado: dataframe
    '''
    nota_ponderada = df.withColumn('nota_ponderada', col('Nota') * col('Credito')).drop('Carrera_c','Codigo de Curso','Nota','Numero de Carnet')
    agrupar_por_estudiante= nota_ponderada.groupBy("Nombre Completo", "Carrera").sum()
    agrupar_por_estudiante_sumas = \
    agrupar_por_estudiante.select(
        col('Nombre Completo'),
        col('Carrera'),
        col('sum(Credito)').alias('Credito'),col('sum(nota_ponderada)').alias('nota_ponderada'))
    promedio_ponderado = agrupar_por_estudiante_sumas.withColumn("promedio_ponderado", col('nota_ponderada') / col('Credito')).drop('Credito', 'nota_ponderada')
   
    return promedio_ponderado
    
promedio_ponderado_df = agregaciones_parciales(df_joined_2)
promedio_ponderado_df.show()
    

    

# particion_carrera_df = Window.partitionBy("Carrera").orderBy(col("promedio_ponderado").desc())

# rankin_df = promedio_ponderado_df.withColumn("rank",rank().over(particion_carrera_df))

# mejores_dos_promedios_carrera = rankin_df.filter(col("rank") <= 2).drop("rank")

# mejores_dos_promedios_carrera.show()


def resultados_finales(df):
    '''La funcion recibe el dataframe generado en la funcion agregaciones parciales y devuelve los dos mejores promedios por cada carrera
    Args:
    df: dataframe
    
    Return:
    
    mejores_dos_promedios_carrera: dataframe
    
    '''
    particion_carrera = Window.partitionBy("Carrera").orderBy(col("promedio_ponderado").desc())
    rankin_df = df.withColumn("rank",rank().over(particion_carrera))
    mejores_dos_promedios_carrera = rankin_df.filter(col("rank") <= 2).drop("rank")
    mejores_dos_promedios_carrera_renombrada = mejores_dos_promedios_carrera.withColumnRenamed('promedio_ponderado','Mejores_promedios')
    
    return mejores_dos_promedios_carrera_renombrada
    
mejores_dos_promedios_carrera_df = resultados_finales(promedio_ponderado_df)
mejores_dos_promedios_carrera_df.show()
