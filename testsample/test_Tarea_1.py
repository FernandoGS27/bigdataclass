from .Tarea_1 import unir_datos, agregaciones_parciales, resultados_finales

def test_unir_datos(spark_session):

    nota_data = [(1,980,70.5), (1,325,85.4),(2,980,55.4),(3,725,95.5), (4,589,45.4), (4,589,89.3)]
    nota_ds = spark_session.createDataFrame(nota_data, ['Numero de Carnet','Codigo de Curso','Nota'])
    
    estudiantes_data = [(1,'John Lennon', 'Computacion'),(2,'Paul McCartney', 'Computacion'), (3, 'Ringo Starr', 'Computacion'),(4,'George Harrison','Fisica'),(5,'Pete Best','Fisica')]
    estudiantes_ds = spark_session.createDataFrame(estudiantes_data, ['Numero de Carnet','Nombre Completo','Carrera'])
    
    curso_data = [(980,3,'Computacion'),(325,2,'Computacion'),(589,2,'Fisica'),(725,4,'Computacion')]
    curso_ds = spark_session.createDataFrame(curso_data,['Codigo de Curso','Credito','Carrera_c'])
    
    actual_ds = unir_datos(nota_ds,estudiantes_ds,curso_ds)
    
    expected_ds = spark_session.createDataFrame([(980,1,70.5,'John Lennon','Computacion',3),(325,1,85.4,'John Lennon','Computacion',2),(980,2,55.4,'Paul McCartney','Computacion',3),\
                                                  (725,3,95.5,'Ringo Starr', 'Computacion',4),(589,4,45.4,'George Harrison','Fisica',2),(589,4,89.3,'George Harrison','Fisica',2)],\
                                                  ['Codigo de Curso','Numero de Carnet','Nota','Nombre Completo','Carrera','Credito'])
    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
    
def test_agregaciones_parciales(spark_session):

    prueba_data = [(980,1,70.50,'John Lennon','Computacion',3),(325,1,85.40,'John Lennon','Computacion',2),(980,2,55.40,'Paul McCartney','Computacion',4),(725,3,95.50,'Ringo Starr','Computacion',4),\
                    (589,4,45.40,'George Harrison','Fisica',2),(589,4,89.30,'George Harrison','Fisica',2)]
                    
    prueba_ds = spark_session.createDataFrame(prueba_data, ['Codigo de Curso','Numero de Carnet','Nota','Nombre Completo','Carrera','Credito'])
                    
    actual_ds = agregaciones_parciales(prueba_ds)
    
    expected_ds = spark_session.createDataFrame([('John Lennon','Computacion',76.46),('Paul McCartney','Computacion',55.40),('Ringo Starr', 'Computacion',95.50),('George Harrison','Fisica',67.35)],\
                                                 ['Nombre Completo','Carrera','promedio_ponderado'])
    
    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
    
def test_resultados_finales(spark_session):

    prueba_data = [('John Lennon','Computacion',76.46),('Paul McCartney','Computacion',55.4),('Ringo Starr','Computacion',95.4),('George Harrison','Fisica',67.35)]
    
    prueba_ds = spark_session.createDataFrame(prueba_data, ['Nombre Completo','Carrera','promedio_ponderado'])
    
    actual_ds = resultados_finales(prueba_ds,1)
    
    expected_ds = spark_session.createDataFrame([('Ringo Starr','Computacion',95.4),('George Harrison','Fisica',67.35)],['Nombre Completo','Carrera','Mejores_promedios'])
    
    expected_ds.show()
    actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
    
    
                    

