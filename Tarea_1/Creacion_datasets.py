import pandas as pd
import random
import numpy as np

def generate_career():
    careers = ['Mecanica', 'Computacion', 'Electronica', 'Electrica', 'Fisica','Electromecanica','Quimica','Biotecnologia']
    return random.choice(careers)

# Create Student DataFrame
student_data = []
for i in range(1, 1001):
    student_data.append([i,f'Person{i}'+ f'LastName{i}', generate_career()])

student_df = pd.DataFrame(student_data, columns=['id', 'student name', 'career'])

# Function to generate random course credit
def generate_credit():
    return random.randint(2, 5)

# Create Course DataFrame
course_data = []
for career in student_df['career'].unique():
    for i in range(1, 11):
        course_data.append([f'Course{i}', career, generate_credit()])

course_df = pd.DataFrame(course_data, columns=['id', 'career', 'course credit'])

def combine_length_last_char(row):
    length = len(row['career'])
    last_char = row['id'][-1]
    return f'{length}{last_char}'

# Create a new column 'Combined' using the custom function
course_df['Course_id'] = course_df.apply(combine_length_last_char, axis=1)
course_df=course_df.drop(columns=['id'])
course_df=course_df.reindex(columns=['Course_id','course credit','career'])


# Display the DataFrames
print("Student DataFrame:")
print(student_df.head())
print(student_df.info())

print("\nCourse DataFrame:")
print(course_df.head())
print(course_df.info())

new_dataset=pd.merge(student_df,course_df,'inner',left_on='career',right_on='career')

#Se quitan 3000 random records para que todos los estudiantes no lleven la misma cantidad de cursos
indices_to_delete = np.random.choice(new_dataset.index, 3000, replace=False)

# Create a new DataFrame with 4000 records removed
new_df = new_dataset.drop(indices_to_delete)
new_df=new_df.drop(columns=['student name','career','course credit'])

#Se quitan estudiantes que no matricularon
new_df = new_df[~new_df['id'].isin([25,87,150,458,356,158,825,258,725,259])]

#Se quita la carrera de Fisica que no fue matriculada por ningun estudiante
new_df = new_df[~new_df['Course_id'].isin(['60','61','62','63','64','65','66','67','68','69'])]

#Se añadeden records para loas estudiantes que han llevado el curso mas de una vez
duplicates = new_df.sample(n=700,replace=True)
new_df_2=pd.concat([new_df,duplicates],ignore_index=True)

#Se añade nota para cada curso
new_df_2['Grade'] = np.random.normal(loc=70, scale=15, size=len(new_df_2))
new_df_2['Grade'] = new_df_2['Grade'].clip(0, 100)
new_df_2['Grade'] = new_df_2['Grade'].round(1)

#Se ordenan las columnas
new_df_2=new_df_2.reindex(columns=['id','Course_id','Grade'])

print(new_df.info())
print(new_df.head(15))

student_df.to_csv('estudiante.csv', index=False)
course_df.to_csv('curso.csv', index=False)
new_df_2.to_csv('nota.csv',index=False)