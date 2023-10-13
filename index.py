import psycopg2
import pandas as pd
import configparser
config = configparser.ConfigParser()
config.read('.env')

from utils.helper import my_postgres_conn

['DB_CONN']
host = config['DB_CONN']['host']
user = config['DB_CONN']['user']
password = config['DB_CONN']['password']
database = config['DB_CONN']['database']



# #==========================Create connection to database
conn = my_postgres_conn(host,user,password,database)
print('connected')
cursor = conn.cursor()


#======================Test connection
query = 'SELECT * FROM employees'
cursor.execute(query)
print(cursor.fetchall())

#=====================Create a dataframe
query = 'SELECT * FROM employees'
cursor.execute(query)
result = cursor.fetchall()

df = pd.DataFrame(result)
print(df)


#===============EXECUTE SOME QUERIES

#=========1. What is the average salary paid to each department in the organization? Columns to return: department, average salary
query = 'SELECT department, round(avg(salary),2) from departments group by department;'
cursor.execute(query)
print(cursor.fetchall())


#======== 2. What are the appraisal scores of the top two paid (in descending order of salary and in ascending order of the employees’ names) employees? Columns to return: Name of Employee, Appraisal Score

query = '''SELECT e.name, a.last_appraisal_score
            FROM employees e
            LEFT JOIN appraisals a
                ON e._id = a.employee_id
            LEFT JOIN departments d
                ON e.department_id = d._id
            GROUP BY salary, name, last_appraisal_score
            ORDER BY salary desc, name 
			limit 2;
            '''
cursor.execute(query)
print(cursor.fetchall())


#========3. What are the appraisal scores of the top two paid (in descending order of salary and  in ascending order of the employees’ names) employees with a PHD?
query = '''SELECT e.name, a.last_appraisal_score
            FROM employees e
            LEFT JOIN appraisals a
                ON e._id = a.employee_id
            LEFT JOIN departments d
                ON e.department_id = d._id
            WHERE highest_education_level = 'PHD'
            GROUP BY salary, name, last_appraisal_score
            ORDER BY salary desc, name 
			limit 2;
            '''
cursor.execute(query)
print(cursor.fetchall())


#=========4. A list of employees appraised by the Head of Business? Columns to return: Employee’s Name, Appraiser.
query = ''' SELECT e.name, a.appraised_by 
            FROM employees e
            LEFT JOIN appraisals a
                ON e._id = a.employee_id
            WHERE appraised_by = 'Head of Business';'''

cursor.execute(query)
print(cursor.fetchall())


#===========5. Who appraised the employees with the highest appraisal score from the Corporate Sales Department? Columns to return: EmployeeID, Department, Appraisal Score, Appraiser
query = '''SELECT e._id, d.department, a.last_appraisal_score, a.appraised_by
            FROM employees e
            LEFT JOIN departments d
                ON e.department_id = d._id
            LEFT JOIN appraisals a
                ON e._id = a.employee_id
            WHERE department = 'Corporate Sales' and a.last_appraisal_score = 10 
            GROUP BY a.last_appraisal_score, e._id, d.department, a.appraised_by
            ORDER BY a.last_appraisal_score desc
         ;'''

cursor.execute(query)
print(cursor.fetchall())


#==========6. How many employees have an MSC in the Retails Sales department? Columns to return: Department, Degree, Total Employees
query = '''
        SELECT count(*) AS total_employees, d.department, e.highest_education_level
        FROM employees e
        LEFT JOIN departments d
            ON e.department_id = d._id
        WHERE d.department = 'Retail Sales' and e.highest_education_level = 'MSC'
        GROUP BY d.department, e.highest_education_level
;'''
cursor.execute(query)
print(cursor.fetchall())


#=========7. How much is spent on each department on bonuses? Columns to return: Department, number of employees, entitled_bonus per department,Total Bonus Amount per department  
query = '''
       select department, count(name) AS employee_total,entitled_bonus,
case when (department = 'Shipments') THEN (count(name) * entitled_bonus) END AS total_shipments_bonus
,case when (department = 'Retail Sales') THEN (count(name) * entitled_bonus) END AS total_retails_sales_bonus
,case when (department = 'Accounts') THEN (count(name) * entitled_bonus) END AS total_accounts_bonus
,case when (department = 'Corporate Sales') THEN (count(name) * entitled_bonus) END AS total_corporate_sales_bonus
from employees e
LEFT JOIN departments d 
ON e.department_id = d._id
group by department, entitled_bonus
;'''
cursor.execute(query)
print(cursor.fetchall())


#========8. Who are the top 3 salary earners? Order your result by their entitled bonuses. 
# Columns to return: Employees’ Names, Degree, Salary, Bonus

query = '''
        SELECT e.name, e.highest_education_level, salary, entitled_bonus
        FROM employees e 
        LEFT JOIN departments d
            ON e.department_id = d._id
        GROUP BY entitled_bonus, e.name, e.highest_education_level, salary
        ORDER BY entitled_bonus desc
        LIMIT 3
;'''
cursor.execute(query)
print(cursor.fetchall())


cursor.close()
conn.close()





