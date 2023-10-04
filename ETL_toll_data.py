# import the libraries

from datetime import timedelta
# import the DAG object; to instantiate a DAG
from airflow import DAG
# import Operators to write the tasks
from airflow.operators.bash_operator import BashOperator
#import days_ago for scheduling
from airflow.utils.dates import days_ago

# DEFINING DAG ARGUMENTS
default_args = {
    'owner' : 'Bishwa', #dummy name
    'start_date':days_ago(0), #starts today
    'email': ['bishwa@dummymail.com'], # dummy email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DEFINING THE DAG

dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args = default_args, # defined earlier 
    description= 'Apache Airflow Final Assignment',
    schedule_interval= timedelta(days=1) #daily once
)

# DEFINING THE TASKS

# 1.3 unzip the data
# our desination directory : /home/project/airflow/dags/finalassignment
# unzip the data into from /home/project/airflow/dags/finalassignment/tolldata.tgz to destination
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= 'tar zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz '
    '-C /home/project/airflow/dags/finalassignment',
    dag=dag,

)

# 1.4 extract data from csv
# This task extracts the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type
# from the vehicle-data.csv file and saves them into a file named csv_data.csv
# the fields are 1 through 4; separated by comma (,)
# cut -d , fields input > output
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command = 
    'cut -d\',\' -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv '
    '> /home/project/airflow/dags/finalassignment/csv_data.csv',
    #bash_command='cut -d\',\' -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,

)

# 1.5 extract_data_from_tsv
# This task extracts the fields Number of axles, Tollplaza id, and Tollplaza code
# from the tollplaza-data.tsv file and saves it into a file named tsv_data.csv
# these fields are number 5,6,7 in the tollplaza-data.tsv
# cut -fields input | tr replace tab with comma > output

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command = 
    'cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv '
    '| tr \'\\t\' \',\' '
    '> /home/project/airflow/dags/finalassignment/tsv_data.csv',
    #bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | tr \'\\t\' \',\' > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# 1.6 extract_data_from_fixed_width
# This task extracts the fields Type of Payment code, and Vehicle Code 
# from the fixed width file payment-data.txt and saves it into
# a file named fixed_width_data.csv
# these fields are f6,7 of the payment-data.txt file

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command= '''
    awk '{
    match($0, /PT[A-Z] VC[0-9]+/); 
    if (RSTART) {
        pte_vc = substr($0, RSTART, RLENGTH);
        gsub(/ /, ",", pte_vc);
        print pte_vc >> "/home/project/airflow/dags/finalassignment/fixed_width_data.csv";
    }
    }' /home/project/airflow/dags/finalassignment/payment-data.txt
    ''',
    dag=dag,)


# 1.7 consolidate_data
# This task creates a single csv file named extracted_data.csv 
# by combining data from the following files:
# csv_data.csv # tsv_data.csv # fixed_width_data.csv
# The final csv file uses the fields in the order given below:
# Rowid, Timestamp, Anonymized Vehicle number, Vehicle type,
# Number of axles, Tollplaza id, Tollplaza code, 
# Type of Payment code, and Vehicle Code
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," '
        '/home/project/airflow/dags/finalassignment/csv_data.csv '
        '/home/project/airflow/dags/finalassignment/tsv_data.csv'
        '/home/project/airflow/dags/finalassignment/fixed_width_data.csv > '
        '/home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# 1.8 transform_data
# This task transforms the vehicle_type field in extracted_data.csv 
# into capital letters and 
# saves it into a file named transformed_data.csv in the staging directory
transform_data  = BashOperator(
    task_id= 'transform_data',
    bash_command = 
        "awk -F',' "
        "'BEGIN {OFS=\",\"} "
        "{$4=toupper($4)}1' "
        "/home/project/airflow/dags/finalassignment/extracted_data.csv > "
        "/home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,
)

# Defining the task pipeline
unzip_data >> extract_data_from_csv 
extract_data_from_csv >> extract_data_from_tsv 
extract_data_from_tsv >> extract_data_from_fixed_width
extract_data_from_fixed_width >> consolidate_data
consolidate_data >> transform_data