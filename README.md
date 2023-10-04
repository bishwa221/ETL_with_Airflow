# ETL_with_Airflow

In this project, I apply Bash to author a DAG and run, and monitor it on Airflow.
It uses the Cloud IDE based on Theia and Apache Airflow and MySQL database running in a Docker container.

Initially, 
1. Started Airflow with
``` bash
start_airflow
```
2. Created directory structure for staging areas as follows:
 ``` bash
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
```
2. Downloaded the file to perform ETL with, to the /finalassignment directory
``` bash
wget -P /home/project/airflow/dags/assignment https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
```
3. Navigated to staging directory
```bash
cd /home/project/airflow/dags/finalassignment/staging
```
4. Authored the DAG, ETL_tool_data.py
5. Summitted the DAG to Airflow and got it running.
