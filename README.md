# ETL_with_Airflow

In this project, I apply Bash to author a DAG and run, and monitor it on Airflow.
It uses the Cloud IDE based on Theia and Apache Airflow and MySQL database running in a Docker container.

Initially, 
1. Directory structure was created for staging areas as follows:

sudo mkdir -p /home/project/airflow/dags/finalassignment/staging


2. File downloaded to the /finalassignment directory
wget -P /home/project/airflow/dags/assignment https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz

3. changed to staging directory
cd /home/project/airflow/dags/finalassignment/staging
