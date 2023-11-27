This is a pretty rough tool only really meant for Jon to use.

TaskFlow: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html



# Setup

To get going, run the `setup.sh` script to build a local airflow image that contains the AWS dependencies.



Original docs: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


**To kill the image for rebuilding**

`docker compose down --volumes --remove-orphans`

Run the start:

`./start.sh`

Load the UI:

http://0.0.0.0:8080

Username & Password: airflow:airflow
