mkdir -p dags logs config plugins

export AIRFLOW_IMAGE_NAME=rustyrazorblade/airflow:latest
docker-compose up
