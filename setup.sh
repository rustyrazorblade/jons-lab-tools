set -x

python3 -m venv ~/venvs/airflow

source bin/activate


cat << EOF
Add the following to bin/activate:

mkdir -p airflow
export AIRFLOW_HOME="$(pwd)/airflow"

Then start with:

airflow standalone

EOF


