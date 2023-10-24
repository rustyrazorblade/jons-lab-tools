This is a pretty rough tool only really meant for Jon to use.


# Setup

Instructions are Mac centric:

Might need to upgrade pip first:

```shell
python3.11 -m pip install --upgrade pip
```



```shell
pip3 install virtualenv
virtualenv venv
source venv/bin/activate
pip3 install "apache-airflow[celery]==2.7.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt"


airflow standalone

```


