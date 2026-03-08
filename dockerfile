FROM apache/airflow

WORKDIR /app

USER airflow

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

RUN pip cache purge