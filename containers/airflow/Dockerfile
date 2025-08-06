FROM apache/airflow:2.7.3

USER airflow
RUN pip install --upgrade pip
RUN pip install astronomer-cosmos[dbt-postgres]
RUN pip install astronomer-cosmos[dbt-spark]
RUN pip install dbt-core
RUN pip install dbt-spark
RUN pip install psycopg2-binary
RUN pip install dbt-postgres
RUN pip install dbt-spark[PyHive]