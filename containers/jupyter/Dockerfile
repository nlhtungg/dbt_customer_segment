FROM bitnami/spark:3.4.1

USER root

RUN pip install notebook pyspark pytz

ARG ICEBERG_VERSION=1.9.2
ARG AWS_SDK_VERSION=2.25.46
ARG HADOOP_AWS_VERSION=3.3.1

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY jars/*.jar /opt/bitnami/spark/jars/

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser", "--NotebookApp.token=''"]

USER ${spark_uid}