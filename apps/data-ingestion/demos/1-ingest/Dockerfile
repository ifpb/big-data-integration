# docker image [gcp]
FROM gcr.io/spark-operator/spark-py:v3.0.0

# using root user
USER root:root

# create directory for apps
RUN mkdir -p /app

# copy spark program
COPY etl.py /app/

# copy jar files
COPY jars /opt/spark/jars

# set work directory
WORKDIR /app

# user
USER 1001