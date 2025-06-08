FROM apache/spark-py:v3.4.0
USER root
RUN pip install pandas
WORKDIR /opt/spark-app
COPY app.py .