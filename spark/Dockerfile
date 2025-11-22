FROM ubuntu:22.04

# System deps: Java 11, Python 3.11, tools
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3.11 python3.11-venv python3.11-distutils \
    curl wget ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.11
RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
 && python3.11 /tmp/get-pip.py \
 && rm /tmp/get-pip.py

# Make python3 point to 3.11 and pip3 available
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2 \
 && ln -sf /usr/local/bin/pip /usr/bin/pip3

# Python libs for the project
# pyspark matches Spark 3.4.1
RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    pandas \
    pyarrow \
    fastparquet \
    requests \
    gridstatus

# Spark install
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
 && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Env
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin:/opt/spark/sbin"
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

WORKDIR /project
CMD ["sleep", "infinity"]
