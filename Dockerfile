# pull official base image
FROM apache/airflow:latest-python3.9

USER root

# install psycopg2, git
RUN apt-get update \
    # pakage required for psycopg2
    && apt-get -y install libpq-dev gcc gosu git wget
    # && pip install psycopg2

###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update
#     apt-get install -y default-jre && \
#     apt-get install -y default-jdk && \
#     # apt-get install -y openjdk-11-jdk && \
#     apt-get update && \
#     java -version  && \
#     javac -version

# # Setup JAVA_HOME 
# # Get list java
# # update-alternatives --config java
# # Path remove /bin/java 
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
# RUN export JAVA_HOME
# ##############################
# # Finish JAVA installation
# ##############################


# ###############################
# ## SPARK files and variables
# ###############################
# ARG SPARK_VERSION="3.1.2"
# ARG HADOOP_VERSION="3.2"

# ENV SPARK_HOME /tmp/local/spark

# # Spark submit binaries (uncomment to enable jars) (Spark binaries must be the same version of spark cluster)
# RUN cd "/tmp" && \
#         wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         mkdir -p "${SPARK_HOME}/bin" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
#         mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
#         rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# # Create SPARK_HOME env var
# RUN export SPARK_HOME
# ENV PATH $PATH:$SPARK_HOME/bin
# ###############################
# ## Finish SPARK files and variables
# ###############################

USER airflow

# Required for airflow 
# RUN pip install passlib
# Install pakage python 
COPY ./requirements.txt .
RUN pip install -r requirements.txt