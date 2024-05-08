FROM apache/airflow:2.6.1rc2

# Install Java for PySpark
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Switch back to the airflow user
USER airflow

# Copy requirements file to the container
COPY requirements.txt /tmp/requirements.txt

# Install additional Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Run Airflow database upgrade
RUN airflow db upgrade

# Continue with the setup...
