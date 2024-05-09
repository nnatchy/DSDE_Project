FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

# Copy requirements file to the container
COPY requirements.txt /tmp/requirements.txt

# Install additional Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Run Airflow database upgrade
RUN airflow db upgrade

# Continue with the setup...
