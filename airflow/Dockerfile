FROM apache/airflow:2.6.1rc2

# Copy requirements file to the container
COPY requirements.txt /tmp/requirements.txt

# Install additional Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install the forked version of kafka-python
RUN pip uninstall -y kafka-python  
# Ensure removal of the existing kafka-python if it's included in requirements.txt
RUN pip install git+https://github.com/yourfork/kafka-python-ng.git

# Continue with the setup...
