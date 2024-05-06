FROM apache/airflow:2.6.1rc2

# Copy requirements file to the container
COPY requirements.txt /tmp/requirements.txt

# Install additional Python packages specified in requirements.txt
# Note that we no longer use the --user flag because it's not needed within the Docker container environment
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Continue with the setup...
