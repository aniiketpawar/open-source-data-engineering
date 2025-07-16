# /superset/Dockerfile

# Start from the official, stable Apache Superset image
FROM apache/superset:3.1.1

# Switch to the root user to install packages
USER root

# Copy our requirements file into the container
COPY requirements-superset.txt /app/

# Install the required python packages from our file
RUN pip install -r /app/requirements-superset.txt

# Switch back to the default superset user for security
USER superset
