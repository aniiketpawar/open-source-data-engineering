# Dockerfile
# This file defines a custom Docker image for our Airflow services.
# It starts from the official Airflow image and adds our specific Python and system dependencies.

# Use the official Apache Airflow image as the base
FROM apache/airflow:2.8.2

# --- FIX STARTS HERE ---
# Switch to the root user to be able to install system packages
USER root

# Install the Java Development Kit (JDK), which is required by spark-submit
# The '-y' flag automatically answers yes to prompts.
# 'apt-get update' refreshes the package list to make sure we get the latest packages.
RUN apt-get update && apt-get install -y default-jdk

# Switch back to the default airflow user for security
USER airflow
# --- FIX ENDS HERE ---

# Copy our requirements.txt file into the image at the root directory
COPY requirements.txt /

# Install the Python packages specified in the requirements file.
RUN pip install --no-cache-dir -r /requirements.txt
