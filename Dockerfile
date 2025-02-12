# Use an official Python runtime as the base image
FROM python:3.9-slim

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y git libssl-dev cmake libjpeg-dev build-essential python3-setuptools && \
    apt-get clean

# Set the working directory in the container
WORKDIR /app

# Install the required Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --src /usr/local/src

# Copy the application code into the container
COPY . .

# Find and install requirements.txt files in the plugins subfolder
RUN find plugins -name "requirements.txt" -exec pip install --no-cache-dir -r {} \;

# Command to run the application
CMD ["python", "yadacoin/app.py", "--config=config/config.json", "--mongohost=mongodb"]