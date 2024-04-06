FROM python:3.11.6-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1 \
    PYTHONUNBUFFERED 1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# Set working directory inside the container
WORKDIR /usr/src/bmaps_api

# Install system dependencies
RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y --fix-missing && \
    apt-get install -f && \
    apt-get install -y gcc libpq-dev python3-dev build-essential cron && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up the cron schedule and start cron
RUN (echo "1 0 * * * /usr/local/bin/python /home/ubuntu/BMAPS_API/update_master.py >> /var/log/cron.log 2>&1") | crontab - \
    && mkdir -p /var/log \
    && touch /var/log/cron.log

# Upgrade pip and Install Python dependencies
COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy your application code and scripts
COPY . .

# Ensure script is executable
RUN chmod +x /home/ubuntu/BMAPS_API/update_master.py

# Expose the port number used by the application
EXPOSE 3000