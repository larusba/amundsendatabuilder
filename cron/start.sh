#!/bin/bash

SCHEDULER_ENVIRONMENT="Amundsen"

# Select the crontab file based on the environment
CRON_FILE="./cron/crontab.$SCHEDULER_ENVIRONMENT"

echo "Loading crontab file: $CRON_FILE"

# Load the crontab file
crontab $CRON_FILE

# Start cron
echo "Starting cron..."
cron -f