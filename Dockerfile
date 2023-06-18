FROM python:3-alpine3.14
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# COPY cronjob /etc/crontabs/root

# Set permissions for the cron job file
# RUN chmod 0644 /etc/crontabs/root


# CMD ["python3", "load.py"]