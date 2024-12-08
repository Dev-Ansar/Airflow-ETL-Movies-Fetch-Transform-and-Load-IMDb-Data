FROM apache/airflow:2.10.3

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt