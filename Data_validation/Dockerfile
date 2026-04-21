# Use slim Python base — smaller image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline scripts
COPY pipeline/ ./pipeline/

# /data is mounted at runtime (not baked in)
VOLUME ["/data"]

CMD ["python", "pipeline/run_all.py"]