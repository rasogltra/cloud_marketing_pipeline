FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app code
COPY src/ ./src
COPY config/ ./config
COPY README.md .

# Mount directories
RUN mkdir -p raw_data processed_data logs

CMD ["python", "src/main.py"]