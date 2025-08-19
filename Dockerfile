FROM python:3.13-slim-trixie

# sets the working dir inside container
WORKDIR /app 

COPY requirements.txt .

# installs app dependencies specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# copies the app code from local into container
COPY . .

CMD ["python", "main.py"]
