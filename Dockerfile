# Use a lightweight Python base image
FROM python:3.9-slim

# Set environment variables for proxy
ENV HTTP_PROXY=http://MeditelProxy.meditel.int:80
ENV HTTPS_PROXY=http://MeditelProxy.meditel.int:80

# Optionnel : désactiver la vérification SSL (⚠️ à utiliser avec prudence)
ENV PIP_NO_VERIFY_CERTS=1

# Set the working directory
WORKDIR /app

# Copy the requirements file first to leverage Docker's cache
COPY requirements.txt .

# Install Python dependencies with trusted hosts
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    -r requirements.txt

# Copy the rest of your application code
COPY . .

# Set the default command to run your scraper script
CMD ["python", "booking_wifi_score_scraper.py"]