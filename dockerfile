FROM python:3.11

# Set Python to run in unbuffered mode so logs appear immediately
ENV PYTHONUNBUFFERED=1

# Copy the packages file into the build
WORKDIR /app
COPY ./ /app/

# Run the install using the packages manifest file
RUN pip install --no-cache-dir -r requirements.txt

# When the container launches run the pipeline script
CMD ["python", "-u", "load_salesforce_marketing_cloud.py"]