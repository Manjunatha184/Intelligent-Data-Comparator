FROM spark:3.5.3-java17-python3

USER root
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir --retries 10 --timeout 300 -r /app/requirements.txt
COPY . /app

EXPOSE 8000
CMD ["python3", "-m", "uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
