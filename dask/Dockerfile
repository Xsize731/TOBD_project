FROM daskdev/dask:latest

# Устанавливаем совместимые версии пакетов
RUN pip install --no-cache-dir \
    apache-airflow[postgres]==2.6.3 \          
    pandas==2.0.3 \
    sqlalchemy==1.4.51 \                        
    psycopg2-binary==2.9.9 \
    pyarrow==14.0.1 \
    s3fs==2023.12.0 \
    boto3==1.34.0

WORKDIR /app