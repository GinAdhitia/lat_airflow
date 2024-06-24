mkdir -p ./dags ./logs ./plugins

pip install cryptography

export FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

docker-compose up airflow initdb
docker-compose logs airflow
docker-compose logs postgres

docker-compose up

docker-compose exec airflow bash

airflow users create \
    --username admin \
    --firstname adminf \
    --lastname adminl \
    --role Admin \
    --email admin@admin.com \
    --password admin

airflow db init
airflow scheduler




