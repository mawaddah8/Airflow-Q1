# Q2

Run

docker-compose up
Jupyter http://localhost:8886/ notebooks (createData.ipynb): to add faker data and download it in user2020 table in postgresql localhost:5432
and using pgadmin http://localhost:8000/ to create server with postgres_data host

mongodb: http://localhost:8081/ 

Airflow http://localhost:8080/  to add the pipeline use "docker-compose run airflow-worker airflow info"

but I've faced "could not translate host name  to address: Temporary failure in name resolution" issue when trying to extract data from postgresql
