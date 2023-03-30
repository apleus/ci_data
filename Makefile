help:
		@echo "	build		Builds extended Airflow Docker image."
		@echo "	up			Setup Airflow Docker container network; create S3 connection."
		@echo "	down		Shuts down pipeline and removes Docker containers."
		@echo "	shell		Open Airflow webserver container shell"

build:
		docker build . --tag extending_airflow:latest

up:
		docker compose up -d

down:
		docker compose down

shell:
		docker exec -it $(docker container ls -q --filter name=ci_data-airflow-webserver*) sh