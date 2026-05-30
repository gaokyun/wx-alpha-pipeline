CREATE DATABASE jupyterhub;
GRANT ALL PRIVILEGES ON DATABASE jupyterhub TO airflow;

--docker compose exec -u postgres postgres psql -c "SELECT 'CREATE DATABASE jupyterhub' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'jupyterhub')\gexec"
--docker compose exec postgres psql -U airflow -l