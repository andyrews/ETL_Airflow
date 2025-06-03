# ETL_Airflow
## Project Scope
1. __
2. Prediction Target: Next Day Price
3. Time Horizon: Daily
4. Models: Bi-LSTM / LSTM / Prophet
5. Metrics: RMSE, MAPE

## Installation
1. Install docker compose file
2. Run: 
```sh
docker-compose run airflow-worker airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```