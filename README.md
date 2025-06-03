# Setup

1. Set up GCP project
2. Create three datasets: 1. sales 2. source 3. seeds
3. Create json service account key
4. Replace .env variables with your own names

# Run order

create_date_dim.py

Day_One_Upload.py

python dbt_run.py seed --profiles-dir ./profiles --target seed --full-refresh

python dbt_run.py run --profiles-dir ./profiles --target dev --select tag:staging --full-refresh

Day_Two_Upload.py

python dbt_run.py run --profiles-dir ./profiles --target dev --select tag:sales

python generate_schema_yaml.py --models-base-dir models
