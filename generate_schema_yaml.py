import os
import argparse
from google.cloud import bigquery
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()

COLUMN_METADATA_QUERY = """
SELECT
  column_name,
  data_type
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table_name}'
ORDER BY ordinal_position
"""

def generate_schema_yaml(model_name: str, columns_info: List[Dict[str, str]]) -> str:
    yaml_content = f"version: 2\n\nmodels:\n  - name: {model_name}\n    description: \"\"\n    columns:\n"
    for col in columns_info:
        yaml_content += (
            f"      - name: {col['name']}\n"
            f"        data_type: {col['data_type']}\n"
        )
    return yaml_content

def get_table_columns(project: str, dataset: str, table_name: str, bq_client: bigquery.Client) -> List[Dict[str, str]]:
    query = COLUMN_METADATA_QUERY.format(
        project=project,
        dataset=dataset,
        table_name=table_name
    )
    df = bq_client.query(query).to_dataframe()
    columns_info = []
    for _, row in df.iterrows():
        columns_info.append({
            'name': row['column_name'],
            'data_type': row['data_type']
        })
    return columns_info

def main(bq_client: bigquery.Client, models_base_dir: str, project: str, dataset: str) -> None:
    for root, _, files in os.walk(models_base_dir):
        for file in files:
            if file.endswith('.sql'):
                model_name = file.replace('.sql', '')
                columns_info = get_table_columns(project, dataset, model_name, bq_client)
                yaml_content = generate_schema_yaml(model_name, columns_info)
                yml_filename = f"{model_name}.yml"
                yml_path = os.path.join(root, yml_filename)
                if os.path.exists(yml_path):
                    continue
                with open(yml_path, 'w') as f:
                    f.write(yaml_content)
                print(f"Generated schema file: {yml_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate dbt schema YAML from BigQuery metadata.")
    parser.add_argument(
        '--models-base-dir',
        default='models/dimensional',
        help='Directory containing .sql model files (recursively).'
    )
    parser.add_argument(
        '--project',
        default=os.getenv('GCP_PROJECT'),
        help='BigQuery project ID.'
    )
    parser.add_argument(
        '--dataset',
        default=os.getenv('BQ_DATASET_SALES'),
        help='BigQuery dataset name.'
    )
    args = parser.parse_args()

    # This assumes GOOGLE_APPLICATION_CREDENTIALS is already set
    bq_client = bigquery.Client(project=args.project)
    main(bq_client, args.models_base_dir, args.project, args.dataset)
