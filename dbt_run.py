import os
import sys
import subprocess
from dotenv import load_dotenv

# Load .env from the current working directory (project root)
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    print(f"No .env found at {dotenv_path}. Continuing with system env only.")

# Pass through any arguments after the script
# E.g. python dbt_run_with_env.py run --profiles-dir ./profiles --target dev
dbt_cmd = ["dbt"] + sys.argv[1:]

print(f"Running: {' '.join(dbt_cmd)}")
result = subprocess.run(dbt_cmd)

if result.returncode == 0:
    print("dbt run succeeded.")
else:
    print(f"dbt run failed with exit code {result.returncode}.")
    sys.exit(result.returncode)
