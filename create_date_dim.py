import pandas as pd
from pathlib import Path

SEEDS_DIR = Path(__file__).resolve().parent / "seeds"

def generate_calendar_dim(start, end):
    dates = pd.date_range(start, end)
    df = pd.DataFrame({'date': dates})
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')  # Standard ISO format
    df['year'] = pd.to_datetime(df['date']).dt.year
    df['quarter'] = 'Q' + pd.to_datetime(df['date']).dt.quarter.astype(str)
    return df[['date', 'year', 'quarter']]

df = generate_calendar_dim('2024-01-01', '2025-02-01')
csv_path = SEEDS_DIR / "date_spine.csv"
df.to_csv(csv_path, index=False)

print(f"CSV written to: {csv_path}")
