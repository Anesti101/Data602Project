import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

BASE_DIR = Path(__file__).parent
EXPORT_DIR = BASE_DIR / "etl_exports"

DB_USER = "neondb_owner"
DB_PASSWORD = "npg_iZAtxc3z9gPS"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    "@ep-summer-field-ab82vxw2-pooler.eu-west-2.aws.neon.tech:5432/neondb"
    "?sslmode=require"
)

load_order = [
    "candidate",
    "technology",
    "strength",
    "weakness",
    "trainer",
    "competency",
    "assessment",
    "interview",
    "candidate_technology",
    "candidate_strength",
    "candidate_weakness",
    "trainee",
    "weekly_review",
    "score"
]

for table in load_order:

    print(f"Loading {table}...")

    df = pd.read_csv(EXPORT_DIR / f"{table}.csv")

    df.to_sql(
        table,
        engine,
        if_exists="append",
        index=False
    )

    print(f"✓ Loaded {table}")

print("\nAll tables loaded successfully.")