from src.transform import *
import os


print("=" * 50)
print("CREATING TRANSFORMED TABLES")
print("=" * 50)

candidate_df = create_candidate_table(spark)
assessment_df = create_assessment_table(spark)
interview_df = create_interview_table(spark)
technology_df = create_technology_table(spark)
candidate_technology_df = create_candidate_technology_table(spark)
strength_df = create_strength_table(spark)
candidate_strength_df = create_candidate_strength_table(spark)
weakness_df = create_weakness_table(spark)
candidate_weakness_df = create_candidate_weakness_table(spark)
trainer_df = create_trainer_table(spark)
trainee_df = create_trainee_table(spark)
competency_df = create_competency_table(spark)
weekly_review_df = create_weekly_review_table(spark)
score_df = create_score_table(spark)

tables = {
    "candidate": candidate_df,
    "assessment": assessment_df,
    "interview": interview_df,
    "technology": technology_df,
    "candidate_technology": candidate_technology_df,
    "strength": strength_df,
    "candidate_strength": candidate_strength_df,
    "weakness": weakness_df,
    "candidate_weakness": candidate_weakness_df,
    "trainer": trainer_df,
    "trainee": trainee_df,
    "competency": competency_df,
    "weekly_review": weekly_review_df,
    "score": score_df
}

print("\n")
print("=" * 50)
print("EXPORTING TABLES")
print("=" * 50)

export_location = "etl_exports"
os.makedirs(export_location, exist_ok=True)
for table_name, df in tables.items():

    try:
        row_count = df.count()

        print(f"\nExporting {table_name}")
        print(f"Rows: {row_count}")

        pandas_df = df.toPandas()

        csv_file = f"{export_location}/{table_name}.csv"

        pandas_df.to_csv(
            csv_file,
            index=False
        )

        print(f"✓ Saved: {csv_file}")

    except Exception as e:
        print(f"✗ Failed exporting {table_name}")
        print(str(e))

print("\n")
print("=" * 50)
print("EXPORT COMPLETE")
print("=" * 50)