from src.transform import *
from src.load import *
from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("sparta-global-etl")
        .getOrCreate()
    )


def main():

    spark = None

    try:

        spark = create_spark_session()

        print("=" * 50)
        print("Creating transformed DataFrames...")
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

        print("\nValidating transformed DataFrames...")
        print("-" * 50)

        tables = {
            "Candidate": candidate_df,
            "Assessment": assessment_df,
            "Interview": interview_df,
            "Technology": technology_df,
            "CandidateTechnology": candidate_technology_df,
            "Strength": strength_df,
            "CandidateStrength": candidate_strength_df,
            "Weakness": weakness_df,
            "CandidateWeakness": candidate_weakness_df,
            "Trainer": trainer_df,
            "Trainee": trainee_df,
            "Competency": competency_df,
            "WeeklyReview": weekly_review_df,
            "Score": score_df
        }

        for table_name, df in tables.items():
            print(f"{table_name}: {df.count()} rows")

        print("\nLoading dimension tables...")
        print("-" * 50)

        load_candidate(candidate_df)
        load_technology(technology_df)
        load_strength(strength_df)
        load_weakness(weakness_df)
        load_trainer(trainer_df)
        load_competency(competency_df)

        print("\nLoading assessment/interview tables...")
        print("-" * 50)

        load_assessment(assessment_df)
        load_interview(interview_df)

        print("\nLoading relationship tables...")
        print("-" * 50)

        load_candidate_technology(candidate_technology_df)
        load_candidate_strength(candidate_strength_df)
        load_candidate_weakness(candidate_weakness_df)

        print("\nLoading academy tables...")
        print("-" * 50)

        load_trainee(trainee_df)
        load_weekly_review(weekly_review_df)

        print("\nLoading score tables...")
        print("-" * 50)

        load_score(score_df)

        print("\n" + "=" * 50)
        print("Pipeline completed successfully.")
        print("=" * 50)

    except Exception as e:

        print("\n" + "=" * 50)
        print("PIPELINE FAILED")
        print("=" * 50)
        print(f"Error: {e}")

        raise

    finally:

        if spark is not None:
            spark.stop()
            print("\nSpark session stopped.")


if __name__ == "__main__":
    main()