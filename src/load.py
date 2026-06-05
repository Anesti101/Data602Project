"""
Module: load.py

Purpose:
Load transformed Spark DataFrames into the target database tables.

Responsibilities:
- Write data using JDBC or SQLAlchemy-backed pandas operations
- Provide one loader function per destination table
- Ensure the correct append load mode for analytics tables

Author: Project Team
"""

from src.config import JDBC_URL, DB_USER, DB_PASSWORD


def load_table(df, table_name):
    """
    Append a Spark DataFrame to a database table using JDBC.

    Args:
        df: Spark DataFrame to write.
        table_name: Destination table name in the target database.
    """
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# NOTE: The second load_table definition overrides the earlier JDBC implementation.
# It is retained unchanged to preserve repository behavior, even though it uses
# an external engine variable that is not defined in this module.

def load_table(df, table_name):
    """
    Append a pandas-compatible DataFrame to a database table using SQLAlchemy.

    Args:
        df: Spark DataFrame to convert and write.
        table_name: Destination table name in the target database.
    """
    pandas_df = df.toPandas()

    pandas_df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )

    print(f"{table_name} loaded successfully.")


def load_candidate(candidate_df):
    """
    Load the candidate dimension table.

    Args:
        candidate_df: Candidate DataFrame.
    """
    load_table(candidate_df, "candidate")


def load_assessment(assessment_df):
    """
    Load the assessment fact table.

    Args:
        assessment_df: Assessment DataFrame.
    """
    load_table(assessment_df, "assessment")


def load_interview(interview_df):
    """
    Load the interview fact table.

    Args:
        interview_df: Interview DataFrame.
    """
    load_table(interview_df, "interview")


def load_technology(technology_df):
    """
    Load the technology dimension table.

    Args:
        technology_df: Technology DataFrame.
    """
    load_table(technology_df, "technology")


def load_candidate_technology(candidate_technology_df):
    """
    Load the candidate-technology relationship table.

    Args:
        candidate_technology_df: Candidate technology DataFrame.
    """
    load_table(candidate_technology_df, "candidate_technology")


def load_strength(strength_df):
    """
    Load the strength dimension table.

    Args:
        strength_df: Strength DataFrame.
    """
    load_table(strength_df, "strength")


def load_candidate_strength(candidate_strength_df):
    """
    Load the candidate-strength relationship table.

    Args:
        candidate_strength_df: Candidate strength DataFrame.
    """
    load_table(candidate_strength_df, "candidate_strength")


def load_weakness(weakness_df):
    """
    Load the weakness dimension table.

    Args:
        weakness_df: Weakness DataFrame.
    """
    load_table(weakness_df, "weakness")


def load_candidate_weakness(candidate_weakness_df):
    """
    Load the candidate-weakness relationship table.

    Args:
        candidate_weakness_df: Candidate weakness DataFrame.
    """
    load_table(candidate_weakness_df, "candidate_weakness")


def load_trainer(trainer_df):
    """
    Load the trainer dimension table.

    Args:
        trainer_df: Trainer DataFrame.
    """
    load_table(trainer_df, "trainer")


def load_trainee(trainee_df):
    """
    Load the trainee table for academy enrollment.

    Args:
        trainee_df: Trainee DataFrame.
    """
    load_table(trainee_df, "trainee")


def load_competency(competency_df):
    """
    Load the competency dimension table.

    Args:
        competency_df: Competency DataFrame.
    """
    load_table(competency_df, "competency")


def load_weekly_review(review_df):
    """
    Load the weekly review records for academy trainees.

    Args:
        review_df: Weekly review DataFrame.
    """
    load_table(review_df, "weekly_review")


def load_score(score_df):
    """
    Load the trainee score table.

    Args:
        score_df: Score DataFrame.
    """
    load_table(score_df, "score")
