from src.config import JDBC_URL, DB_USER, DB_PASSWORD

    
def load_table(df, table_name):
    # Materialise the dataframe
    materialised_df = df.cache() # cache the dataframe
    materialised_df.count() # force the dataframe to be materialised
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
    print(f"{table_name} loaded successfully.")



def load_candidate(candidate_df):
    load_table(candidate_df, "candidate")


def load_assessment(assessment_df):
    load_table(assessment_df, "assessment")


def load_interview(interview_df):
    load_table(interview_df, "interview")


def load_technology(technology_df):
    load_table(technology_df, "technology")


def load_candidate_technology(candidate_technology_df):
    load_table(candidate_technology_df, "candidate_technology")


def load_strength(strength_df):
    load_table(strength_df, "strength")


def load_candidate_strength(candidate_strength_df):
    load_table(candidate_strength_df, "candidate_strength")


def load_weakness(weakness_df):
    load_table(weakness_df, "weakness")


def load_candidate_weakness(candidate_weakness_df):
    load_table(candidate_weakness_df, "candidate_weakness")


def load_trainer(trainer_df):
    load_table(trainer_df, "trainer")


def load_trainee(trainee_df):
    load_table(trainee_df, "trainee")


def load_competency(competency_df):
    load_table(competency_df, "competency")


def load_weekly_review(review_df):
    load_table(review_df, "weekly_review")


def load_score(score_df):
    load_table(score_df, "score")