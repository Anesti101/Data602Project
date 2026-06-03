from pyspark.sql.functions import ( col, explode, monotonically_increasing_id, row_number)
from pyspark.sql.window import Window


def create_candidate_table(spark):

    applicants = spark.table("bronze_applicants")

    candidate_df = applicants.select(
        "id",
        "name",
        "gender",
        "dob",
        "email",
        "city",
        "address",
        "postcode",
        "phone_num",
        "uni",
        "degree",
        "invited_date",
        "invited_by"
    )

    return candidate_df


def create_interview_table(spark):
    candidates = spark.table("candidates_clean")

    interview_df = candidates.select(
            "candidate_id",
            col("date").alias("interview_date"),
            "result",
            "course_interest",
        )
        # .withColumn("interview_id", row_number().over(Window.orderBy("candidate_id")))
        # .select(
        #     "interview_id",
        #     "candidate_id",
        #     "interview_date",
        #     "result",
        #     "course_interest",
        # )
    

    return interview_df

def create_technology_table(spark):
    technology_df = spark.table("technology_clean").select(
        "technology_id",
        "language"
    ).withColumnRenamed("language", "technology_name")

    return technology_df


def create_candidate_technology_table(spark):
    candidate_technology_df = spark.table("candidate_technology_clean").select(
        "candidate_id", 
        "technology_id", 
        "score"
    )

    return candidate_technology_df


def create_strength_table(spark):
    candidates = spark.table("candidates_clean")

    strengths_df = candidates.select(
        explode(col("strengths")).alias("strength_name")
        ).distinct() 
        # .withColumn(
            # "strength_id", 
            # row_number().over(Window.orderBy("strength_name"))
    
    return strengths_df


def create_candidate_strength_table(spark):
    candidates = spark.table("candidates_clean")

    candidate_strength_df = candidates.select(
        "candidate_id", 
        explode(col("strengths")).alias("strength_name")
    )
    # strengths = create_strength_table(spark)

    # candidate_strength_df = candidate_strength_df.join(
    #     strengths, on="strength_name", how="left"
    # ).select("candidate_id", "strength_id")

    return candidate_strength_df


def create_weakness_table(spark):
    candidates = spark.table("candidates_clean")

    weaknesses_df = (
        candidates.select(explode(col("weaknesses")).alias("weakness_name"))
        .distinct()
        # .withColumn("weakness_id", row_number().over(Window.orderBy("weakness_name")))
    )
    return weaknesses_df


def create_candidate_weakness_table(spark):
    candidates = spark.table("candidates_clean")

    candidate_weakness_df = candidates.select(
        "candidate_id", 
        explode(col("weaknesses")).alias("weakness_name")
    )
    # weaknesses = create_weakness_table(spark)

    # candidate_weakness_df = candidate_weakness_df.join(
    #     weaknesses, on="weakness_name", how="left"
    # ).select("candidate_id", "weakness_id")

    return candidate_weakness_df


def create_trainee_table(spark):
    pass

def create_trainer_table(spark):
    pass

def create_weekly_review_table(spark):
    pass

def create_competency_table(spark):
    pass

def create_score_table(spark):
    pass
