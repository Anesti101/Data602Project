from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_candidate_table(spark):
    pass

 


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

    academy_df = spark.table("all_academy")
    trainer_df = (academy_df.select("trainer").distinct())
    trainer_df = trainer_df.withColumn("trainer_id",row_number().over(Window.orderBy("trainer")))
    trainer_df = trainer_df.select("trainer_id", "trainer")

    return trainer_df

def create_weekly_review_table(spark):

    academy_df = spark.table("all_academy") # 
    competency_columns = [c for c in academy_df.columns if "_W" in c] 
    weeks = sorted(set(int(c.split("_W")[1])for c in competency_columns ))
    base_df = (academy_df.select("name","trainer" ).distinct())
    weekly_review_df = None

    for week in weeks:
        temp_df = (base_df.withColumn("week",lit(week)))

        if weekly_review_df is None:
            weekly_review_df = temp_df
        else:
            weekly_review_df = weekly_review_df.union(temp_df)

    weekly_review_df = weekly_review_df.withColumn("review_id",row_number().over(Window.orderBy( "name", "week")))
    weekly_review_df = weekly_review_df.select( "review_id","name", "trainer","week")
    return weekly_review_df



def create_competency_table(spark):

    competencies = [
        ("Analytic",),
        ("Independent",),
        ("Determined",),
        ("Professional",),
        ("Studious",),
        ("Imaginative",)

    ]
    competency_df = spark.createDataFrame(competencies,["competency_name"])
    competency_df = competency_df.withColumn("competency_id", row_number().over(Window.orderBy("competency_name")))
    competency_df = competency_df.select(  "competency_id", "competency_name")

    return competency_df
    
from pyspark.sql.functions import lower, trim, col

def create_assessment_table(spark):
    assessments = spark.table("silver_assessments")
    candidates = spark.table("candidates_clean")

    assessment_df = assessments.join(
        candidates,
        lower(trim(assessments.candidate_name)) == lower(trim(candidates.name)),
        "left"
    ).select(
        candidates.candidate_id,
        assessments.psychometric_score,
        assessments.presentation_score,
        assessments.assessment_date,
        assessments.location
    )

    return assessment_df


def create_score_table(spark):
    pass
