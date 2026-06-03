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
    pass

def create_technology_table(spark):
    pass

def create_strength_table(spark):
    pass

def create_weakness_table(spark):
    pass

def create_trainee_table(spark):
    pass

def create_score_table(spark):
    pass