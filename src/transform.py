from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _clean_text(column: Column) -> Column:
    cleaned = F.trim(column.cast("string"))
    return F.when(
        cleaned.isNull() | (cleaned == "") | (F.lower(cleaned) == "null"),
        F.lit(None)
    ).otherwise(cleaned)


def _name_key(column: Column) -> Column:
    return F.lower(F.regexp_replace(F.trim(column.cast("string")), r"\s+", " "))


def _parse_date(column: Column) -> Column:

    cleaned = _clean_text(column)

    return (
        F.when(cleaned.isNull(), None)
        .when(F.trim(cleaned) == "", None)
        .otherwise(
            F.coalesce(
                F.try_to_timestamp(cleaned, F.lit("dd/MM/yyyy")).cast("date"),
                F.try_to_timestamp(cleaned, F.lit("d/M/yyyy")).cast("date"),
                F.try_to_timestamp(cleaned, F.lit("MMMM d yyyy")).cast("date"),
                F.try_to_timestamp(cleaned, F.lit("d MMMM yyyy")).cast("date"),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd")).cast("date")
            )
        )
    )


def _candidate_lookup(spark: SparkSession) -> DataFrame:
    candidates = create_candidate_table(spark).select(
        "candidate_id",
        _name_key(F.col("name")).alias("candidate_name_key")
    )

    return (
        candidates
        .filter(F.col("candidate_name_key").isNotNull())
        .withColumn(
            "name_match_count",
            F.count("*").over(Window.partitionBy("candidate_name_key"))
        )
        .filter(F.col("name_match_count") == 1)
        .drop("name_match_count")
    )


def _academy_base_with_candidate(spark: SparkSession) -> DataFrame:
    academy_df = spark.table("all_academy")
    candidate_lookup = _candidate_lookup(spark)

    return (
        academy_df
        .withColumn("candidate_name_key", _name_key(F.col("name")))
        .withColumn("trainer_name", _clean_text(F.col("trainer")))
        .withColumn(
            "academy_cohort",
            F.regexp_extract(
                F.col("source_file"),
                r"Academy/([^/]+)_\d{4}-\d{2}-\d{2}\.csv$",
                1
            )
        )
        .withColumn(
            "start_date",
            F.to_date(
                F.regexp_extract(
                    F.col("source_file"),
                    r"_(\d{4}-\d{2}-\d{2})\.csv$",
                    1
                ),
                "yyyy-MM-dd"
            )
        )
        .join(candidate_lookup, on="candidate_name_key", how="inner")
        .filter(
            F.col("candidate_id").isNotNull()
            & F.col("academy_cohort").isNotNull()
            & (F.col("academy_cohort") != "")
            & F.col("start_date").isNotNull()
        )
    )




def create_candidate_table(spark: SparkSession) -> DataFrame:
    applicants = spark.table("all_applicants")
    phone_col = "phone_num" if "phone_num" in applicants.columns else "phone_number"

    df_cleaned = (
        applicants
        .withColumn("name", _clean_text(F.col("name")))
        .withColumn("gender", _clean_text(F.col("gender")))
        .withColumn("email", F.lower(_clean_text(F.col("email"))))
        .withColumn("city", _clean_text(F.col("city")))
        .withColumn("address", _clean_text(F.col("address")))
        .withColumn("postcode", F.upper(_clean_text(F.col("postcode"))))
        .withColumn("phone_num", _clean_text(F.col(phone_col)))
        .withColumn("uni", _clean_text(F.col("uni")))
        .withColumn("degree", _clean_text(F.col("degree")))
        .withColumn("invited_by", _clean_text(F.col("invited_by")))
        .withColumn("dob", _parse_date(F.col("dob")))
        .withColumn(
                "invited_date",
               F.when(
                    _clean_text(F.col("month")).isNull(),
                    None
               ).otherwise(
                    F.try_to_timestamp(
                       F.concat_ws(
                            "-",
                        F.col("invited_date").cast("string"),
                        _clean_text(F.col("month"))
                   ),
                   F.lit("d-MMMM yyyy")
                ).cast("date")
            )
      )
      
        .withColumn("source_file", F.col("Source_file"))
        .dropDuplicates(["name", "dob", "email", "source_file", "invited_date"])
    )

    window_spec = Window.orderBy(
        _name_key(F.col("name")),
        F.col("dob"),
        F.col("email"),
        F.col("source_file"),
        F.col("id")
    )

    return (
        df_cleaned
        .withColumn("candidate_id", F.row_number().over(window_spec))
        .select(
            "candidate_id",
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
    )


def create_assessment_table(spark: SparkSession) -> DataFrame:

    assessments = spark.table("all_assessments")

    candidate_lookup = _candidate_lookup(spark)

    assessment_df = (
        assessments

        .filter(
            F.col("line_text").contains("Psychometrics")
        )

        .withColumn(
            "candidate_name",
            F.trim(
                F.regexp_extract(
                    F.col("line_text"),
                    r"^(.*?)\s*-\s*Psychometrics",
                    1
                )
            )
        )

        .withColumn(
            "psychometric_score",
            F.regexp_extract(
                F.col("line_text"),
                r"Psychometrics:\s*(\d+)",
                1
            ).cast("int")
        )

        .withColumn(
            "presentation_score",
            F.regexp_extract(
                F.col("line_text"),
                r"Presentation:\s*(\d+)",
                1
            ).cast("int")
        )

        .withColumn(
            "candidate_name_key",
            _name_key(F.col("candidate_name"))
        )

        .join(
            candidate_lookup,
            on="candidate_name_key",
            how="inner"
        )
    )

    date_lookup = (
        assessments
        .filter(
            F.col("line_text").rlike(
                r"^\w+\s+\d+\s+\w+\s+\d{4}$"
            )
        )
        .select(
            "source_file",
            F.to_date(
                F.regexp_replace(
                    F.col("line_text"),
                    r"^\w+\s+",
                    ""
                ),
                "d MMMM yyyy"
            ).alias("assessment_date")
        )
    )

    location_lookup = (
        assessments
        .filter(
            F.col("line_text").contains("Academy")
        )
        .select(
            "source_file",
            F.col("line_text").alias("location")
        )
    )

    assessment_df = (
        assessment_df
        .join(
            date_lookup,
            on="source_file",
            how="left"
        )
        .join(
            location_lookup,
            on="source_file",
            how="left"
        )
        .withColumn(
            "assessment_id",
            F.row_number().over(
                Window.orderBy(
                    "candidate_id",
                    "assessment_date"
                )
            )
        )
        .select(
            "assessment_id",
            "candidate_id",
            "psychometric_score",
            "presentation_score",
            "assessment_date",
            "location"
        )
    )

    return assessment_df

def create_interview_table(spark: SparkSession) -> DataFrame:

    talent = spark.table("all_talent")
    candidate_lookup = _candidate_lookup(spark)

    interview_df = (
        talent
        .withColumn(
            "candidate_name_key",
            _name_key(F.col("name"))
        )
        .join(
            candidate_lookup,
            on="candidate_name_key",
            how="inner"
        )
        .withColumn(
            "interview_date",
            _parse_date(F.col("date"))
        )
        .withColumn(
            "interview_id",
            F.row_number().over(
                Window.orderBy(
                    "candidate_id",
                    "interview_date"
                )
            )
        )
        .select(
            "interview_id",
            "candidate_id",
            "interview_date",
            F.col("result"),
            F.col("course_interest")
        )
    )

    return interview_df

def create_technology_table(spark: SparkSession) -> DataFrame:

    technologies = [
        ("C#",),
        ("Java",),
        ("R",),
        ("JavaScript",),
        ("Python",),
        ("C++",),
        ("Ruby",),
        ("SPSS",),
        ("PHP",)
    ]

    return (
        spark.createDataFrame(
            technologies,
            ["technology_name"]
        )
        .withColumn(
            "technology_id",
            F.row_number().over(
                Window.orderBy("technology_name")
            )
        )
        .select(
            "technology_id",
            "technology_name"
        )
    )

def create_candidate_technology_table(
    spark: SparkSession
) -> DataFrame:

    talent = spark.table("all_talent")
    candidate_lookup = _candidate_lookup(spark)
    technology_df = create_technology_table(spark)

    tech_columns = [
        "tech_self_score.C#",
        "tech_self_score.Java",
        "tech_self_score.R",
        "tech_self_score.JavaScript",
        "tech_self_score.Python",
        "tech_self_score.C++",
        "tech_self_score.Ruby",
        "tech_self_score.SPSS",
        "tech_self_score.PHP"
    ]

    stack_expr = """
    stack(
        9,
        'C#', `tech_self_score.C#`,
        'Java', `tech_self_score.Java`,
        'R', `tech_self_score.R`,
        'JavaScript', `tech_self_score.JavaScript`,
        'Python', `tech_self_score.Python`,
        'C++', `tech_self_score.C++`,
        'Ruby', `tech_self_score.Ruby`,
        'SPSS', `tech_self_score.SPSS`,
        'PHP', `tech_self_score.PHP`
    ) as (technology_name, score)
    """

    candidate_technology_df = (
        talent
        .withColumn(
            "candidate_name_key",
            _name_key(F.col("name"))
        )
        .join(
            candidate_lookup,
            on="candidate_name_key",
            how="inner"
        )
        .select(
            "candidate_id",
            F.expr(stack_expr)
        )
        .filter(F.col("score").isNotNull())
        .join(
            technology_df,
            on="technology_name",
            how="inner"
        )
        .select(
            "candidate_id",
            "technology_id",
            F.col("score").cast("int")
        )
        .dropDuplicates(
            ["candidate_id", "technology_id"]
        )
    )

    return candidate_technology_df


def create_strength_table(
    spark: SparkSession
) -> DataFrame:

    return (
        spark.table("all_talent")
        .select(
            F.explode("strengths").alias(
                "strength_name"
            )
        )
        .withColumn(
            "strength_name",
            _clean_text(
                F.col("strength_name")
            )
        )
        .filter(
            F.col("strength_name").isNotNull()
        )
        .distinct()
        .withColumn(
            "strength_id",
            F.row_number().over(
                Window.orderBy("strength_name")
            )
        )
        .select(
            "strength_id",
            "strength_name"
        )
    )

def create_candidate_strength_table(
    spark: SparkSession
) -> DataFrame:

    talent = spark.table("all_talent")
    candidate_lookup = _candidate_lookup(spark)
    strength_df = create_strength_table(spark)

    return (
        talent
        .withColumn(
            "candidate_name_key",
            _name_key(F.col("name"))
        )
        .join(
            candidate_lookup,
            on="candidate_name_key",
            how="inner"
        )
        .select(
            "candidate_id",
            F.explode("strengths").alias(
                "strength_name"
            )
        )
        .join(
            strength_df,
            on="strength_name",
            how="inner"
        )
        .select(
            "candidate_id",
            "strength_id"
        )
        .dropDuplicates()
    )


def create_weakness_table(
    spark: SparkSession
) -> DataFrame:

    return (
        spark.table("all_talent")
        .select(
            F.explode("weaknesses").alias(
                "weakness_name"
            )
        )
        .withColumn(
            "weakness_name",
            _clean_text(
                F.col("weakness_name")
            )
        )
        .filter(
            F.col("weakness_name").isNotNull()
        )
        .distinct()
        .withColumn(
            "weakness_id",
            F.row_number().over(
                Window.orderBy("weakness_name")
            )
        )
        .select(
            "weakness_id",
            "weakness_name"
        )
    )


def create_candidate_weakness_table(
    spark: SparkSession
) -> DataFrame:

    talent = spark.table("all_talent")
    candidate_lookup = _candidate_lookup(spark)
    weakness_df = create_weakness_table(spark)

    return (
        talent
        .withColumn(
            "candidate_name_key",
            _name_key(F.col("name"))
        )
        .join(
            candidate_lookup,
            on="candidate_name_key",
            how="inner"
        )
        .select(
            "candidate_id",
            F.explode("weaknesses").alias(
                "weakness_name"
            )
        )
        .join(
            weakness_df,
            on="weakness_name",
            how="inner"
        )
        .select(
            "candidate_id",
            "weakness_id"
        )
        .dropDuplicates()
    )


def create_trainer_table(spark: SparkSession) -> DataFrame:
    return (
        spark.table("all_academy")
        .select(_clean_text(F.col("trainer")).alias("trainer_name"))
        .filter(F.col("trainer_name").isNotNull())
        .distinct()
        .withColumn(
            "trainer_id",
            F.row_number().over(Window.orderBy("trainer_name"))
        )
        .select("trainer_id", "trainer_name")
    )


def create_trainee_table(spark: SparkSession) -> DataFrame:
    academy_base = _academy_base_with_candidate(spark)

    return (
        academy_base
        .select("candidate_id", "start_date", "academy_cohort")
        .distinct()
        .withColumn(
            "trainee_id",
            F.row_number().over(
                Window.orderBy("candidate_id", "start_date", "academy_cohort")
            )
        )
        .select("trainee_id", "candidate_id", "start_date", "academy_cohort")
    )


def create_competency_table(spark: SparkSession) -> DataFrame:
    competencies = [
        ("Analytic",),
        ("Independent",),
        ("Determined",),
        ("Professional",),
        ("Studious",),
        ("Imaginative",)
    ]

    return (
        spark.createDataFrame(competencies, ["competency_name"])
        .withColumn(
            "competency_id",
            F.row_number().over(Window.orderBy("competency_name"))
        )
        .select("competency_id", "competency_name")
    )


def create_weekly_review_table(spark: SparkSession) -> DataFrame:
    academy_df = spark.table("all_academy")
    academy_base = _academy_base_with_candidate(spark)
    trainee_df = create_trainee_table(spark)
    trainer_df = create_trainer_table(spark)

    competency_columns = [column for column in academy_df.columns if "_W" in column]
    weeks = sorted(set(int(column.split("_W")[1]) for column in competency_columns))
    week_df = spark.createDataFrame([(week,) for week in weeks], ["week"])

    base_df = (
        academy_base
        .select("candidate_id", "start_date", "academy_cohort", "trainer_name")
        .distinct()
        .join(
            trainee_df,
            on=["candidate_id", "start_date", "academy_cohort"],
            how="inner"
        )
        .join(trainer_df, on="trainer_name", how="inner")
        .select("trainee_id", "trainer_id")
        .distinct()
    )

    return (
        base_df
        .crossJoin(week_df)
        .withColumn(
            "review_id",
            F.row_number().over(Window.orderBy("trainee_id", "week"))
        )
        .select("review_id", "trainee_id", "trainer_id", "week")
    )


def create_score_table(spark: SparkSession) -> DataFrame:
    academy_df = spark.table("all_academy")

    academy_base = _academy_base_with_candidate(spark)
    trainee_df = create_trainee_table(spark)
    trainer_df = create_trainer_table(spark)
    weekly_review_df = create_weekly_review_table(spark)
    competency_df = create_competency_table(spark)

    competency_columns = [
        column
        for column in academy_base.columns
        if "_W" in column
    ]

    # Force all competency columns to the same datatype
    for column in competency_columns:
        academy_base = academy_base.withColumn(
            column,
            F.col(column).cast("double")
        )

    stack_expr = ", ".join(
        f"'{column}', `{column}`"
        for column in competency_columns
    )

    score_df = academy_base.selectExpr(
        "candidate_id",
        "start_date",
        "academy_cohort",
        "trainer_name",
        f"""
        stack(
            {len(competency_columns)},
            {stack_expr}
        ) as (competency_week, score_value)
        """
    )

    score_df = (
        score_df
        .withColumn(
            "competency_name",
            F.split(
                F.col("competency_week"),
                "_W"
            )[0]
        )
        .withColumn(
            "week",
            F.split(
                F.col("competency_week"),
                "_W"
            )[1].cast("int")
        )
    )

    return (
        score_df
        .filter(
            F.col("score_value").isNotNull()
        )
        .join(
            trainee_df,
            on=[
                "candidate_id",
                "start_date",
                "academy_cohort"
            ],
            how="inner"
        )
        .join(
            trainer_df,
            on="trainer_name",
            how="inner"
        )
        .join(
            weekly_review_df,
            on=[
                "trainee_id",
                "trainer_id",
                "week"
            ],
            how="inner"
        )
        .join(
            competency_df,
            on="competency_name",
            how="inner"
        )
        .select(
            "review_id",
            "competency_id",
            F.col("score_value")
             .cast("int")
             .alias("score_value")
        )
        .dropDuplicates(
            ["review_id", "competency_id"]
        )
        .withColumn(
            "score_id",
            F.row_number().over(
                Window.orderBy(
                    "review_id",
                    "competency_id"
                )
            )
        )
        .select(
            "score_id",
            "review_id",
            "competency_id",
            "score_value"
        )
    )
