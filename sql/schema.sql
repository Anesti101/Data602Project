DROP TABLE IF EXISTS score;
DROP TABLE IF EXISTS weekly_review;
DROP TABLE IF EXISTS trainee;
DROP TABLE IF EXISTS candidate_weakness;
DROP TABLE IF EXISTS candidate_strength;
DROP TABLE IF EXISTS candidate_technology;
DROP TABLE IF EXISTS interview;
DROP TABLE IF EXISTS assessment;
DROP TABLE IF EXISTS competency;
DROP TABLE IF EXISTS trainer;
DROP TABLE IF EXISTS weakness;
DROP TABLE IF EXISTS strength;
DROP TABLE IF EXISTS technology;
DROP TABLE IF EXISTS candidate;

CREATE TABLE candidate (
    candidate_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(50),
    dob DATE,
    email VARCHAR(320),
    city VARCHAR(255),
    address TEXT,
    postcode VARCHAR(20),
    phone_num VARCHAR(50),
    uni VARCHAR(255),
    degree VARCHAR(50),
    invited_date DATE,
    invited_by VARCHAR(255)
);

CREATE TABLE technology (
    technology_id INTEGER PRIMARY KEY,
    technology_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE strength (
    strength_id INTEGER PRIMARY KEY,
    strength_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE weakness (
    weakness_id INTEGER PRIMARY KEY,
    weakness_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE trainer (
    trainer_id INTEGER PRIMARY KEY,
    trainer_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE competency (
    competency_id INTEGER PRIMARY KEY,
    competency_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE assessment (
    assessment_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    psychometric_score INTEGER,
    presentation_score INTEGER,
    assessment_date DATE,
    location VARCHAR(255)
);

CREATE TABLE interview (
    interview_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    interview_date DATE,
    result VARCHAR(50),
    course_interest VARCHAR(255)
);

CREATE TABLE candidate_technology (
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    technology_id INTEGER NOT NULL REFERENCES technology(technology_id),
    score INTEGER,
    PRIMARY KEY (candidate_id, technology_id)
);

CREATE TABLE candidate_strength (
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    strength_id INTEGER NOT NULL REFERENCES strength(strength_id),
    PRIMARY KEY (candidate_id, strength_id)
);

CREATE TABLE candidate_weakness (
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    weakness_id INTEGER NOT NULL REFERENCES weakness(weakness_id),
    PRIMARY KEY (candidate_id, weakness_id)
);

CREATE TABLE trainee (
    trainee_id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidate(candidate_id),
    start_date DATE NOT NULL,
    academy_cohort VARCHAR(100) NOT NULL,
    UNIQUE (candidate_id, start_date, academy_cohort)
);

CREATE TABLE weekly_review (
    review_id INTEGER PRIMARY KEY,
    trainee_id INTEGER NOT NULL REFERENCES trainee(trainee_id),
    trainer_id INTEGER NOT NULL REFERENCES trainer(trainer_id),
    week INTEGER NOT NULL,
    UNIQUE (trainee_id, trainer_id, week)
);

CREATE TABLE score (
    score_id INTEGER  PRIMARY KEY,
    review_id INTEGER NOT NULL REFERENCES weekly_review(review_id),
    competency_id INTEGER NOT NULL REFERENCES competency(competency_id),
    score_value INTEGER,
    UNIQUE (review_id, competency_id)
);
