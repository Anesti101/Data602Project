# Sparta Global Recruitment and Academy ETL Pipeline

## Project Overview

This project involves designing and developing an end to end ETL (Extract, Transform, Load) pipeline for a fictionalised version of Sparta Global's recruitment and academy process.

The pipeline will extract data from multiple source files containing candidate applications, Sparta Day assessments, interview results, academy enrolment records, and trainee performance reviews. The data will then be validated, cleaned, transformed, and loaded into a fully normalised PostgreSQL database.

The goal is to create a reliable and scalable data platform that supports efficient data management and provides a foundation for future reporting, analytics, and dashboard development.

## Technologies

- Python
- Pandas
- PostgreSQL
- SQL
- Git & GitHub
- Trello
- Agile Scrum Methodology

## Project Management

### Trello Board

[Trello Scrum Board](https://trello.com/b/O4sMbjqY)

This board is used to manage the project backlog, sprint planning, task assignments, and progress tracking throughout the development lifecycle.

## Project Structure

```text
sparta-etl-project/
│
├── README.md
│
├── docs/
│   ├── project-plan.md
│   ├── user-stories.md
│   └── erd.png
│
├── sql/
│   ├── schema.sql
│   └── queries.sql
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── tests/
│   └── test_etl.py
│
└── databricks/
    ├── ingest.py
    ├── transform.py
    └── load.py
```
