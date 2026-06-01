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


## Project Management


### Trello Board

Manage project backlog, sprint planning, task assignments, and progress tracking.

[Trello Scrum Board](https://trello.com/b/O4sMbjqY)

---

## Database Design

### Entity Relationship Diagram (ERD)

The database design and table relationships are documented in the ERD.

[Lucidchart ERD](https://lucid.app/lucidchart/1b20898e-7bbb-4a2b-a74b-ddf758de2c46/edit?invitationId=inv_dccf5f6c-687c-4862-9100-6ad4f48f462c&page=0_0#)

---

## Documentation

### Documentation Guide

Project standards, notation conventions, and supporting documentation.

[Documentation Guide](https://docs.google.com/document/d/1jaSOddx5mvyZotscgOYWtfAxV1iPI86eR_tOPjIF16Y/edit?tab=t.0)
