# 📄 Submission.md — SLM Data Engineer Task

## 🚀 Problem Understanding

The task was to build a mini ETL pipeline using PySpark that:
- Ingests a large dataset (movies)
- Cleans and transforms the data
- Saves it in a structured format
- Exposes APIs to fetch the data

---

## ⚙️ Tools Used

- Python, PySpark
- FastAPI, Pandas
- Uvicorn (for running server)
- KaggleHub (to download dataset)

---

## 🧱 Pipeline Steps

1. **Download dataset**: Used `kagglehub` to fetch movie data.
2. **ETL pipeline (PySpark)**:
   - Loaded raw CSV
   - Dropped null values and duplicates
   - Cast budget to double
   - Saved cleaned data using Spark writer
3. **API Server (FastAPI)**:
   - Endpoint `/movies?limit=10`
   - Endpoint `/summary` for budget & rating
   - Endpoint `/search?title=Batman` to filter movie titles

---

## 🧠 Key Learnings

- Writing Spark ETL scripts from scratch
- Handling schema, nulls, and casting in PySpark
- Serving data via REST API using FastAPI
- Structuring a clean project with documentation

---

## 📈 Improvements Possible

- Add pagination in `/movies`
- Deploy API on Render or Railway
- Add tests using Pytest
- Integrate with Airflow (bonus)

---

## ✅ Conclusion

This task helped build a complete pipeline from raw data ingestion to a production-ready API. It shows ability to work with Spark, design REST APIs, and build end-to-end data apps.
