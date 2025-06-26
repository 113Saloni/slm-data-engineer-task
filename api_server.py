from fastapi import FastAPI
import pandas as pd
import os
import glob

app = FastAPI()

def get_spark_csv_file():
    """Find Spark's part-*.csv file from data/processed_data/ folder."""
    csv_files = glob.glob("data/processed_data/part-*.csv")
    if not csv_files:
        return None
    return csv_files[0]

@app.get("/")
def read_root():
    return {"message": "SLM API is running 🚀"}

@app.get("/movies")
def get_movies(limit: int = 10):
    file_path = get_spark_csv_file()
    if not file_path or not os.path.exists(file_path):
        return {"error": "CSV part file not found."}
    if not os.access(file_path, os.R_OK):
        return {"error": "CSV part file is not readable."}

    try:
        df = pd.read_csv(
            file_path,
            sep=",",
            quotechar='"',
            escapechar="\\",
            engine="python",
            on_bad_lines="skip"
        )
        return df.head(limit).to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

@app.get("/summary")
def get_summary():
    file_path = get_spark_csv_file()
    if not file_path or not os.path.exists(file_path):
        return {"error": "CSV part file not found."}
    if not os.access(file_path, os.R_OK):
        return {"error": "CSV part file is not readable."}

    try:
        df = pd.read_csv(
            file_path,
            sep=",",
            quotechar='"',
            escapechar="\\",
            engine="python",
            on_bad_lines="skip"
        )

        for col in ["budget", "vote_average"]:
            if col not in df.columns:
                return {"error": f"Missing expected column: {col}"}

        df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
        df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")

        summary = {
            "total_movies": int(len(df)),
            "avg_budget": round(df["budget"].mean(), 2),
            "avg_rating": round(df["vote_average"].mean(), 2)
        }
        return summary

    except Exception as e:
        return {"error": str(e)}

# ✅ New: Search endpoint
@app.get("/search")
def search_movies(title: str):
    file_path = get_spark_csv_file()
    if not file_path or not os.path.exists(file_path):
        return {"error": "CSV part file not found."}
    if not os.access(file_path, os.R_OK):
        return {"error": "CSV part file is not readable."}

    try:
        df = pd.read_csv(
            file_path,
            sep=",",
            quotechar='"',
            escapechar="\\",
            engine="python",
            on_bad_lines="skip"
        )
        if "title" not in df.columns:
            return {"error": "Missing 'title' column in data."}

        matches = df[df["title"].str.contains(title, case=False, na=False)]

        if matches.empty:
            return {"message": f"No movies found for title containing '{title}'."}

        return matches.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

