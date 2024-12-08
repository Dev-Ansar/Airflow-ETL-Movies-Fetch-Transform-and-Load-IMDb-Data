import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Load environment variables from the .env file
load_dotenv()

# Fetch RapidAPI key
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")



# Function to fetch movie data (Extract)
def get_imdb_movies_data(ti):
    print(f"Using RapidAPI Key: {RAPIDAPI_KEY}")
    url = "https://imdb-top-100-movies.p.rapidapi.com/"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "imdb-top-100-movies.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # Extract key details into a DataFrame
        movies = []
        for movie in data:
            movies.append({
                "Rank": movie.get("rank"),
                "Title": movie.get("title"),
                "Year": movie.get("year"),
                "Rating": float(movie.get("rating", 0)),
                "Genre": movie.get("genre", "Unknown"),
                "Description": movie.get("description", ""),
                "IMDB_Link": movie.get("imdb_link")
            })

        df = pd.DataFrame(movies)

        # Push the raw DataFrame to XCom for transformations
        ti.xcom_push(key='raw_movie_data', value=df.to_dict('records'))
    else:
        raise ValueError(f"Failed to fetch data: {response.status_code}")

# Function to transform movie data (Transform)
def transform_movie_data(ti):
    # Pull raw data from XCom
    raw_movie_data = ti.xcom_pull(key='raw_movie_data', task_ids='fetch_movie_data')
    if not raw_movie_data:
        raise ValueError("No raw movie data found")

    df = pd.DataFrame(raw_movie_data)

    # Basic Transformations
    df["Rating"] = pd.to_numeric(df["Rating"])
    df["Era"] = df["Year"].apply(lambda x: "Classic" if x < 2000 else "Modern")
    transformed_df = df[df["Rating"] > 8.5].sort_values(by="Rank")

    # Push transformed data to XCom
    ti.xcom_push(key='transformed_movie_data', value=transformed_df.to_dict('records'))

# Function to load transformed data into PostgreSQL (Load)
def insert_movies_into_postgres(ti):
    movie_data = ti.xcom_pull(key='transformed_movie_data', task_ids='transform_movie_data')
    if not movie_data:
        raise ValueError("No transformed movie data found")

    postgres_hook = PostgresHook(postgres_conn_id='movies_db_conn')
    insert_query = """
    INSERT INTO movies (rank, title, year, rating, genre, description, imdb_link, era)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for movie in movie_data:
        postgres_hook.run(insert_query, parameters=(
            movie['Rank'], movie['Title'], movie['Year'], movie['Rating'],
            movie['Genre'], movie['Description'], movie['IMDB_Link'], movie['Era']
        ))

# Default arguments for the DAG
default_args = {
    'owner': 'movie-airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fetch_and_store_imdb_movies',
    default_args=default_args,
    description='A simple DAG to fetch movie data from IMDb and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

# Tasks
fetch_movie_data_task = PythonOperator(
    task_id='fetch_movie_data',
    python_callable=get_imdb_movies_data,
    dag=dag,
)

transform_movie_data_task = PythonOperator(
    task_id='transform_movie_data',
    python_callable=transform_movie_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='movies_db_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        rank INT,
        title TEXT NOT NULL,
        year INT,
        rating FLOAT,
        genre TEXT,
        description TEXT,
        imdb_link TEXT,
        era TEXT
    );
    """,
    dag=dag,
)

insert_movie_data_task = PythonOperator(
    task_id='insert_movie_data',
    python_callable=insert_movies_into_postgres,
    dag=dag,
)

# Task dependencies
fetch_movie_data_task >> transform_movie_data_task >> create_table_task >> insert_movie_data_task