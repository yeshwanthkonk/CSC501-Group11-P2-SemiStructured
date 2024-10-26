import ast
import json

import pandas as pd
from dotenv import load_dotenv
import os

from pymongo import MongoClient

load_dotenv()


movies_df = pd.read_csv("./datafiles/final_movies_rating.csv")
director_df = pd.read_csv("./datafiles/new_director.csv")

final_etl_data = pd.merge(
    movies_df,
    director_df,
    how='inner',
    on=['id']
)

final_etl_data.rename(columns={"id": "movie_id", "rating_mean": "avg_rating"}, inplace=True)

final_etl_data = final_etl_data[["movie_id", "revenue", "title", "genres", "budget", "director", "avg_rating"]]
final_etl_data["genres"] = final_etl_data.genres.apply(lambda each: [item["name"] for item in ast.literal_eval(each)])
final_etl_data["director"] = final_etl_data.director.apply(ast.literal_eval)

final_data = json.loads(final_etl_data.to_json(orient="records"))
CONN_URL = os.getenv("CONN_URL")
collection = MongoClient(CONN_URL)[os.getenv("DATABASE_NAME")][os.getenv("COLL_NAME")]
collection.insert_many(final_data)
