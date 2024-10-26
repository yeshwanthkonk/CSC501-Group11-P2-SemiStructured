import ast

import pandas as pd

git_df = pd.read_json("./datafiles/movies.json")[["title","year"]]
kaggle_data = pd.read_csv("./datafiles/movies_metadata.csv")[["id", "revenue", "title", "genres", "budget", "release_date"]]
ratings_df = pd.read_csv("./datafiles/ratings_small.csv")
ratings_df = pd.DataFrame({"movieId": ratings_df["movieId"], "rating": ratings_df["rating"]})[["movieId", "rating"]]
director_df = pd.read_csv("./datafiles/credits.csv")[["id", "crew"]]


kaggle_data.dropna(subset=["release_date"], inplace=True)
kaggle_data = kaggle_data[~kaggle_data["id"].str.contains("-")]
kaggle_data["id"] = kaggle_data["id"].astype(int)
kaggle_data["release_date"] = pd.to_datetime(kaggle_data["release_date"])

kaggle_data["year"] = kaggle_data["release_date"].apply(lambda each: str(each)[:4]).astype(int)

final_merge = pd.merge(
    pd.merge(
        kaggle_data,
        ratings_df,
        how='inner',
        left_on=['id'],
        right_on=['movieId']
    ),
    git_df,
    how='inner',
    on=["title", "year"]
)

grouped_rating_mean = final_merge.groupby(["title", "year"]).rating.mean().reset_index(name="rating_mean")

movies_two = pd.merge(
    kaggle_data,
    git_df,
    how='inner',
    on=["title", "year"]
)

movies_ratings = pd.merge(
    movies_two,
    grouped_rating_mean,
    how='inner',
    on=["title", "year"]
)

director_df["director"] = director_df.crew.apply(lambda item: [each["name"]+"-"+str(each["id"]) for each in ast.literal_eval(item) if each["job"]=="Director"])

movies_ratings.to_csv("./datafiles/movies_rating.csv", index=False)
