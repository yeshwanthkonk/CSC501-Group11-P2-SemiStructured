import json
import pandas as pd

fh_git = open("./datafiles/movies.json", encoding="utf-8")
kaggle_data = pd.read_csv("./datafiles/movies_metadata.csv")
git_data = json.load(fh_git)
rat = pd.read_csv("./datafiles/ratings_small.csv")

rat = pd.DataFrame({"movieId": rat["movieId"], "rating": rat["rating"]})

k = [e["title"] +"_"+ str(e["year"]) for e in git_data]
l = kaggle_data["title"]+"_"+kaggle_data["release_date"].apply(lambda x: str(x)[-4:])

q = pd.merge(kaggle_data[["id", "revenue", "title", "genres", "budget", "popularity"]], rat[["movieId", "rating"]], how='inner', left_on=['id'], right_on=['movieId'])

q.to_csv("./datafiles/movies_rating.csv")

ratings_merge = pd.merge(kaggle_data[["id", "revenue", "title", "genres", "budget", "popularity"]], rat[["movieId", "rating"]], how='inner', left_on=['id'], right_on=['movieId'])
ratings_merge = ratings_merge.loc[(ratings_merge.revenue > 10000) & (ratings_merge.budget > 1000)]

k = [e["title"] for e in git_data]
l = kaggle_data["title"]
