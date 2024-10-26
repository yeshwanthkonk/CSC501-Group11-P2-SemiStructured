import ast

import pandas as pd
import multiprocessing as mp

LARGE_FILE = "./datafiles/credits.csv"
CHUNK_SIZE = 10000

def extract_directors(director_df):
    return director_df.crew.apply(
        lambda item: [each["name"]+"-"+str(each["id"]) for each in ast.literal_eval(item) if each["job"]=="Director"]
    )


def start():
    reader = pd.read_table(LARGE_FILE, sep=",", chunksize=CHUNK_SIZE)
    pool = mp.Pool(mp.cpu_count())

    pool_list = []
    for df in reader:
        f = pool.apply_async(extract_directors,[df])
        pool_list.append(f)

    result = pd.DataFrame()
    for f in pool_list:
        result = pd.concat([result, f.get(timeout=10)])
if __name__ == "__main__":
    start()
