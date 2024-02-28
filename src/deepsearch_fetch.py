import pandas as pd
import threading
import datetime as dt
from tqdm import tqdm
import deepsearch as ds
from numerize.numerize import numerize
from deepsearch.cps.queries import DataQuery
from deepsearch.cps.client.components.elastic import ElasticProjectDataCollectionSource


class ArticleFetcher:
    def __init__(self, project_key, index_key, date_limit):
        self.project_key_ = project_key
        self.index_key_ = index_key
        self.date_limit = date_limit
        self.collection_coordinates = ElasticProjectDataCollectionSource(
            proj_key=self.project_key_, index_key=self.index_key_)

    def prepare_query(self, query) -> DataQuery:
        return DataQuery(search_query=query, source=['*'], limit=100, coordinates=self.collection_coordinates)

    def fetch_articles(self, query) -> pd.DataFrame:
        q = self.prepare_query(query)
        cursor = api.queries.run_paginated_query(q)
        # Iterate through query results
        results = []
        for result_page in tqdm(cursor):
            for row in result_page.outputs["data_outputs"]:
                text = ''
                for txt in row["_source"]["main-text"]:
                    text += txt['text']
                # Add row to results table
                date = row["_source"]["main-text"][1]['text']
                try:
                    date = dt.datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z')
                    if date > self.date_limit:
                        results.append({
                            "Title": row["_source"]["main-text"][0]['text'],
                            "Date": date,
                            "Text": text
                        })
                except:
                    pass
        print(f'found {len(results)} articles regarding topic: {query}')
        results = sorted(results, key=lambda x: x["Date"], reverse=True)
        return pd.DataFrame(results)


    def fetch_articles_raw(self, query) -> pd.DataFrame:
        q = self.prepare_query(query)
        cursor = api.queries.run_paginated_query(q)
        results = []
        for result_page in tqdm(cursor):
            for row in result_page.outputs["data_outputs"]:
                text = ''
                for txt in row["_source"]["main-text"]:
                    text += txt['text'] if "text" in txt else ""
                # Add row to results table
                results.append({
                    "Title": row["_source"]["main-text"][0]['text'],
                    "Text": text
                })
        return pd.DataFrame(results)

if __name__ == "__main__":
    api = ds.CpsApi.from_env()
    PROJ_KEY = '628052f6ea6d4f03c8e4f6adc50a8bf98dcc53e6'
    BBC_INDEX_KEY = '68cd3a7d7790df65d5cd02bce5c2c6f350d07a9a'
    EU_PRESS_IDX_KEY = 'c0ea43d24c5deb9c15db6308cbfb175b7b32aeb4'

    def fetch_and_save(fetcher, query):
        df = fetcher.fetch_articles(query)
        df.to_csv("news_headers_df.csv", index=False)
    
    articleFetcherBBC = ArticleFetcher(PROJ_KEY, BBC_INDEX_KEY, dt.datetime(2022, 1, 1))
    articleFetcherEUPress = ArticleFetcher(PROJ_KEY, EU_PRESS_IDX_KEY, dt.datetime(2022, 1, 1))

    fetcher_threads = []
    
    thbbc = threading.Thread(target=fetch_and_save, args=(articleFetcherBBC,'Ukraine'))
    fetcher_threads.append(thbbc)

    
    def fetch_and_save(fetcher, query):
        df = fetcher.fetch_articles_raw(query)
        df.to_csv("news_headers.csv", index=False)
    theupress = threading.Thread(target=fetch_and_save, args=(articleFetcherEUPress,'Ukraine'))
    fetcher_threads.append(theupress)

    for th in fetcher_threads:
        th.start()
    
    # coll_coords = ElasticProjectDataCollectionSource(proj_key=PROJ_KEY, index_key=EU_PRESS_IDX_KEY)
    # # Prepare the data query
    # query = DataQuery(
    #     search_query="Ukraine",  # The search query to be executed
    #     source=["*"],
    #     limit=100,
    #     coordinates=coll_coords,  # The data collection to be queries
    # )
    # # Query Deep Search for the documents matching the query
    # results = []
    # query_results = api.queries.run(query)
    # for row in query_results.outputs["data_outputs"]:
    #     text = ''
    #     for txt in row["_source"]["main-text"]:
    #         text += txt['text'] if "text" in txt else ""
    #     # Add row to results table
    #     results.append({
    #         "Title": row["_source"]["main-text"][0]['text'],
    #         "Text": text
    #     })

    # print(f'Finished fetching all data. Total is {len(results)} records.')

    # dataframe = pd.DataFrame(results)
    # dataframe.to_csv("news_headers_df.csv", index=False)
