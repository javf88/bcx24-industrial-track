import pandas as pd
import threading
import datetime as dt
from tqdm import tqdm
import deepsearch as ds
from deepsearch.cps.queries import DataQuery
from deepsearch.cps.client.components.elastic import ElasticProjectDataCollectionSource


class ArticleFetcher:
    """
    A class to fetch articles from a Deep Search project.

    Attributes:
    - project_key (str): The key of the Deep Search project.
    - index_key (str): The key of the index to fetch articles from.
    - date_limit (datetime): The date limit to filter fetched articles.
    """

    def __init__(self, project_key, index_key, date_limit):
        self.project_key_ = project_key
        self.index_key_ = index_key
        self.date_limit = date_limit
        self.collection_coordinates = ElasticProjectDataCollectionSource(
            proj_key=self.project_key_, index_key=self.index_key_)

    def prepare_query(self, query) -> DataQuery:
        """
        Prepare a query for fetching articles.

        Args:
        - query (str): The search query for articles.

        Returns:
        - DataQuery: The prepared query.
        """
        return DataQuery(search_query=query, source=['*'], limit=100, coordinates=self.collection_coordinates)

    def fetch_articles(self, query) -> pd.DataFrame:
        """
        Fetch articles based on a query.

        Args:
        - query (str): The search query for articles.

        Returns:
        - pd.DataFrame: DataFrame containing fetched articles.
        """
        q = self.prepare_query(query)
        cursor = api.queries.run_paginated_query(q)
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
        """
        Fetch raw articles without filtering based on date.

        Args:
        - query (str): The search query for articles.

        Returns:
        - pd.DataFrame: DataFrame containing fetched articles.
        """
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

    # Define function to fetch and save articles
    def fetch_and_save(fetcher, query):
        df = fetcher.fetch_articles(query)
        df.to_csv("news_headers_df.csv", index=False)

    # Create instances of ArticleFetcher for different projects
    articleFetcherBBC = ArticleFetcher(PROJ_KEY, BBC_INDEX_KEY, dt.datetime(2022, 1, 1))
    articleFetcherEUPress = ArticleFetcher(PROJ_KEY, EU_PRESS_IDX_KEY, dt.datetime(2022, 1, 1))

    # Create threads for fetching articles
    fetcher_threads = []
    
    # Thread for fetching articles related to Ukraine from BBC
    thbbc = threading.Thread(target=fetch_and_save, args=(articleFetcherBBC,'Ukraine'))
    fetcher_threads.append(thbbc)

    # Thread for fetching articles related to Ukraine from EU Press
    def fetch_and_save(fetcher, query):
        df = fetcher.fetch_articles_raw(query)
        df.to_csv("news_headers.csv", index=False)
    theupress = threading.Thread(target=fetch_and_save, args=(articleFetcherEUPress,'Ukraine'))
    fetcher_threads.append(theupress)

    # Start all threads
    for th in fetcher_threads:
        th.start()
    
    # Join all threads
    for th in fetcher_threads:
        th.join()
