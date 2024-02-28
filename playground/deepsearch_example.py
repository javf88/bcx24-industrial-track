import deepsearch as ds
from deepsearch.cps.client.components.data_indices import (
    ElasticProjectDataCollectionSource,
)
from deepsearch.cps.client.components.elastic import ElasticProjectDataCollectionSource
from deepsearch.cps.queries import CorpusSemanticQuery
from deepsearch.cps.queries.results import RAGResult, SearchResult, SearchResultItem
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel

from typing import Annotated

import deepsearch as ds
from deepsearch.core.client.settings import ProfileSettings
from fastapi import Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic(
    description="Provide the email and api_key for connecting to Deep Search."
)


async def get_deepsearch_api(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)]
):
    """Initialize the Deep Search Toolkit with the default env settings"""

    settings = ProfileSettings(
        username=credentials.username,
        api_key=credentials.password,
        host="https://sds.app.accelerate.science/",
        verify_ssl=True,
    )
    api = ds.CpsApi.from_settings(settings=settings)

    return api

app = FastAPI()


class QueryResponseResultItem(BaseModel):
    doc_hash: str
    path_in_doc: str
    passage: str
    source_is_text: bool


class QueryResponse(BaseModel):
    results: list[QueryResponseResultItem]


@app.get("/")
async def read_root() -> dict:
    """
    Root hello world endpoint
    """
    return {"Hello": "World"}


@app.get(
    "/query/documents/private/{proj_key}/{index_key}",
    description="Run the semantic query on a private collection.",
)
async def query_private_documents(
    proj_key: str,
    index_key: str,
    query: str,
    num_items: int = 10,
    api: ds.CpsApi = Depends(get_deepsearch_api),
) -> QueryResponse:
    """
    Run the semantic query on a private collection
    """

    question_query = CorpusSemanticQuery(
        question=query,
        project=proj_key,
        index_key=index_key,
        # optional params:
        retr_k=num_items,
        # text_weight=TEXT_WEIGHT,
        # rerank=RERANK,
    )
    api_output = api.queries.run(question_query)
    search_result = SearchResult.from_api_output(api_output)
    for item in search_result.search_result_items:
        print(item)
    
    return QueryResponse(
        results=[
            QueryResponseResultItem.model_validate_json(item.json())
            for item in search_result.search_result_items
        ]
    )

if __name__ == "__main__":
    api = ds.CpsApi.from_env()
    import asyncio
    PROJ_KEY = '628052f6ea6d4f03c8e4f6adc50a8bf98dcc53e6'
    BBC_INDEX_KEY = '68cd3a7d7790df65d5cd02bce5c2c6f350d07a9a'
    EU_PRESS_IDX_KEY = 'c0ea43d24c5deb9c15db6308cbfb175b7b32aeb4'
    asyncio.run(query_private_documents(PROJ_KEY, BBC_INDEX_KEY, "Geopolitical situation", api=api))
    pass