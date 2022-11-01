import dotenv
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from greenscreen.companies import query_companies_handler
from greenscreen.scores import get_scores_handler

dotenv.load_dotenv()

main = FastAPI()

main.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@main.get("/v1/scores")
def scores_query(domain_name: str):
    return get_scores_handler.scores_query(domain_name)


@main.get("/v1/companies")
def companies_query(search_text: str):
    return query_companies_handler.companies_query(search_text)
