import dotenv
from fastapi import FastAPI

from greenscreen.scores import get_scores_handler

dotenv.load_dotenv()

main = FastAPI()


@main.get("/v1/scores")
def scores_query(domain_name: str):
    return get_scores_handler.scores_query(domain_name)
