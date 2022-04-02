from typing import Optional

import dns.name
import dotenv
from fastapi import FastAPI, Response, status
from pydantic import BaseModel

from greenscreen.db import DBConnection
from greenscreen.models import ScoreSummary, Company
from greenscreen.scores import guess_company_name, get_scores_for_company, \
    calculate_main_score

dotenv.load_dotenv()

main = FastAPI()


class ErrorInfo(BaseModel):
    message: str


@main.get("/scores")
def scores_query(domain_name: str):
    db = DBConnection()

    company_name = guess_company_name(db, dns.name.from_text(domain_name))
    if company_name:
        score_components = get_scores_for_company(db, company_name)
        return ScoreSummary(
            company=Company(name=company_name),
            main_score=calculate_main_score(score_components),
            score_components=score_components,
        )
    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
