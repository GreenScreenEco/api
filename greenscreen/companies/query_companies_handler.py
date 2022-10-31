from typing import List

from pydantic import BaseModel
from starlette import status
from starlette.responses import Response

from greenscreen.companies.processor import find_company_names
from greenscreen.db import DBConnection
from greenscreen.scores.calculation import get_scores_for_company, calculate_main_score
from greenscreen.scores.types import Score


class ScoreSummary(BaseModel):
    main_score: Score
    score_components: List[Score]


class CompanyDetails(BaseModel):
    name: str
    scores: ScoreSummary


class CompaniesQueryResponse(BaseModel):
    results: List[CompanyDetails]


def companies_query(search_text: str = None):
    db = DBConnection()

    company_names = find_company_names(db, search_text)
    company_details: List[CompanyDetails] = []
    for company_name in company_names:
        score_components = get_scores_for_company(db, company_name)
        company_details.append(CompanyDetails(
            name=company_name,
            scores=ScoreSummary(
                main_score=calculate_main_score(score_components),
                score_components=score_components,
            )
        ))

    response = CompaniesQueryResponse(results=company_details)
    if len(response.results) > 0:
        return response
    else:
        return Response(content=response, status_code=status.HTTP_204_NO_CONTENT)
