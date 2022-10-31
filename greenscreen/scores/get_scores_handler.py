from typing import List

import dns.name
from pydantic import BaseModel
from starlette import status
from starlette.responses import Response

from greenscreen.db import DBConnection
from greenscreen.scores.calculation import guess_company_name, get_scores_for_company, \
    calculate_main_score
from greenscreen.scores.types import Score, Company


class ScoresQueryResponse(BaseModel):
    company: Company
    main_score: Score
    score_components: List[Score]


def scores_query(domain_name: str):
    db = DBConnection()

    company_name = guess_company_name(db, dns.name.from_text(domain_name))
    if company_name:
        score_components = get_scores_for_company(db, company_name)
        return ScoresQueryResponse(
            company=Company(name=company_name),
            main_score=calculate_main_score(score_components),
            score_components=score_components,
        )
    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
