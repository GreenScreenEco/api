from typing import List

from pydantic import BaseModel


class ScoreSource(BaseModel):
    identity: str
    description: str


class Score(BaseModel):
    value: float
    min: float
    max: float
    label: str
    description: str
    source: ScoreSource


class Company(BaseModel):
    name: str


class ScoreSummary(BaseModel):
    company: Company
    main_score: Score
    score_components: List[Score]
