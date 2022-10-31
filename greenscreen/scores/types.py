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
