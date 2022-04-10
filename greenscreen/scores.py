import heapq
from difflib import SequenceMatcher
from typing import Optional, Set, Iterable, Dict, List

import dns.name
import Levenshtein

from greenscreen.db import DBConnection
from greenscreen.ingest import SRayEnterpriseSource
from greenscreen.models import Score, ScoreSource


def get_scores_for_company(db: DBConnection, company_name: str) -> List[Score]:
    sray_table = db.handle()[SRayEnterpriseSource.table_name]
    rows = sray_table.select(
        SELECT=[
            sray_table["gc_hr"],
            sray_table["gc_lr"],
            sray_table["gc_en"],
        ],
        WHERE=(sray_table["name"] == company_name))

    scores = []
    for row in rows:
        human_score = row[0]  # gc_hr
        if human_score:
            scores.append(Score(
                value=human_score,
                min=0,
                max=100,
                label="Human",
                description="Human Rights, Labour Rights, Occupational Health and "
                            "Safety, Employment Quality, Diversity; Product Quality "
                            "and Safety, Product Access, Community Relations",
                source=SRayEnterpriseSource.score_source,
            ))
        labor_score = row[1]  # gc_lr
        if labor_score:
            scores.append(Score(
                value=labor_score,
                min=0,
                max=100,
                label="Labor",
                description="Labor Rights, Occupational Health and Safety, Diversity, "
                            "Compensation, Training and Development, Employment Quality",
                source=SRayEnterpriseSource.score_source,
            ))
        environment_score = row[2]  # gc_en
        if environment_score:
            scores.append(Score(
                value=environment_score,
                min=0,
                max=100,
                label="Environment",
                description="Emissions, Waste, Environmental Stewardship, "
                            "Environmental Management, Resource Use, Water, "
                            "Environmental Solutions",
                source=SRayEnterpriseSource.score_source,
            ))

    return scores


def calculate_main_score(scores: List[Score]) -> Score:
    score_source = ScoreSource(
        identity="GreenScreen, LLC",
        description="Hold the companies you shop at accountable for sustainability.")

    scaled_scores = []
    for input_score in scores:
        scaled = input_score.value - input_score.min
        scaled *= 100 / (input_score.max - input_score.min)
        scaled_scores.append(scaled)

    return Score(
        value=sum(scaled_scores) / len(scaled_scores),
        min=0,
        max=100,
        label="Main Score",
        description="The collective knowledge of our data sources summarized into one "
                    "score.",
        source=score_source,
    )


def guess_company_name(db: DBConnection, domain_name: dns.name.Name) -> Optional[str]:
    all_company_names = _load_all_company_names(db)

    # Check each individual label in the domain name, plus the full name without dots.
    targets = [domain_name.to_text().replace(".", " ")]
    d = domain_name
    while d != dns.name.root:
        # Skip the topmost non-root label, as that is always an organizational label.
        if d.parent() != dns.name.root:
            # Skip common most-significant labels.
            if d[0] != "www":
                targets += str(d[0])
        d = d.parent()

    # Compare every target against every candidate
    best_score_each_candidate = {}
    for target in targets:
        for candidate in all_company_names:
            score = _get_similarity_score(target, candidate)
            if candidate not in best_score_each_candidate:
                best_score_each_candidate[candidate] = score
            elif score < best_score_each_candidate[candidate]:
                best_score_each_candidate[candidate] = score

    # Sort all candidates
    best_candidates = []
    for candidate, best_score in best_score_each_candidate.items():
        heapq.heappush(best_candidates, (best_score, candidate))

    # Get the best-scoring candidate
    best_score, best_candidate = best_candidates[0]
    # print(f"Best score for domain {domain_name.to_text()} is {best_candidate} at "
    #       f"{best_score}")

    # Attempt to filter false positives
    if best_score < 0.5:
        return best_candidate
    else:
        return None


def _get_similarity_score(target: str, company_name: str) -> float:
    return 1 - SequenceMatcher(
        None,
        a=target.lower(),
        b=_simplify_company_name(company_name)
    ).ratio()


def _simplify_company_name(name: str) -> str:
    name = name.lower()
    name = name.replace(".", "")
    name = name.replace(",", "")
    name = name.replace("/", "")
    name = " ".join(filter(lambda part: len(part) > 2, name.split(" ")))
    name = (name + " ").replace(" plc ", "")
    name = (name + " ").replace(" ltd ", "")
    name = (name + " ").replace(" llc ", "")
    name = (name + " ").replace(" sca ", "")
    name = (name + " ").replace(" kgaa ", "")
    name = (name + " ").replace(" gmbh ", "")
    return name.strip()


def _load_all_company_names(db: DBConnection) -> Set[str]:
    table = db.handle()[SRayEnterpriseSource.table_name]
    rows = table.select_distinct(SELECT=[table["name"]])
    return {row[0] for row in rows}
