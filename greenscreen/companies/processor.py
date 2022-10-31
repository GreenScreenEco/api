from typing import List, Tuple, Optional

from greenscreen.db import DBConnection
from greenscreen.ingest import SRayEnterpriseSource


def find_company_names(db: DBConnection, search_text: str = None) -> List[str]:
    sray_table = db.handle()[SRayEnterpriseSource.table_name]
    rows = sray_table.select(
        SELECT=[
            sray_table["name"]])

    def match_company(subject: str, search: str) -> Optional[int]:
        """Returns a match priority for the company name, with 0 being best match."""
        if search in subject:
            return 0
        for search_text_part in search.split():
            if search_text_part in subject:
                return 1
        return None

    matched_names: List[Tuple[int, str]] = []
    for row in rows:
        company_name: str = row[0]
        if search_text is not None:
            match_priority = match_company(company_name.lower(), search_text.lower())
            if match_priority is not None:
                matched_names.append((match_priority, company_name))
        else:
            matched_names.append((0, company_name))

    matched_names.sort()

    return [match[1] for match in matched_names]

