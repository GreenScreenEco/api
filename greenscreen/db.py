import os
from typing import Optional

import psycopg2
import sqllex as sx


class DBConnection:

    def __init__(self, dsn: Optional[str] = None) -> None:
        super().__init__()

        if dsn is None:
            dsn = os.environ.get("GS_DATABASE")

        self._connect(dsn=dsn)

    def _connect(self, dsn: str) -> None:
        """
        :param dsn: PostgreSQL Data Source Name
        """
        self._postgres = psycopg2.connect(dsn=dsn)
        self._sx = sx.PostgreSQLx(engine=psycopg2, connection=self._postgres)

    def raw(self):
        return self._postgres

    def handle(self) -> sx.PostgreSQLx:
        return self._sx

