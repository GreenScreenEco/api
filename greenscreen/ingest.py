import csv
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Optional

import click
import dotenv
import sqllex as sx
from psycopg2.errors import DuplicateTable, IntegrityError

from greenscreen.db import DBConnection
from greenscreen.scores.types import ScoreSource


class ScoreInputSchema(ABC):

    @abstractmethod
    def ingest(self, db: DBConnection, filestream):
        pass


class SRayEnterpriseSource(ScoreInputSchema):
    score_source = ScoreSource(
        identity="Arabesque S-Ray Enterprise ESG",
        description="Arabesque S-Ray is a global financial services company that "
                    "focuses on advisory and data solutions by combining big data and "
                    "environmental, social and governance (ESG) metrics to assess the "
                    "performance and sustainability of publicly listed companies "
                    "worldwide."
    )

    table_name = "datasource_s_ray_enterprise"

    def ingest(self, db: DBConnection, filestream):
        self.create_table(db)
        table = db.handle()[self.table_name]
        csvreader = csv.DictReader(
            filestream,
            fieldnames=None,  # Use the first row for column names
            restkey="extra",  # Group cells beyond the detected columns into this column
            restval="",  # Fill empty cells with an empty string
            delimiter=",",
            quotechar='"')
        n_success = 0
        n_fail = 0

        # The file starts with a unicode control character (byte order mark), it must be
        # removed else it will be considered part of the column name.
        fixed_fieldnames = list(csvreader.fieldnames)
        fixed_fieldnames[0] = "date"
        csvreader.fieldnames = fixed_fieldnames

        for row in csvreader:
            try:
                if "extra" in row:
                    print(f"Row had extra data, truncating: {row}")
                    del row["extra"]
                table.insert(**row)
                n_success += 1
            except IntegrityError:
                print(f"Unable to insert row: {row}")
                db.raw().rollback()
                n_fail += 1

        print(f"Ingestion complete! Rows succeeded: {n_success}, Rows failed: {n_fail}")

    def create_table(self, db: DBConnection):
        db.handle().create_table(
            self.table_name,
            {
                "date": [sx.DATE, sx.NOT_NULL],
                "name": [sx.TEXT, sx.NOT_NULL],
                "ticker": sx.TEXT,
                "dom_region": sx.TEXT,
                "dom_country_iso": sx.TEXT,
                "economic_sector": sx.TEXT,
                "industry": sx.TEXT,
                "esg": sx.NUMERIC,
                "esg_e": sx.NUMERIC,
                "esg_s": sx.NUMERIC,
                "esg_g": sx.NUMERIC,
                "gc": sx.NUMERIC,
                "gc_hr": sx.NUMERIC,
                "gc_lr": sx.NUMERIC,
                "gc_en": sx.NUMERIC,
                "gc_ac": sx.NUMERIC,
            },
            IF_NOT_EXIST=True)
        # Sqllex does not properly support UNIQUE in the create table function, so
        # we must add it after with raw postgres commands.
        try:
            c = db.raw().cursor()
            c.execute(f"ALTER TABLE {self.table_name} "
                      "ADD CONSTRAINT date_name_unique UNIQUE (date, name)")
            db.raw().commit()
        except DuplicateTable:
            db.raw().rollback()
            pass


class DataSource(Enum):
    SRAY_ENTERPRISE = "s-ray"

    def get_input_schema(self) -> ScoreInputSchema:
        if self == DataSource.SRAY_ENTERPRISE:
            return SRayEnterpriseSource()
        else:
            raise ValueError(f"No input schema available for: {repr(self)}")


class DataSourceParam(click.ParamType):
    name = "DATA_SOURCE"

    def convert(
            self,
            value: Any,
            param: Optional[click.Parameter],
            ctx: Optional[click.Parameter],
    ) -> Any:
        try:
            return DataSource(value)
        except ValueError:
            self.fail(f"Source must be one of: {','.join(DataSource)}")


@click.command()
@click.option(
    "--connection",
    type=str,
    envvar="GS_DATABASE",
    help="PostgreSQL database connection string (DSN).",
    show_envvar=True)
@click.option(
    "--source",
    type=DataSourceParam(),
    required=True,
    help=f"Data source. One of: ({','.join(map(lambda s: s.value, DataSource))})")
@click.option(
    "--file",
    required=True,
    help="Path to a data file from that data source.")
def ingest_cmdline(connection: str, source: DataSource, file: str):
    db = DBConnection(dsn=connection)

    try:
        with open(file, "r") as filestream:
            source.get_input_schema().ingest(db, filestream)
    except FileNotFoundError:
        print(f"Unable to open file: {file}")


if __name__ == "__main__":
    dotenv.load_dotenv()
    ingest_cmdline()
