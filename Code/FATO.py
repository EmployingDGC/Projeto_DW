from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import utilities as utl
import STAGES as stgs
import DEFAULTS_VALUES as DFLT


def get_all_dimensions(conn_input: MockConnection) -> list[pd.DataFrame]:
    return [
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name=DFLT.SCHEMA_NAMES[1],
            table_name=DFLT.DIMENSIONS_NAMES[0]
        ),
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name=DFLT.SCHEMA_NAMES[1],
            table_name=DFLT.DIMENSIONS_NAMES[1]
        ),
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name=DFLT.SCHEMA_NAMES[1],
            table_name=DFLT.DIMENSIONS_NAMES[2]
        )
    ]


def run(conn_output: MockConnection) -> None:
    pass
