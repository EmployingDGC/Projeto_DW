from pandas.core.config_init import table_schema_cb
from sqlalchemy.engine.mock import MockConnection

import pandas as pd


def convert_table_to_dataframe(conn_input: MockConnection,
                               schema_name: str,
                               table_name: str,
                               columns_name: list[str] = None) -> pd.DataFrame:
    return pd.DataFrame(
        [x for x in conn_input.execute(
            f" select * from \"{schema_name}\".\"{table_name}\""
        )],
        columns=columns_name
    )
