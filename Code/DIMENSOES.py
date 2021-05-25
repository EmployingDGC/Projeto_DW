import pandas as pd
from sqlalchemy.engine.mock import MockConnection

import utilities as utl


def create_all_dimensions(conn_output: MockConnection,
                          schema_name: str,
                          frames: list[pd.DataFrame],
                          dimensions_names: list[str]) -> bool:
    if len(frames) != len(dimensions_names):
        return False

    utl.create_schema(
        database=conn_output,
        schema_name=schema_name
    )

    for i in range(len(dimensions_names)):
        frames[i].to_sql(
            con=conn_output,
            schema=schema_name.lower(),
            name=dimensions_names[i],
            if_exists="replace"
        )

    return True
