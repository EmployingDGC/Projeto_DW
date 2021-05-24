from sqlalchemy.engine.mock import MockConnection

import pandas as pd


def partition_csv(path: str,
                  delimiter: str,
                  qty_parts: int,
                  error_bad_lines: bool = True) -> list[pd.DataFrame]:
    list_dataframe = []

    if qty_parts <= 0:
        return list_dataframe

    frame_csv = pd.read_csv(
        path,
        sep=delimiter,
        low_memory=False,
        error_bad_lines=error_bad_lines
    )

    qty_rows_per_dataframe = frame_csv.shape[0] // qty_parts
    qty_over_rows = frame_csv.shape[0] % qty_parts

    start = 0
    end = qty_rows_per_dataframe

    for i in range(qty_parts):
        list_dataframe.append(
            frame_csv.iloc[start:end]
        )

        start += qty_rows_per_dataframe
        end += qty_rows_per_dataframe

    if qty_over_rows > 0:
        list_dataframe.append(
            frame_csv.iloc[start:start + qty_over_rows]
        )

    return list_dataframe


def load_stg_to_dw(conn_input: MockConnection,
                   conn_output: MockConnection,
                   schema_name: str,
                   table_name: str) -> None:
    pd.read_sql_query(
        f"SELECT * FROM \"{conn_input.name}\".\"{schema_name}\".\"{table_name}\"",
        conn_input.to_sql(
            name=table_name,
            con=conn_output,
            schema="stage",  # STAGE
            if_exists="replace"
        )
    )


def create_stg(path: str,
               name: str,
               delimiter: str,
               conn_output: MockConnection,
               error_bad_lines: bool = True,
               if_exists: str = "replace") -> None:
    pd.read_csv(
        path,
        sep=delimiter,
        low_memory=False,
        error_bad_lines=error_bad_lines
    ).to_sql(
        name=name,
        con=conn_output,
        schema="stage",  # STAGE
        if_exists=if_exists
    )


def create_optimized_table_from_csv(path: str,
                                    delimiter: str,
                                    schema_name: str,
                                    table_name: str,
                                    qty_parts: int,
                                    conn_output: MockConnection,
                                    error_bad_lines: bool = True) -> bool:
    if qty_parts <= 0:
        return False

    partitions = partition_csv(
        path=path,
        delimiter=delimiter,
        qty_parts=qty_parts,
        error_bad_lines=error_bad_lines
    )

    if len(partitions) == 0:
        return False

    for frame in partitions:
        frame.to_sql(
            name=table_name,
            con=conn_output,
            schema=schema_name.lower(),
            if_exists="append"
        )

    return True
