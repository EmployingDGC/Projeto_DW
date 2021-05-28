from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import DEFAULTS_VALUES as DFLT


def partition_csv(path: str,
                  delimiter: str,
                  qty_parts: int = 100,
                  columns: list[str] = None,
                  error_bad_lines: bool = True) -> list[pd.DataFrame]:
    list_dataframe = []

    if qty_parts <= 0:
        qty_parts = 1

    frame_csv = pd.read_csv(
        path,
        sep=delimiter,
        low_memory=False,
        usecols=columns,
        error_bad_lines=error_bad_lines
    )

    qty_rows = frame_csv.shape[0]

    qty_rows_per_dataframe = qty_rows // qty_parts
    qty_over_rows = qty_rows % qty_parts

    start = 0
    end = qty_rows_per_dataframe

    if end < 1:
        qty_parts = 1
        qty_over_rows = 0
        end = qty_rows

    for i in range(qty_parts):
        list_dataframe.append(
            frame_csv.iloc[start:end]
        )

        start += qty_rows_per_dataframe
        end += qty_rows_per_dataframe

    if qty_over_rows > 0:
        list_dataframe.append(
            frame_csv.iloc[start:]
        )

    return list_dataframe


def partition_data_frame(data_frame: pd.DataFrame,
                         qty_parts: int = 100) -> list[pd.DataFrame]:
    list_data_frame = []

    if qty_parts <= 0:
        qty_parts = 1

    qty_rows = data_frame.shape[0]

    qty_rows_per_dataframe = qty_rows // qty_parts
    qty_over_rows = qty_rows % qty_parts

    start = 0
    end = qty_rows_per_dataframe

    if end < 1:
        qty_parts = 1
        qty_over_rows = 0
        end = qty_rows

    for i in range(qty_parts):
        list_data_frame.append(
            data_frame.iloc[start:end]
        )

        start += qty_rows_per_dataframe
        end += qty_rows_per_dataframe

    if qty_over_rows > 0:
        list_data_frame.append(
            data_frame.iloc[start:]
        )

    return list_data_frame


def union_data_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    union_frame = pd.DataFrame()

    for i in range(len(frames)):
        union_frame = pd.concat([union_frame, frames[i]])

    return union_frame.reset_index(drop=True)


def create_optimized_table_from_csv(path: str,
                                    delimiter: str,
                                    schema_name: str,
                                    table_name: str,
                                    conn_output: MockConnection,
                                    qty_parts: int = 100,
                                    columns: list[str] = None,
                                    error_bad_lines: bool = True,
                                    replace_table: bool = False,
                                    index: bool = False) -> bool:
    if qty_parts <= 0:
        qty_parts = 1

    partitions = partition_csv(
        path=path,
        delimiter=delimiter,
        qty_parts=qty_parts,
        error_bad_lines=error_bad_lines,
        columns=columns
    )

    if len(partitions) == 0:
        return False

    if replace_table:
        drop_tables(
            conn_output=conn_output,
            schema_name=schema_name.lower(),
            dimensions_names=[table_name]
        )

    for frame in partitions:
        frame.to_sql(
            name=table_name,
            con=conn_output,
            schema=schema_name.lower(),
            if_exists="append",
            index=index
        )

    return True


def create_index_dataframe(data_frame: pd.DataFrame,
                           first_index: int = 0) -> list[int]:
    list_index = [i + first_index for i in range(data_frame.shape[0])]

    return list_index


def convert_table_to_dataframe(conn_input: MockConnection,
                               schema_name: str,
                               table_name: str,
                               columns: list[str] = "*",
                               qty_parts: int = 100) -> pd.DataFrame:
    str_columns = ""

    if columns[0] == "*" or len(columns) == 0:
        str_columns = "*"

    else:
        for col in columns:
            str_columns += f"\"{col}\", "

        str_columns = str_columns[:-2]

    select = conn_input.execute(
        f"select {str_columns} "
        f"from \"{schema_name.lower()}\".\"{table_name}\" "
    )

    if qty_parts <= 0:
        qty_parts = 100

    data_frame = pd.DataFrame()

    select_list = [x for x in select]

    qty_rows = len(select_list)

    qty_rows_per_dataframe = qty_rows // qty_parts
    qty_over_rows = qty_rows % qty_parts

    start = 0
    end = qty_rows_per_dataframe

    if end < 1:
        qty_parts = 1
        qty_over_rows = 0
        end = qty_rows

    for i in range(qty_parts):
        data_frame = pd.concat([
            data_frame,
            pd.DataFrame(
                select_list[start:end],
                columns=select.keys()
            )
        ])

        start += qty_rows_per_dataframe
        end += qty_rows_per_dataframe

    if qty_over_rows > 0:
        data_frame = pd.concat([
            data_frame,
            pd.DataFrame(
                select_list[start:end],
                columns=select.keys(),
            )
        ])

    return data_frame.reset_index(drop=True)


def convert_column_to_boolean(column_data_frame: pd.Series) -> pd.Series:
    return column_data_frame.apply(
        lambda num:
        str(num).upper() in DFLT.TRUE_VALUES
    )


def convert_column_to_float64(column_data_frame: pd.Series,
                              default: float) -> pd.Series:
    return column_data_frame.apply(
        lambda num:
        float(num) if str(num).isnumeric() else
        float(str(num).replace(",", ".")) if str(num).replace(",", ".").replace(".", "").isnumeric() else
        float(default)
    )


def convert_column_to_int64(column_data_frame: pd.Series,
                            default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda num:
        int(num) if str(num).isnumeric() else
        int(str(num).split(",")[0].split(".")[0]) if str(num).replace(",", ".").replace(".", "").isnumeric() else
        int(default)
    )


def create_table(conn_output: MockConnection,
                 schema_name: str,
                 table_name: str,
                 table_vars: dict[str, str]) -> None:
    str_vars = ""

    for k, v in table_vars.items():
        str_vars += f"{k} {v}, "

    str_vars = str_vars[:-2]

    conn_output.execute(f"create table if not exists \"{schema_name.lower()}\".\"{table_name}\" ({str_vars})")


def create_schema(database: MockConnection,
                  schema_name: str) -> None:
    database.execute(f" create schema if not exists {schema_name.lower()}")


def drop_tables(conn_output: MockConnection,
                schema_name: str,
                dimensions_names: list[str]) -> None:
    for i in range(len(dimensions_names)):
        conn_output.execute(
            f" drop table if exists \"{schema_name.lower()}\".\"{dimensions_names[i]}\""
        )


def drop_schemas(conn_output: MockConnection,
                 schemas_names: list[str]) -> None:
    for schema in schemas_names:
        conn_output.execute(f" drop schema if exists {schema.lower()}")
