import pandas as pd


def load_stg_to_dw(conn_input,
                   conn_output,
                   name):
    pd.read_sql_query(
        f"SELECT * FROM BANCO.SCHEMA.{name}",
        conn_input.to_sql(
            name=name,
            con=conn_output,
            schema="stage",  # STAGE
            if_exists="replace"
        )
    )


def create_stg(path,
               name,
               delimiter,
               conn_output):
    pd.read_csv(
        path,
        sep=delimiter,
        encoding="utf8",
        low_memory=False
    ).to_sql(
        name=name,
        con=conn_output,
        schema="stage",  # STAGE
        if_exists="replace"
    )
