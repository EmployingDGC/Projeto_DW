from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl


def get_all_stages(conn_input: MockConnection) -> list[pd.DataFrame]:
    return [
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name="stage",
            table_name="STG_TS_RESULTADO_ALUNO",
        ),
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name="stage",
            table_name="STG_ESCOLAS",
        ),
        utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name="stage",
            table_name="STG_DADOS_IBGE",
        )
    ]


def drop_all_stages(conn_output: MockConnection) -> None:
    utl.drop_table(conn_output, "stage", "STG_TS_RESULTADO_ALUNO")
    utl.drop_table(conn_output, "stage", "STG_ESCOLAS")
    utl.drop_table(conn_output, "stage", "STG_DADOS_IBGE")


def create_all_stages(conn_output: MockConnection) -> None:
    path_ts_resultado_aluno = "../Datasets/TS_RESULTADO_ALUNO.csv"
    path_escolas = "../Datasets/ESCOLAS.CSV"
    path_dados_ibge = "../Datasets/DADOS_IBGE.csv"

    utl.create_schema(conn_output, "stage")

    utl.create_optimized_table_from_csv(
        path=path_ts_resultado_aluno,
        delimiter=";",
        schema_name="stage",
        table_name="STG_TS_RESULTADO_ALUNO",
        qty_parts=80,
        conn_output=conn_output,
        replace_table=True
    )

    utl.create_optimized_table_from_csv(
        path=path_escolas,
        delimiter="|",
        schema_name="stage",
        table_name="STG_ESCOLAS",
        qty_parts=80,
        conn_output=conn_output,
        replace_table=True
    )

    utl.create_optimized_table_from_csv(
        path=path_dados_ibge,
        delimiter=";",
        schema_name="stage",
        table_name="STG_DADOS_IBGE",
        qty_parts=80,
        conn_output=conn_output,
        replace_table=True
    )
