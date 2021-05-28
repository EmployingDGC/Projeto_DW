from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl


def get_stg_escolas(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_ESCOLAS",
        columns=[
            "ANO_CENSO",
            "PK_COD_ENTIDADE",
            "NO_ENTIDADE",
            "SIGLA",
            "FK_COD_MUNICIPIO",
            "FK_COD_DISTRITO",
            "ID_DEPENDENCIA_ADM",
            "ID_LOCALIZACAO"
        ]
    )


def get_stg_dados_ibge(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_DADOS_IBGE"
    )


def get_stg_ts_resultado_aluno(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_TS_RESULTADO_ALUNO",
        columns=[
            "ID_MUNICIPIO",
            "ID_UF",
            "ID_LOCALIZACAO",
            "ID_ESCOLA",
            "ID_DEPENDENCIA_ADM",
            "ID_TURMA",
            "ID_TURNO",
            "ID_SERIE",
            "IN_SITUACAO_CENSO",
            "IN_PREENCHIMENTO",
            "IN_PROFICIENCIA",
            "PROFICIENCIA_LP_SAEB",
            "PROFICIENCIA_MT_SAEB"
        ]
    )


def get_all_stages(conn_input: MockConnection) -> list[pd.DataFrame]:
    return [
        get_stg_escolas(conn_input),
        get_stg_dados_ibge(conn_input),
        get_stg_ts_resultado_aluno(conn_input)
    ]


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
        conn_output=conn_output,
        replace_table=True
    )

    utl.create_optimized_table_from_csv(
        path=path_escolas,
        delimiter="|",
        schema_name="stage",
        table_name="STG_ESCOLAS",
        conn_output=conn_output,
        replace_table=True
    )

    utl.create_optimized_table_from_csv(
        path=path_dados_ibge,
        delimiter=";",
        schema_name="stage",
        table_name="STG_DADOS_IBGE",
        conn_output=conn_output,
        replace_table=True
    )
