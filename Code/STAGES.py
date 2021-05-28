from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import DEFAULTS_VALUES as DFLT


def get_stg_escolas(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[1]
    )


def get_stg_dados_ibge(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[0]
    )


def get_stg_ts_resultado_aluno(conn_input: MockConnection) -> pd.DataFrame:
    frame = utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[2]
    )

    return frame.drop(
        frame[
            (frame["IN_SITUACAO_CENSO"] != 1) |
            (frame["IN_PREENCHIMENTO"] != 1) |
            (frame["IN_PROFICIENCIA"] != 1)
        ].index
    ).reset_index(drop=True)


def get_all_stages(conn_input: MockConnection) -> list[pd.DataFrame]:
    return [
        get_stg_escolas(conn_input),
        get_stg_dados_ibge(conn_input),
        get_stg_ts_resultado_aluno(conn_input)
    ]


def run(conn_output: MockConnection) -> None:
    path_ts_resultado_aluno = "../Datasets/TS_RESULTADO_ALUNO.csv"
    path_escolas = "../Datasets/ESCOLAS.CSV"
    path_dados_ibge = "../Datasets/DADOS_IBGE.csv"

    utl.create_schema(conn_output, DFLT.SCHEMA_NAMES[0])

    utl.create_optimized_table_from_csv(
        path=path_ts_resultado_aluno,
        delimiter=";",
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[2],
        conn_output=conn_output,
        replace_table=True,
        columns=[
            "ID_MUNICIPIO",
            "ID_UF",
            "ID_TURMA",
            "ID_TURNO",
            "ID_SERIE",
            "IN_SITUACAO_CENSO",
            "IN_PREENCHIMENTO",
            "IN_PROFICIENCIA"
        ]
    )

    utl.create_optimized_table_from_csv(
        path=path_escolas,
        delimiter="|",
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[1],
        conn_output=conn_output,
        replace_table=True,
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

    utl.create_optimized_table_from_csv(
        path=path_dados_ibge,
        delimiter=";",
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.STAGES_NAMES[0],
        conn_output=conn_output,
        replace_table=True
    )
