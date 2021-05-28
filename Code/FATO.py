from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import utilities as utl
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


def get_csv_ts_resultado_aluno() -> pd.DataFrame:
    frame_ts_resultado_aluno = pd.read_csv(
        "../Datasets/TS_RESULTADO_ALUNO.csv",
        delimiter=";",
        usecols=[
            "ID_MUNICIPIO",
            "ID_TURMA",
            "ID_ESCOLA",
            "ID_PROVA_BRASIL",
            "PROFICIENCIA_MT_SAEB",
            "PROFICIENCIA_LP_SAEB",
            "IN_SITUACAO_CENSO",
            "IN_PREENCHIMENTO",
            "IN_PROFICIENCIA"
        ]
    )

    return frame_ts_resultado_aluno.drop(
        frame_ts_resultado_aluno[
            (frame_ts_resultado_aluno["IN_SITUACAO_CENSO"] != 1) |
            (frame_ts_resultado_aluno["IN_PREENCHIMENTO"] != 1) |
            (frame_ts_resultado_aluno["IN_PROFICIENCIA"] != 1)
            ].index
    ).reset_index(drop=True)


def treat_f_prova_br(frame_ts_resultado_aluno: pd.DataFrame,
                     dimensions: list[pd.DataFrame]) -> pd.DataFrame:
    frame_f_prova_br = pd.DataFrame()

    frame_f_prova_br["CD_ANO"] = utl.convert_column_to_int64(
        column_data_frame=frame_ts_resultado_aluno["ID_PROVA_BRASIL"],
        default=DFLT.CD[0]
    )

    frame_f_prova_br["VL_PROFICIENCIA_MT_SAEB"] = utl.convert_column_to_float64(
        column_data_frame=frame_ts_resultado_aluno["PROFICIENCIA_MT_SAEB"],
        default=DFLT.CD[0]
    )

    frame_f_prova_br["VL_PROFICIENCIA_LP_SAEB"] = utl.convert_column_to_float64(
        column_data_frame=frame_ts_resultado_aluno["PROFICIENCIA_LP_SAEB"],
        default=DFLT.CD[0]
    )

    frame_f_prova_br["FL_SITUACAO_CENSO"] = utl.convert_column_to_boolean(
        column_data_frame=frame_ts_resultado_aluno["IN_SITUACAO_CENSO"]
    )

    frame_f_prova_br["FL_PREENCHIMENTO"] = utl.convert_column_to_boolean(
        column_data_frame=frame_ts_resultado_aluno["IN_PREENCHIMENTO"]
    )

    frame_f_prova_br["FL_PROFICIENCIA"] = utl.convert_column_to_boolean(
        column_data_frame=frame_ts_resultado_aluno["IN_PROFICIENCIA"]
    )

    frame_f_prova_br["SK_LOCALIDADE"] = frame_ts_resultado_aluno.merge(
        dimensions[0],
        how="inner",
        left_on="ID_MUNICIPIO",
        right_on="CD_MUNICIPIO"
    )["SK_LOCALIDADE"]

    frame_f_prova_br["SK_ESCOLA"] = frame_ts_resultado_aluno.merge(
        dimensions[1],
        how="inner",
        left_on="ID_ESCOLA",
        right_on="CD_ESCOLA"
    )["SK_ESCOLA"]

    frame_f_prova_br["SK_TURMA"] = frame_ts_resultado_aluno.merge(
        dimensions[2],
        how="inner",
        left_on="ID_TURMA",
        right_on="CD_TURMA"
    )["SK_TURMA"]

    return frame_f_prova_br


def get_fato(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=DFLT.SCHEMA_NAMES[0],
        table_name=DFLT.FATO_NAME
    )


def run(conn_output: MockConnection) -> None:
    frame_ts_resultado_aluno = get_csv_ts_resultado_aluno()

    dimensions = get_all_dimensions(conn_output)

    frame_f_prova_br = treat_f_prova_br(frame_ts_resultado_aluno, dimensions)

    frame_f_prova_br.to_sql(
        con=conn_output,
        schema=DFLT.SCHEMA_NAMES[1],
        name=DFLT.FATO_NAME,
        if_exists="replace",
        index=False
    )
