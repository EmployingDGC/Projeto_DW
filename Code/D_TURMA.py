import pandas as pd

import utilities as utl
import DEFAULTS_VALUES as DFLT


def treat_d_turma(resultado_aluno: pd.DataFrame) -> pd.DataFrame:
    frame_d_turma = pd.DataFrame()

    resultado_aluno["ID_TURMA"] = utl.convert_column_to_int64(
        resultado_aluno["ID_TURMA"],
        DFLT.CD[0]
    )

    resultado_aluno["ID_TURNO"] = utl.convert_column_to_int64(
        resultado_aluno["ID_TURNO"],
        DFLT.CD[0]
    )

    resultado_aluno["ID_SERIE"] = utl.convert_column_to_int64(
        resultado_aluno["ID_SERIE"],
        DFLT.CD[0]
    )

    frame_d_turma["CD_TURMA"] = resultado_aluno["ID_TURMA"]

    frame_d_turma["CD_TURNO"] = resultado_aluno["ID_TURNO"]

    frame_d_turma["CD_SERIE"] = resultado_aluno["ID_SERIE"]

    frame_d_turma["DS_TURNO"] = resultado_aluno["ID_TURNO"].apply(
        lambda num:
        "Matutino" if num == 1 else
        "Vespertino" if num == 2 else
        "Noturno" if num == 3 else
        "Intermediário" if num == 4 else
        DFLT.DS[0]
    )

    frame_d_turma["DS_SERIE"] = resultado_aluno["ID_SERIE"].apply(
        lambda num:
        "4ª série / 5º ano EF" if num == 5 else
        "8ª série / 9º ano EF" if num == 9 else
        DFLT.DS[0]
    )

    frame_d_turma.drop_duplicates(
        subset="CD_TURMA",
        inplace=True
    )

    frame_d_turma["SK_TURMA"] = utl.create_index_dataframe(
        data_frame=frame_d_turma,
        first_index=1
    )

    return frame_d_turma[[k for k in DFLT.KEYS_DIMENSIONS["TURMAS"]]]

