import pandas as pd

import utilities as utl
import DEFAULTS_VALUES as DFLT


def treat_d_escola(escolas: pd.DataFrame) -> pd.DataFrame:
    frame_d_escola = pd.DataFrame()

    frame_d_escola["DS_LOCALIZACAO"] = escolas["ID_LOCALIZACAO"].apply(
        lambda num:
        "Urbana" if num == 1 else
        "Rural" if num == 2 else
        DFLT.DS[0]
    )

    frame_d_escola["CD_LOCALIZACAO"] = escolas["ID_LOCALIZACAO"]

    frame_d_escola["NO_ESCOLA"] = escolas["NO_ENTIDADE"]

    frame_d_escola["CD_ESCOLA"] = escolas["PK_COD_ENTIDADE"]

    frame_d_escola["DS_DEPENDENCIA_ADM"] = escolas["ID_DEPENDENCIA_ADM"].apply(
        lambda num:
        "Federal" if num == 1 else
        "Estadual" if num == 2 else
        "Municipal" if num == 3 else
        "Privada" if num == 4 else
        DFLT.DS[0]
    )

    frame_d_escola["CD_DEPENDENCIA_ADM"] = escolas["ID_DEPENDENCIA_ADM"]

    frame_d_escola["SK_ESCOLA"] = utl.create_index_dataframe(
        data_frame=frame_d_escola,
        first_index=1
    )

    return frame_d_escola[[k for k in DFLT.KEYS_DIMENSIONS["ESCOLAS"]]]
