import pandas as pd

import utilities as utl
import DEFAULTS_VALUES as DFLT


def treat_d_localildade(dados_ibge: pd.DataFrame,
                        resultado_aluno: pd.DataFrame) -> pd.DataFrame:
    frame_d_localidade = pd.DataFrame()

    dados_ibge.rename(
        columns={
            "Cód.": "CD",
            "Brasil, Grande Região, Unidade da Federação e Município": "DS"
        },
        inplace=True
    )

    frame_mu_dados_ibge = dados_ibge.query(
        f"Nível == 'MU'"
    )[["CD", "DS"]]

    frame_uf_dados_ibge = dados_ibge.query(
        f"Nível == 'UF'"
    )[["CD", "DS"]]

    frame_d_localidade["DS_MUNICIPIO"] = resultado_aluno.merge(
        frame_mu_dados_ibge,
        how="inner",
        left_on="ID_MUNICIPIO",
        right_on="CD"
    )["DS"].apply(
        lambda ds: str(ds)[:-5]
    )

    frame_d_localidade["DS_UF"] = resultado_aluno.merge(
        frame_uf_dados_ibge,
        how="inner",
        left_on="ID_UF",
        right_on="CD"
    )["DS"]

    frame_d_localidade["CD_MUNICIPIO"] = resultado_aluno.merge(
        frame_mu_dados_ibge,
        how="inner",
        left_on="ID_MUNICIPIO",
        right_on="CD"
    )["CD"]

    frame_d_localidade["CD_UF"] = resultado_aluno.merge(
        frame_uf_dados_ibge,
        how="inner",
        left_on="ID_UF",
        right_on="CD"
    )["CD"]

    frame_d_localidade.drop_duplicates(
        subset="CD_MUNICIPIO",
        inplace=True
    )

    frame_d_localidade["SK_LOCALIDADE"] = utl.create_index_dataframe(
        data_frame=frame_d_localidade,
        first_index=1
    )

    return frame_d_localidade[[k for k in DFLT.KEYS_DIMENSIONS["LOCALIDADE"]]]

