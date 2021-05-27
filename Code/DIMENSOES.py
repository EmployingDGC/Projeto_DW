from sqlalchemy.engine.mock import MockConnection

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

    return frame_d_localidade


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

    frame_d_escola["DS_DEPENDENCIA_ADM"] = escolas["ID_DEPENDENCIA_ADM"].apply(
        lambda num:
        "Federal" if num == 1 else
        "Estadual" if num == 2 else
        "Municipal" if num == 3 else
        "Privada" if num == 4 else
        DFLT.DS[0]
    )

    frame_d_escola["CD_DEPENDENCIA_ADM"] = escolas["ID_DEPENDENCIA_ADM"]

    return frame_d_escola


def treat_d_turma(resultado_aluno: pd.DataFrame) -> pd.DataFrame:
    frame_d_localidade = pd.DataFrame()

    resultado_aluno["ID_TURMA"] = utl.convert_column_to_int64(
        resultado_aluno["ID_TURNO"],
        DFLT.CD[0]
    )

    resultado_aluno["ID_TURNO"] = utl.convert_column_to_int64(
        resultado_aluno["ID_TURNO"],
        DFLT.CD[0]
    )

    resultado_aluno["ID_SERIE"] = utl.convert_column_to_int64(
        resultado_aluno["ID_TURNO"],
        DFLT.CD[0]
    )

    frame_d_localidade["CD_TURMA"] = resultado_aluno["ID_TURMA"]

    frame_d_localidade["CD_TURNO"] = resultado_aluno["ID_TURNO"]

    frame_d_localidade["CD_SERIE"] = resultado_aluno["ID_SERIE"]

    frame_d_localidade["DS_TURNO"] = resultado_aluno["ID_TURNO"].apply(
        lambda num:
        "Matutino" if num == 1 else
        "Vespertino" if num == 2 else
        "Noturno" if num == 3 else
        "Intermediário" if num == 4 else
        DFLT.DS[0]
    )

    frame_d_localidade["DS_SERIE"] = resultado_aluno["ID_SERIE"].apply(
        lambda num:
        "4ª série / 5º ano EF" if num == 5 else
        "8ª série / 9º ano EF" if num == 9 else
        DFLT.DS[0]
    )

    return frame_d_localidade


def treat_all_dimensions(dados_ibge: pd.DataFrame,
                         escolas: pd.DataFrame,
                         resultado_aluno: pd.DataFrame) -> list[pd.DataFrame]:
    return [
        treat_d_localildade(dados_ibge, resultado_aluno),
        treat_d_escola(escolas),
        treat_d_turma(resultado_aluno)
    ]


def create_all_dimensions(conn_output: MockConnection,
                          schema_name: str,
                          frames: list[pd.DataFrame],
                          dimensions_names: list[str]) -> bool:
    if len(frames) != len(dimensions_names):
        return False

    utl.create_schema(
        database=conn_output,
        schema_name=schema_name
    )

    for i in range(len(dimensions_names)):
        frames[i].to_sql(
            con=conn_output,
            schema=schema_name.lower(),
            name=dimensions_names[i],
            if_exists="replace"
        )

    return True
