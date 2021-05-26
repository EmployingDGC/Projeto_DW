from time import time

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms


if __name__ == '__main__':
    time_exec = time()

    pd.set_option("display.max_columns", None)

    default_sk = [-1, -2, -3]
    default_cd = [-1, -2, -3]
    default_ds = ["Não Informado", "Não Aplicável", "Desconhecido"]

    conn_database = conn.create_connection_postgre(
        server="localhost",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # stgs.create_all_stages(conn_database)

    frame_list_stages = stgs.get_all_stages(conn_database)

    frame_escolas = frame_list_stages[0]
    frame_dados_ibge = frame_list_stages[1]
    frame_resultado_aluno = frame_list_stages[2]

    frame_dados_ibge.rename(
        columns={
            "Cód.": "CD",
            "Brasil, Grande Região, Unidade da Federação e Município": "DS"
        },
        inplace=True
    )

    frame_mu_dados_ibge = frame_dados_ibge.query(
        f"Nível == 'MU'"
    )[["CD", "DS"]]

    frame_uf_dados_ibge = frame_dados_ibge.query(
        f"Nível == 'UF'"
    )[["CD", "DS"]]

    frame_d_localidade = pd.DataFrame()
    frame_d_escola = pd.DataFrame()
    frame_f_prova = pd.DataFrame()

    frame_d_localidade["DS_MUNICIPIO"] = frame_resultado_aluno.merge(
        frame_mu_dados_ibge,
        how="inner",
        left_on="ID_MUNICIPIO",
        right_on="CD"
    )["DS"].apply(
        lambda ds: str(ds)[:-5]
    )

    frame_d_localidade["DS_UF"] = frame_resultado_aluno.merge(
        frame_uf_dados_ibge,
        how="inner",
        left_on="ID_UF",
        right_on="CD"
    )["DS"]

    frame_d_localidade["CD_MUNICIPIO"] = frame_resultado_aluno.merge(
        frame_mu_dados_ibge,
        how="inner",
        left_on="ID_MUNICIPIO",
        right_on="CD"
    )["CD"]

    frame_d_localidade["CD_UF"] = frame_resultado_aluno.merge(
        frame_uf_dados_ibge,
        how="inner",
        left_on="ID_UF",
        right_on="CD"
    )["CD"]

    frame_d_escola["DS_LOCALIZACAO"] = frame_escolas["ID_LOCALIZACAO"].apply(
        lambda num:
        "Urbana" if num == 1 else
        "Rural" if num == 2 else
        default_ds[0]
    )

    frame_d_escola["CD_LOCALIZACAO"] = frame_escolas["ID_LOCALIZACAO"]

    frame_d_escola["NO_ESCOLA"] = frame_escolas["NO_ENTIDADE"]

    frame_d_escola["DS_DEPENDENCIA_ADM"] = frame_escolas["ID_DEPENDENCIA_ADM"].apply(
        lambda num:
        "Federal" if num == 1 else
        "Estadual" if num == 2 else
        "Municipal" if num == 3 else
        "Privada" if num == 4 else
        default_ds[0]
    )

    frame_d_escola["CD_DEPENDENCIA_ADM"] = frame_escolas["ID_DEPENDENCIA_ADM"]

    frame_f_prova["FL_SITUACAO_CENSO"] = frame_resultado_aluno["IN_SITUACAO_CENSO"].apply(
        lambda num:
        num == 1
    )

    frame_f_prova["FL_PREENCHIMENTO"] = frame_resultado_aluno["IN_PREENCHIMENTO"].apply(
        lambda num:
        num == 1
    )

    frame_f_prova["FL_PROFICIENCIA"] = frame_resultado_aluno["IN_PROFICIENCIA"].apply(
        lambda num:
        num == 1
    )

    print(frame_d_localidade)
    print()
    print(frame_d_escola)
    print()
    print(frame_f_prova)

    # dms.create_all_dimensions(
    #     conn_output=conn_database,
    #     schema_name="dw",
    #     frames=[
    #
    #     ],
    #     dimensions_names=[
    #         "D_LOCALIDADE"
    #     ]
    # )

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos")
