from time import time

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms


if __name__ == '__main__':
    time_exec = time()

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

    # frame_resultado_aluno["DS_MUNICIPIO"] = default_ds[0]
    # frame_resultado_aluno["DS_UF"] = default_ds[0]
    # frame_resultado_aluno["DS_MUNICIPIO"] = default_ds[0]
    # frame_resultado_aluno["DS_UF"] = default_ds[0]
    # frame_resultado_aluno["DS_LOCALIZACAO"] = default_ds[0]
    # frame_resultado_aluno["DS_ESCOLA"] = default_ds[0]
    # frame_resultado_aluno["DS_DEPENDENCIA_ADM"] = default_ds[0]
    # frame_resultado_aluno["DS_TURMA"] = default_ds[0]
    # frame_resultado_aluno["DS_TURNO"] = default_ds[0]
    # frame_resultado_aluno["DS_SERIE"] = default_ds[0]

    frame_resultado_aluno.rename(
        columns={
            "ID_MUNICIPIO": "CD_MUNICIPIO",
            "ID_UF": "CD_UF",
            "ID_LOCALIZACAO": "CD_LOCALIZACAO",
            "ID_ESCOLA": "CD_ESCOLA",
            "ID_DEPENDENCIA_ADM": "CD_DEPENDENCIA_ADM",
            "ID_TURMA": "CD_TURMA",
            "ID_TURNO": "CD_TURNO",
            "ID_SERIE": "CD_SERIE"
        },
        inplace=True
    )

    frame_localidade = frame_resultado_aluno.merge(
        frame_dados_ibge,
        how="left",
        left_on="CD_MUNICIPIO",
        right_on="Cód."
    )

    print(frame_localidade.columns)

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

    # for index, row in frame_list_stages[0].iterrows():
    #     if row[""]

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos")
