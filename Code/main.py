from time import time

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms
import DEFAULTS_VALUES as DFLT


if __name__ == '__main__':
    time_exec = time()

    pd.set_option("display.max_columns", None)

    conn_database = conn.create_connection_postgre(
        server="localhost",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # stgs.create_all_stages(conn_database)

    frame_escolas = stgs.get_stg_escolas(conn_database)
    frame_dados_ibge = stgs.get_stg_dados_ibge(conn_database)
    frame_resultado_aluno = stgs.get_stg_ts_resultado_aluno(conn_database)

    frame_d_localidade = dms.treat_d_localildade(frame_dados_ibge, frame_resultado_aluno)
    frame_d_escola = dms.treat_d_escola(frame_escolas)
    frame_d_turma = dms.treat_d_turma(frame_resultado_aluno)
    # frame_f_prova = pd.DataFrame()

    print(frame_d_localidade.dtypes)
    print()
    print(frame_d_localidade)
    print()
    print(frame_d_escola.dtypes)
    print()
    print(frame_d_escola)
    print()
    print(frame_d_turma.dtypes)
    print()
    print(frame_d_turma)
    print()

    # frame_f_prova["FL_SITUACAO_CENSO"] = frame_resultado_aluno["IN_SITUACAO_CENSO"].apply(
    #     lambda num:
    #     num == 1
    # )
    #
    # frame_f_prova["FL_PREENCHIMENTO"] = frame_resultado_aluno["IN_PREENCHIMENTO"].apply(
    #     lambda num:
    #     num == 1
    # )
    #
    # frame_f_prova["FL_PROFICIENCIA"] = frame_resultado_aluno["IN_PROFICIENCIA"].apply(
    #     lambda num:
    #     num == 1
    # )

    # dms.create_all_dimensions(
    #     conn_output=conn_database,
    #     schema_name="dw",
    #     frames=[
    #         frame_d_localidade,
    #         frame_d_escola
    #     ],
    #     dimensions_names=[
    #         "D_LOCALIDADE",
    #         "D_ESCOLA"
    #     ]
    # )

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos")
