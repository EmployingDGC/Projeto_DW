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

    list_dimensions = [
        dms.treat_d_localildade(frame_dados_ibge, frame_resultado_aluno),
        dms.treat_d_escola(frame_escolas),
        dms.treat_d_turma(frame_resultado_aluno)
    ]

    for i in range(len(list_dimensions)):
        print("-*" * 40)
        print(list_dimensions[i].dtypes)
        print()
        print(list_dimensions[i].iloc[:1000])

    dms.create_all_dimensions(
        conn_output=conn_database,
        schema_name=DFLT.SCHEMA_NAMES[1],
        frames=list_dimensions,
        dimensions_names=DFLT.DIMENSIONS_NAMES
    )

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos")
