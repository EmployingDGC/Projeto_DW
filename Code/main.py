from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import CONEXAO as conn
import STAGES as stgs


def convert_column_to_float64(column_data_frame: pd.DataFrame,
                              default: float) -> pd.DataFrame:
    return column_data_frame.apply(
        lambda num:
        num
        if str(num).isnumeric()
        else str(num).replace(",", ".")
        if str(num).replace(",", ".").isnumeric()
        else default
    ).astype("float64")


def convert_column_to_int64(column_data_frame: pd.DataFrame,
                            default: int) -> pd.DataFrame:
    return column_data_frame.apply(
        lambda num:
        num
        if str(num).isnumeric()
        else default
    ).astype("int64")


def create_table(database: MockConnection,
                 schema_name: str,
                 table_name: str,
                 table_vars: dict[str, str]) -> None:
    str_vars = ""

    for k, v in table_vars.items():
        str_vars += f"{k} {v}, "

    str_vars = str_vars[:-2]

    database.execute(f"create table if not exists \"{schema_name}\".\"{table_name}\" ({str_vars})")


def create_schema(database: MockConnection,
                  schema_name: str) -> None:
    database.execute(f" create schema if not exists {schema_name}")


def drop_table(database: MockConnection,
               schema_name: str,
               table_name: str) -> None:
    database.execute(f" drop table if exists \"{schema_name}\".\"{table_name}\"")


if __name__ == '__main__':
    db = conn.create_connection_postgre(
        server="localhost",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # create_schema(db, "stage")

    # drop_table(db, "stage", "STG_ESCOLAS")
    # drop_table(db, "stage", "STG_TS_RESULTADO_ALUNO")
    # drop_table(db, "stage", "STG_DADOS_IBGE")

    # dic = {
    #     "id": "serial primary key",
    #     "nome": "varchar(100)",
    #     "uf": "varchar(2)"
    # }

    # create_table(
    #     db,
    #     "stage",
    #     "teste",
    #     dic
    # )

    path_ts_resultado_aluno = "../Datasets/TS_RESULTADO_ALUNO.csv"
    path_escolas = "../Datasets/ESCOLAS.CSV"
    path_dados_ibge = "../Datasets/DADOS_IBGE.csv"

    # frame_ts_resultado_aluno = pd.read_csv(
    #     path_ts_resultado_aluno,
    #     sep=";",
    #     encoding="utf8",
    #     low_memory=False
    # )

    # frame_escolas = pd.read_csv(
    #     path_escolas,
    #     sep="|",
    #     encoding="utf8",
    #     low_memory=False
    # )

    # frame_dados_ibge = pd.read_csv(
    #     path_dados_ibge,
    #     sep=";",
    #     encoding="utf8",
    #     low_memory=False
    # )

    stgs.create_stg(
        path=path_ts_resultado_aluno,
        name="STG_TS_RESULTADO_ALUNO",
        delimiter=";",
        conn_output=db
    )

    stgs.create_stg(
        path=path_escolas,
        name="STG_ESCOLAS",
        delimiter="|",
        conn_output=db
    )

    stgs.create_stg(
        path=path_dados_ibge,
        name="STG_DADOS_IBGE",
        delimiter=";",
        conn_output=db
    )

    # frame_ts_resultado_aluno["ID_TURNO"] = convert_column_to_int64(
    #     frame_ts_resultado_aluno["ID_TURNO"], 0
    # )

    # frame_ts_resultado_aluno["PESO"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["PESO"], 0
    # )

    # frame_ts_resultado_aluno["PROFICIENCIA_LP"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["PROFICIENCIA_LP"], 0
    # )

    # frame_ts_resultado_aluno["DESVIO_PADRAO_LP"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["DESVIO_PADRAO_LP"], 0
    # )

    # frame_ts_resultado_aluno["PROFICIENCIA_LP_SAEB"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["PROFICIENCIA_LP_SAEB"], 0
    # )

    # frame_ts_resultado_aluno["DESVIO_PADRAO_LP_SAEB"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["DESVIO_PADRAO_LP_SAEB"], 0
    # )

    # frame_ts_resultado_aluno["PROFICIENCIA_MT"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["PROFICIENCIA_MT"], 0
    # )

    # frame_ts_resultado_aluno["DESVIO_PADRAO_MT"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["DESVIO_PADRAO_MT"], 0
    # )

    # frame_ts_resultado_aluno["PROFICIENCIA_MT_SAEB"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["PROFICIENCIA_MT_SAEB"], 0
    # )

    # frame_ts_resultado_aluno["DESVIO_PADRAO_MT_SAEB"] = convert_column_to_float64(
    #     frame_ts_resultado_aluno["DESVIO_PADRAO_MT_SAEB"], 0
    # )
