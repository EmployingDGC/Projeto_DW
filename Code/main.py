import time

from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms


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
    time_exec = time.time()

    path_ts_resultado_aluno = "../Datasets/TS_RESULTADO_ALUNO.csv"
    path_escolas = "../Datasets/ESCOLAS.CSV"
    path_dados_ibge = "../Datasets/DADOS_IBGE.csv"

    db = conn.create_connection_postgre(
        server="localhost",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # create_schema(db, "stage")
    # create_schema(db, "dw")

    # drop_table(db, "stage", "STG_TS_RESULTADO_ALUNO")
    # drop_table(db, "stage", "STG_ESCOLAS")
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

    # frame_ts_resultado_aluno = pd.read_csv(
    #     path_ts_resultado_aluno,
    #     sep=",",
    #     low_memory=False
    # )

    # frame_escolas = pd.read_csv(
    #     path_escolas,
    #     sep="|",
    #     low_memory=False
    # )

    # frame_dados_ibge = pd.read_csv(
    #     path_dados_ibge,
    #     sep=";",
    #     low_memory=False
    # )

    stgs.create_optimized_table_from_csv(
        path=path_ts_resultado_aluno,
        delimiter=";",
        schema_name="stage",
        table_name="STG_TS_RESULTADO_ALUNO",
        qty_parts=80,
        conn_output=db
    )

    # stgs.create_stg(
    #     path=path_escolas,
    #     name="STG_ESCOLAS",
    #     delimiter="|",
    #     conn_output=db
    # )

    # stgs.create_stg(
    #     path=path_dados_ibge,
    #     name="STG_DADOS_IBGE",
    #     delimiter=";",
    #     conn_output=db
    # )

    # frame_select_stg_resultado_aluno = dms.convert_table_to_dataframe(
    #     conn_input=db,
    #     schema_name="stage",
    #     table_name="STG_TS_RESULTADO_ALUNO",
    #     columns_name=[
    #         "ID",
    #         "ID_PROVA_BRASIL",
    #         "ID_UF",
    #         "ID_MUNICIPIO",
    #         "ID_ESCOLA",
    #         "ID_DEPENDENCIA_ADM",
    #         "ID_LOCALIZACAO",
    #         "ID_TURMA",
    #         "ID_TURNO",
    #         "ID_SERIE",
    #         "ID_ALUNO",
    #         "IN_SITUACAO_CENSO",
    #         "IN_PREENCHIMENTO",
    #         "IN_PROFICIENCIA",
    #         "PESO",
    #         "PROFICIENCIA_LP",
    #         "DESVIO_PADRAO_LP",
    #         "PROFICIENCIA_LP_SAEB",
    #         "DESVIO_PADRAO_LP_SAEB",
    #         "PROFICIENCIA_MT",
    #         "DESVIO_PADRAO_MT",
    #         "PROFICIENCIA_MT_SAEB",
    #         "DESVIO_PADRAO_MT_SAEB"
    #     ]
    # )

    # print(f"\n{frame_select_stg_resultado_aluno}")

    # frame_select_stg_escolas = dms.convert_table_to_dataframe(
    #     conn_input=db,
    #     schema_name="stage",
    #     table_name="STG_ESCOLAS",
    #     columns_name=[
    #         "ID",
    #         "ANO_CENSO",
    #         "PK_COD_ENTIDADE",
    #         "NO_ENTIDADE",
    #         "COD_ORGAO_REGIONAL_INEP",
    #         "DESC_SITUACAO_FUNCIONAMENTO",
    #         "DT_ANO_LETIVO_INICIO",
    #         "DT_ANO_LETIVO_TERMINO",
    #         "FK_COD_ESTADO",
    #         "SIGLA",
    #         "FK_COD_MUNICIPIO",
    #         "FK_COD_DISTRITO",
    #         "ID_DEPENDENCIA_ADM",
    #         "ID_LOCALIZACAO",
    #         "DESC_CATEGORIA_ESCOLA_PRIVADA",
    #         "ID_CONVENIADA_PP",
    #         "ID_TIPO_CONVENIO_PODER_PUBLICO",
    #         "ID_MANT_ESCOLA_PRIVADA_EMP",
    #         "ID_MANT_ESCOLA_PRIVADA_ONG",
    #         "ID_MANT_ESCOLA_PRIVADA_SIND",
    #         "ID_MANT_ESCOLA_PRIVADA_S_FINS",
    #         "ID_DOCUMENTO_REGULAMENTACAO",
    #         "ID_LOCAL_FUNC_PREDIO_ESCOLAR",
    #         "ID_LOCAL_FUNC_SALAS_EMPRESA",
    #         "ID_LOCAL_FUNC_PRISIONAL",
    #         "ID_LOCAL_FUNC_TEMPLO_IGREJA",
    #         "ID_LOCAL_FUNC_CASA_PROFESSOR",
    #         "ID_LOCAL_FUNC_GALPAO",
    #         "ID_LOCAL_FUNC_OUTROS",
    #         "ID_LOCAL_FUNC_SALAS_OUTRA_ESC",
    #         "ID_ESCOLA_COMP_PREDIO",
    #         "ID_AGUA_FILTRADA",
    #         "ID_AGUA_REDE_PUBLICA",
    #         "ID_AGUA_POCO_ARTESIANO",
    #         "ID_AGUA_CACIMBA",
    #         "ID_AGUA_FONTE_RIO",
    #         "ID_AGUA_INEXISTENTE",
    #         "ID_ENERGIA_REDE_PUBLICA",
    #         "ID_ENERGIA_GERADOR",
    #         "ID_ENERGIA_OUTROS",
    #         "ID_ENERGIA_INEXISTENTE",
    #         "ID_ESGOTO_REDE_PUBLICA",
    #         "ID_ESGOTO_FOSSA",
    #         "ID_ESGOTO_INEXISTENTE",
    #         "ID_LIXO_COLETA_PERIODICA",
    #         "ID_LIXO_QUEIMA",
    #         "ID_LIXO_JOGA_OUTRA_AREA",
    #         "ID_LIXO_RECICLA",
    #         "ID_LIXO_ENTERRA",
    #         "ID_LIXO_OUTROS",
    #         "ID_SALA_DIRETORIA",
    #         "ID_SALA_PROFESSOR",
    #         "ID_LABORATORIO_INFORMATICA",
    #         "ID_LABORATORIO_CIENCIAS",
    #         "ID_SALA_ATENDIMENTO_ESPECIAL",
    #         "ID_QUADRA_ESPORTES_COBERTA",
    #         "ID_QUADRA_ESPORTES_DESCOBERTA",
    #         "ID_COZINHA",
    #         "ID_BIBLIOTECA",
    #         "ID_SALA_LEITURA",
    #         "ID_PARQUE_INFANTIL",
    #         "ID_BERCARIO",
    #         "ID_SANITARIO_FORA_PREDIO",
    #         "ID_SANITARIO_DENTRO_PREDIO",
    #         "ID_SANITARIO_EI",
    #         "ID_SANITARIO_PNE",
    #         "ID_DEPENDENCIAS_PNE",
    #         "ID_DEPENDENCIAS_OUTRAS",
    #         "NUM_SALAS_EXISTENTES",
    #         "NUM_SALAS_UTILIZADAS",
    #         "ID_EQUIP_TV",
    #         "ID_EQUIP_VIDEOCASSETE",
    #         "ID_EQUIP_DVD",
    #         "ID_EQUIP_PARABOLICA",
    #         "ID_EQUIP_COPIADORA",
    #         "ID_EQUIP_RETRO",
    #         "ID_EQUIP_IMPRESSORA",
    #         "ID_COMPUTADORES",
    #         "NUM_COMPUTADORES",
    #         "NUM_COMP_ADMINISTRATIVOS",
    #         "NUM_COMP_ALUNOS",
    #         "ID_INTERNET",
    #         "ID_BANDA_LARGA",
    #         "NUM_FUNCIONARIOS",
    #         "ID_ALIMENTACAO",
    #         "ID_AEE",
    #         "ID_MOD_ATIV_COMPLEMENTAR",
    #         "ID_MOD_ENS_REGULAR",
    #         "ID_REG_INFANTIL_CRECHE",
    #         "ID_REG_INFANTIL_PREESCOLA",
    #         "ID_REG_FUND_8_ANOS",
    #         "ID_REG_FUND_9_ANOS",
    #         "ID_REG_MEDIO_MEDIO",
    #         "ID_REG_MEDIO_INTEGRADO",
    #         "ID_REG_MEDIO_NORMAL",
    #         "ID_REG_MEDIO_PROF",
    #         "ID_MOD_ENS_ESP",
    #         "ID_ESP_INFANTIL_CRECHE",
    #         "ID_ESP_INFANTIL_PREESCOLA",
    #         "ID_ESP_FUND_8_ANOS",
    #         "ID_ESP_FUND_9_ANOS",
    #         "ID_ESP_MEDIO_MEDIO",
    #         "ID_ESP_MEDIO_INTEGRADO",
    #         "ID_ESP_MEDIO_NORMAL",
    #         "ID_ESP_MEDIO_PROFISSIONAL",
    #         "ID_ESP_EJA_FUNDAMENTAL",
    #         "ID_ESP_EJA_MEDIO",
    #         "ID_MOD_EJA",
    #         "ID_EJA_FUNDAMENTAL",
    #         "ID_EJA_MEDIO",
    #         "ID_FUND_CICLOS",
    #         "ID_LOCALIZACAO_DIFERENCIADA",
    #         "ID_MATERIAL_ESP_NAO_UTILIZA",
    #         "ID_MATERIAL_ESP_QUILOMBOLA",
    #         "ID_MATERIAL_ESP_INDIGENA",
    #         "ID_EDUCACAO_INDIGENA",
    #         "ID_LINGUA_INDIGENA",
    #         "FK_COD_LINGUA_INDIGENA",
    #         "ID_LINGUA_PORTUGUESA"
    #     ]
    # )

    # print(f"\n{frame_select_stg_escolas}")

    # print(f"\n{frame_select_stg_dados_ibge.head()}")

    # frame_select_stg_dados_ibge = dms.convert_table_to_dataframe(
    #     conn_input=db,
    #     schema_name="stage",
    #     table_name="STG_DADOS_IBGE",
    #     columns_name=[
    #         "ID",
    #         "NO_NIVEL",
    #         "CD_IBGE",
    #         "NO_LOCAL",
    #         "CD_ANO"
    #     ]
    # )

    # print(f"\n{frame_select_stg_dados_ibge.head()}")

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

    print(f"Finalizado com sucesso em {(time.time() - time_exec)} segundos")
