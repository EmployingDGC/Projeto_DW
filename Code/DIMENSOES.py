from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import utilities as utl
import STAGES as stgs
import DEFAULTS_VALUES as DFLT
import D_LOCALIDADE as d_loc
import D_TURMA as d_tu
import D_ESCOLA as d_es


def treat_all_dimensions(dados_ibge: pd.DataFrame,
                         escolas: pd.DataFrame,
                         resultado_aluno: pd.DataFrame) -> list[pd.DataFrame]:
    return [
        d_loc.treat_d_localildade(dados_ibge, resultado_aluno),
        d_es.treat_d_escola(escolas),
        d_tu.treat_d_turma(resultado_aluno)
    ]


def create_all_dimensions(conn_output: MockConnection,
                          schema_name: str,
                          frames: list[pd.DataFrame],
                          dimensions_names: list[str],
                          replace_table: bool = True) -> bool:
    if len(frames) != len(dimensions_names):
        return False

    if replace_table:
        utl.drop_tables(
            conn_output=conn_output,
            schema_name=schema_name,
            dimensions_names=dimensions_names
        )

    utl.create_schema(
        database=conn_output,
        schema_name=schema_name
    )

    for i in range(len(dimensions_names)):
        frame_in_parts = utl.partition_data_frame(
            data_frame=frames[i],
            qty_parts=1000
        )

        for df in frame_in_parts:
            df.to_sql(
                con=conn_output,
                schema=schema_name.lower(),
                name=dimensions_names[i],
                if_exists="append",
                index=False
            )

    return True


def run(conn_output: MockConnection) -> None:
    frames = stgs.get_all_stages(conn_output)

    list_dimensions = treat_all_dimensions(
        escolas=frames[0],
        dados_ibge=frames[1],
        resultado_aluno=frames[2]
    )

    create_all_dimensions(
        conn_output=conn_output,
        schema_name=DFLT.SCHEMA_NAMES[1],
        frames=list_dimensions,
        dimensions_names=DFLT.DIMENSIONS_NAMES
    )
