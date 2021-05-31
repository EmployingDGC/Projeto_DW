
SK = (-1, -2, -3)
CD = (-1, -2, -3)
DS = ("Não Informado", "Não Aplicável", "Desconhecido")

DIMENSIONS_NAMES = (
    "D_LOCALIDADE",
    "D_ESCOLA",
    "D_TURMA"
)

SK_NAMES = (
    "SK_LOCALIDADE",
    "SK_ESCOLA",
    "SK_TURMA"
)

STAGES_NAMES = (
    "STG_DADOS_IBGE",
    "STG_ESCOLAS",
    "STG_TS_RESULTADO_ALUNO"
)

SCHEMA_NAMES = (
    "stage",
    "dw"
)

KEYS_FATO = (
    "SK_LOCALIDADE",
    "SK_ESCOLA",
    "SK_TURMA",
    "CD_ANO",
    "VL_PROFICIENCIA_MT_SAEB",
    "VL_PROFICIENCIA_LP_SAEB",
    "FL_SITUACAO_CENSO",
    "FL_PREENCHIMENTO",
    "FL_PROFICIENCIA"
)

KEYS_DIMENSIONS = {
    "LOCALIDADE": (
        "SK_LOCALIDADE",
        "CD_MUNICIPIO",
        "CD_UF",
        "DS_MUNICIPIO",
        "DS_UF"
    ),
    "ESCOLAS": (
        "SK_ESCOLA",
        "CD_ESCOLA",
        "CD_LOCALIZACAO",
        "CD_DEPENDENCIA_ADM",
        "NO_ESCOLA",
        "DS_LOCALIZACAO",
        "DS_DEPENDENCIA_ADM"
    ),
    "TURMAS": (
        "SK_TURMA",
        "CD_TURMA",
        "CD_TURNO",
        "CD_SERIE",
        "DS_TURNO",
        "DS_SERIE"
    )
}

FATO_NAME = "F_PROVA_BR"

TRUE_VALUES = ("TRUE", "1", "YES", "OK", "VERDADE", "V", "T")
