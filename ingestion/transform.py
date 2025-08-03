def transform_data(df):
    # REMOVE LINHAS NULAS E DUPLICADAS
    # df = df.dropna().dropDuplicates()
    # REMOVE APENAS LINHAS DUPLICADAS
    df = df.dropDuplicates()
    # MOSTRA O DATA FRAME
    # df.show()
    return df
