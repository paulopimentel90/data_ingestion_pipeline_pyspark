import os
import shutil

def load_data(df, destination_config):
    """
    Salva o DataFrame em um único arquivo parquet com o nome e caminho exato do destination_config["path"].
    """
    destination_path = destination_config["path"]
    temp_dir = destination_path + "_temp_dir"

    # Salva em uma única partição no diretório temporário
    df.coalesce(1).write.mode("overwrite").parquet(temp_dir)

    # Lista os arquivos parquet na pasta temporária
    temp_files = os.listdir(temp_dir)
    parquet_files = [f for f in temp_files if f.endswith(".parquet")]

    if not parquet_files:
        raise Exception("Nenhum arquivo parquet encontrado na pasta temporária")

    # Caminho completo do arquivo parquet gerado
    source_file = os.path.join(temp_dir, parquet_files[0])

    # Move e renomeia para o destino final
    shutil.move(source_file, destination_path)

    # Remove o diretório temporário
    shutil.rmtree(temp_dir)
