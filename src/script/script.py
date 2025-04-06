from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, when, trim
import logging

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Configuração da SparkSession com conector GCS
    try:
        spark = SparkSession.builder \
            .appName("Processamento-CNAE-GCP") \
            .getOrCreate()
        
        logger.info("SparkSession iniciada com sucesso")
        
        # Desativar arquivo _SUCCESS
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "mapreduce.fileoutputcommitter.marksuccessfuljobs", 
            "false"
        )

        # Caminhos GCS
        input_path = "gs://big-data-dw/raw/Cnaes.csv"
        output_path = "gs://big-data-dw/trusted"
        
        # Schema
        schema = StructType([
            StructField("CODIGO", IntegerType(), nullable=True),
            StructField("DESCRICAO", StringType(), nullable=True)
        ])

        # Ler arquivo CSV
        logger.info(f"Iniciando leitura do arquivo: {input_path}")
        df = spark.read \
            .format("csv") \
            .option("header", "false") \
            .option("delimiter", ";") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .schema(schema) \
            .load(input_path)

        logger.info(f"Total de registros lidos: {df.count()}")

        # Transformações
        logger.info("Aplicando transformações nos dados")
        df_processed = df.withColumn("DESCRICAO", trim(col("DESCRICAO"))) \
            .withColumn("DESCRICAO", 
                      when(col("DESCRICAO").eqNullSafe(""), None)
                      .otherwise(col("DESCRICAO"))) \
            .withColumn("SEGMENTO",
                      when(col("CODIGO") % 2 == 1, "PRIMARIO")  # Ímpares
                      .otherwise("SECUNDARIO"))  # Pares

        # Salvar como arquivo único CSV diretamente
        logger.info(f"Escrevendo dados processados em: {output_path}")
        (df_processed
         .coalesce(1)
         .write
         .format("csv")
         .option("header", "true")
         .option("delimiter", "|")
         .mode("overwrite")
         .save(output_path))
        
        logger.info(f"Processamento concluído com sucesso! Arquivo salvo em: {output_path}")

    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            logger.info("Encerrando SparkSession")
            spark.stop()

if __name__ == "__main__":
    main()