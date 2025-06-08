from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
import logging
import traceback
from pyspark.sql.functions import current_timestamp, lit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)


def clean_column_names(df: DataFrame) -> DataFrame:
    for column in df.columns:
        new_column = column.replace(" ", "_").replace(".", "_")
        df = df.withColumnRenamed(column, new_column)
    return df


def add_metadata_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ETL_CREATED_DATE", current_timestamp())
        .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
        .withColumn("CREATED_BY", lit("ETL_PROCESS"))
        .withColumn("TO_PROCESS", lit(True))
        .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("HQ"))
    )


class TableDiscovery:
    def __init__(self, spark: SparkSession, server_details: Dict[str, str]) -> None:
        self.spark: SparkSession = spark
        self.server_details: Dict[str, str] = server_details

    def discover_all_tables(self) -> List[str]:
        try:
            logging.info("Discovering all tables from the database...")
            query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo'
            """
            jdbc_url = (
                f"jdbc:sqlserver://{self.server_details['host']}:{self.server_details['port']};"
                f"databaseName={self.server_details['database']}"
            )
            logging.info(f"JDBC URL: {jdbc_url}")

            df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
            )
            tables = [row.TABLE_NAME for row in df.collect()]
            logging.info(f"Discovered tables: {tables}")
            if not tables:
                logging.warning("No tables found in the schema 'dbo'.")
            return tables
        except Exception as e:
            logging.error(f"Error discovering tables: {e}")
            logging.error(traceback.format_exc())
            return []


class DataSync:
    def __init__(
        self,
        spark: SparkSession,
        server_details: Dict[str, str],
        azure_details: Dict[str, str],
    ) -> None:
        self.spark: SparkSession = spark
        self.server_details: Dict[str, str] = server_details
        self.azure_details: Dict[str, str] = azure_details

    def extract_table(self, table_name: str) -> DataFrame:
        try:
            logging.info(f"Extracting data from table: {table_name}")
            jdbc_url = (
                f"jdbc:sqlserver://{self.server_details['host']}:{self.server_details['port']};"
                f"databaseName={self.server_details['database']}"
            )

            query = f"SELECT * FROM dbo.{table_name}"

            df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details["username"])
                .option("password", self.server_details["password"])
                .option("encrypt", "true")
                .option("trustServerCertificate", "true")
                .option("fetchsize", 10000)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
            )

            df = clean_column_names(df)
            record_count = df.count()
            logging.info(f"Extracted {record_count} records from table: {table_name}")
            return df

        except Exception as e:
            logging.error(f"Error extracting table {table_name}: {e}")
            logging.error(traceback.format_exc())
            return self.spark.createDataFrame([], schema=None)

    def write_to_adls(self, df: DataFrame, table_name: str) -> None:
        try:
            logging.info(f"Writing data to ADLS for table: {table_name}")
            path = f"{self.azure_details['base_path']}/{self.azure_details['stage']}/HQ/{table_name}"
            logging.info(f"ADLS Path: {path}")

            (
                df.write.format("delta")
                .option("delta.columnMapping.mode", "name")
                .option("delta.minReaderVersion", "2")
                .option("delta.minWriterVersion", "5")
                .mode("overwrite")
                .save(path)
            )

            logging.info(f"Data successfully written to ADLS for table: {table_name}")
        except Exception as e:
            logging.error(f"Error writing to ADLS for table {table_name}: {e}")
            logging.error(traceback.format_exc())


class SnowflakeLoader:
    def __init__(self, spark: SparkSession, snowflake_config: Dict[str, str]) -> None:
        self.spark: SparkSession = spark
        self.config: Dict[str, str] = snowflake_config

    def load_to_snowflake(self, df: DataFrame, staging_table_name: str) -> None:
        try:
            logging.info(f"Loading data into Snowflake table: {staging_table_name}")
            (
                df.write.format("snowflake")
                .options(**self.config)
                .option("dbtable", staging_table_name)
                .mode("overwrite")
                .save()
            )
            logging.info(
                f"Data successfully loaded into Snowflake: {staging_table_name}"
            )
        except Exception as e:
            logging.error(
                f"Error loading data into Snowflake for table {staging_table_name}: {e}"
            )
            logging.error(traceback.format_exc())


def process_table(
    table_name: str, data_sync: DataSync, snowflake_loader: SnowflakeLoader
) -> None:
    logging.info(f"Starting to process table: {table_name}")
    try:
        df = data_sync.extract_table(table_name)
        if df.rdd.isEmpty():
            logging.warning(f"No data found in table {table_name}. Skipping.")
            return

        df = add_metadata_columns(df)

        # Write to ADLS
        data_sync.write_to_adls(df, table_name)

        # Write to Snowflake for all tables
        staging_table_name = f"DEV.QUILITY_EDW_STAGE.STG_HQ_{table_name.upper()}"
        snowflake_loader.load_to_snowflake(df, staging_table_name)

    except Exception as e:
        logging.error(f"Error processing table {table_name}: {e}")
        logging.error(traceback.format_exc())


def main() -> None:
    try:
        logging.info("Starting the HQ_QLT ETL process...")

        spark = (
            SparkSession.builder.appName("HQ_QLT_ETL")
            .config(
                "spark.jars.packages",
                "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8,"
                "net.snowflake:snowflake-jdbc:3.13.8,"
                "net.snowflake:spark-snowflake_2.12:2.9.3",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.databricks.delta.properties.defaults.columnMapping.mode", "name"
            )
            .getOrCreate()
        )
        logging.info("Spark session initialized.")

        server_details = {
            "host": "10.255.2.5",
            "port": "1433",
            "database": "HQ_QLT",
            "username": dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SB-HQ-User"
            ),
            "password": dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SB-HQ-Pass"
            ),
        }

        azure_details = {
            "base_path": "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net",
            "stage": "RAW",
        }

        snowflake_config = {
            "sfURL": "https://hmkovlx-nu26765.snowflakecomputing.com",
            "sfUser": dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-User"
            ),
            "sfPassword": dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
            ),
            "sfDatabase": "DEV",
            "sfWarehouse": "COMPUTE_WH",
            "sfSchema": "QUILITY_EDW_STAGE",
        }

        discovery = TableDiscovery(spark, server_details)
        data_sync = DataSync(spark, server_details, azure_details)
        snowflake_loader = SnowflakeLoader(spark, snowflake_config)

        all_tables = discovery.discover_all_tables()
        if not all_tables:
            logging.error("No tables to process. Exiting the script.")
            return

        logging.info(f"Total tables to process: {len(all_tables)}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for table_name in all_tables:
                futures.append(
                    executor.submit(
                        process_table, table_name, data_sync, snowflake_loader
                    )
                )
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing table: {e}")
                    logging.error(traceback.format_exc())

        logging.info("HQ_QLT ETL process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    main()
