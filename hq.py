import logging
import traceback
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp, lit, col, max as spark_max
from dataclasses import dataclass

# ========== Logging Setup ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)

def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Exception in {func.__qualname__}: {e}")
            logging.error(traceback.format_exc())
            # Handle extract_table: must return empty DataFrame on error, not None!
            if func.__name__ == "extract_table":
                self = args[0]
                return self.spark.createDataFrame(
                    self.spark.sparkContext.emptyRDD(), StructType([])
                )
            return None
    return wrapper

# ========== Configuration Using Dataclasses ==========
@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: str
    database: str
    username: str
    password: str

@dataclass(frozen=True)
class AzureConfig:
    base_path: str
    stage: str

@dataclass(frozen=True)
class SnowflakeConfig:
    sfURL: str
    sfUser: str
    sfPassword: str
    sfDatabase: str
    sfWarehouse: str
    sfSchema: str

# ========== Table Discovery ==========
class TableDiscovery:
    def __init__(self, spark: SparkSession, config: ServerConfig):
        self.spark = spark
        self.config = config

    @log_exceptions
    def discover_all_tables(self) -> List[str]:
        logging.info("Discovering all tables from the database...")
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo'
        """
        jdbc_url = (
            f"jdbc:sqlserver://{self.config.host}:{self.config.port};"
            f"databaseName={self.config.database}"
        )
        df = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", self.config.username)
            .option("password", self.config.password)
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

# ========== Data Sync with Watermark in DBFS ==========
class DataSync:
    def __init__(
        self,
        spark: SparkSession,
        server_cfg: ServerConfig,
        azure_cfg: AzureConfig,
        load_mode: str = "historical"
    ):
        self.spark = spark
        self.server_cfg = server_cfg
        self.azure_cfg = azure_cfg
        self.load_mode = load_mode
        self.metadata_base_path = "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/HQ_QLT/Metadata"

    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        columns = df.columns
        for old_col, new_col in (
            (col, col.replace(" ", "_").replace(".", "_")) for col in columns
        ):
            if old_col != new_col:
                df = df.withColumnRenamed(old_col, new_col)
        return df

    @staticmethod
    def add_metadata_columns(df: DataFrame) -> DataFrame:
        # These are the 5 required metadata columns
        return (
            df.withColumn("ETL_CREATED_DATE", current_timestamp())
            .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
            .withColumn("CREATED_BY", lit("ETL_PROCESS"))
            .withColumn("TO_PROCESS", lit(True))
            .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("HQ"))
        )

    def _get_watermark_path(self, table_name: str) -> str:
        return f"{self.metadata_base_path}/etl_last_update_{table_name}.txt"

    def read_watermark(self, table_name: str) -> Optional[str]:
        path = self._get_watermark_path(table_name)
        try:
            df = self.spark.read.text(path)
            watermark = df.first()[0].strip()
            logging.info(f"Read watermark for {table_name}: {watermark}")
            return watermark
        except Exception as ex:
            logging.info(f"No watermark found for {table_name} at {path} (likely first run): {ex}")
            return None

    def write_watermark(self, table_name: str, value: str):
        path = self._get_watermark_path(table_name)
        self.spark.createDataFrame([(value,)], ["watermark"]).coalesce(1).write.mode("overwrite").text(path)
        logging.info(f"Watermark for {table_name} written to {path}: {value}")

    @log_exceptions
    def extract_table(self, table_name: str) -> DataFrame:
        logging.info(f"Extracting data from table: {table_name}")
        jdbc_url = (
            f"jdbc:sqlserver://{self.server_cfg.host}:{self.server_cfg.port};"
            f"databaseName={self.server_cfg.database}"
        )
        etl_col = "ETL_LAST_UPDATE_DATE"
        query = f"SELECT * FROM dbo.{table_name}"

        if self.load_mode == "delta":
            watermark = self.read_watermark(table_name)
            if watermark:
                query = (
                    f"SELECT * FROM dbo.{table_name} "
                    f"WHERE [{etl_col}] > '{watermark}'"
                )
                logging.info(f"Delta extract for {table_name} using query: {query}")
            else:
                logging.info(f"No watermark found for {table_name}, performing full extract.")

        df = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", self.server_cfg.username)
            .option("password", self.server_cfg.password)
            .option("encrypt", "true")
            .option("trustServerCertificate", "true")
            .option("fetchsize", 10000)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        df = self.clean_column_names(df)
        record_count = df.count()
        logging.info(f"Extracted {record_count} records from table: {table_name}")
        return df

    @log_exceptions
    def write_to_adls(self, df: DataFrame, table_name: str):
        logging.info(f"Writing data to ADLS for table: {table_name}")
        path = f"{self.azure_cfg.base_path}/{self.azure_cfg.stage}/HQ/{table_name}"
        logging.info(f"ADLS Path: {path}")
        write_mode = "overwrite" if self.load_mode == "historical" else "append"
        (
            df.write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .mode(write_mode)
            .save(path)
        )
        logging.info(f"Data successfully written to ADLS for table: {table_name}")

# ========== Snowflake Loader ==========
class SnowflakeLoader:
    def __init__(self, spark: SparkSession, config: SnowflakeConfig, load_mode="historical"):
        self.spark = spark
        self.config = config
        self.load_mode = load_mode

    @log_exceptions
    def load_to_snowflake(self, df: DataFrame, staging_table_name: str):
        write_mode = "overwrite" if self.load_mode == "historical" else "append"
        logging.info(f"Loading data into Snowflake table: {staging_table_name} using mode {write_mode}")
        (
            df.write.format("snowflake")
            .options(**self.config.__dict__)
            .option("dbtable", staging_table_name)
            .mode(write_mode)
            .save()
        )
        logging.info(f"Data successfully loaded into Snowflake: {staging_table_name}")

# ========== Master ETL Pipeline ==========
class ETLPipeline:
    def __init__(self, spark: SparkSession, dbutils, load_mode: str = "historical"):
        # Setup configs using secrets
        server_cfg = ServerConfig(
            host="10.255.2.5",
            port="1433",
            database="HQ_QLT",
            username=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SB-HQ-User"
            ),
            password=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SB-HQ-Pass"
            ),
        )
        azure_cfg = AzureConfig(
            base_path="abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net",
            stage="RAW",
        )
        snowflake_cfg = SnowflakeConfig(
            sfURL="hmkovlx-nu26765.snowflakecomputing.com",
            sfUser=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-User"
            ),
            sfPassword=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
            ),
            sfDatabase="DEV",
            sfWarehouse="COMPUTE_WH",
            sfSchema="QUILITY_EDW_STAGE",
        )
        self.spark = spark
        self.load_mode = load_mode
        self.discovery = TableDiscovery(spark, server_cfg)
        self.sync = DataSync(spark, server_cfg, azure_cfg, load_mode)
        self.loader = SnowflakeLoader(spark, snowflake_cfg, load_mode)

    def _process_table(self, table_name: str):
        logging.info(f"Processing table: {table_name}")
        df = self.sync.extract_table(table_name)
        if not df or df.rdd.isEmpty():
            logging.warning(f"No data found in table {table_name}. Skipping.")
            return

        df = self.sync.add_metadata_columns(df)
        self.sync.write_to_adls(df, table_name)
        staging_table_name = f"DEV.QUILITY_EDW_STAGE.STG_HQ_{table_name.upper()}"
        self.loader.load_to_snowflake(df, staging_table_name)

        etl_col = "ETL_LAST_UPDATE_DATE"
        if etl_col in df.columns:
            max_val = df.agg(spark_max(col(etl_col))).collect()[0][0]
            if max_val:
                self.sync.write_watermark(table_name, str(max_val))
            else:
                logging.warning(f"No max value found for {etl_col} in {table_name}. Watermark not updated.")
        else:
            logging.error(f"{etl_col} not present in data for {table_name}; cannot update watermark.")

    def run(self, max_workers: int = 4):
        tables = self.discovery.discover_all_tables()
        if not tables:
            logging.error("No tables to process. Exiting.")
            return
        logging.info(f"Total tables to process: {len(tables)}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(self._process_table, tables))
        logging.info("HQ_QLT ETL process completed successfully.")

# ========== Main Entrypoint ==========
def main(load_mode="historical"):
    logging.info(f"Starting the HQ_QLT ETL process in {load_mode} mode...")
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
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
        .getOrCreate()
    )
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except Exception:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    pipeline = ETLPipeline(spark, dbutils, load_mode=load_mode)
    pipeline.run()

if __name__ == "__main__":
    # Pass "historical" for full overwrite, or "delta" for incremental append mode
    main(load_mode="delta")  # Or "historical"
