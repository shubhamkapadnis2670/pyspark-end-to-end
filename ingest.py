import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger("Ingest")


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_files method started')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format('csv').option('header',f'{header}').option('inferSchema',f'{inferSchema}').load(file_dir)

    except Exception as e:
        logger.error(f'An error occured at load_file===={str(e)}')
        raise
    else:
        logger.warning(f'dataframe created successfully which is of {format(file_format)}')
    return df


def display_df(df, df_name):
    df_show = df.show()
    return df_show


def df_count(df, df_name):
    try:
        logger.warning(f'here to count the records in the {df_name}')
        df_c = df.count()
    except Exception as e:
        raise
    else:
        logger.warning(f"Number of records present in the {df} are {df_c}")
    return df_c
