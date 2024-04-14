import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('started get_current_date method...')
        output = spark.sql("select current_date")
        loggers.warning(f"validating spark object with current date {output.collect()}")

    except Exception as e:
        loggers.error('An error occurred in get_current_date', str(e))

        raise
    else:
        loggers.warning('validation done , get started')


def print_schema(df, df_name):
    try:
        loggers.warning(f'print schema method executing for dataframe {df_name}')
        sch = df.schema.fields
        for i in sch:
            loggers.info(f"\t{i}")
    except Exception as e:
        loggers.error(f'an error occurred at print schema :: {str(e)}')
        raise
    else:
        loggers.info('print schema done....go frwd')