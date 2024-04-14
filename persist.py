import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Persist')

def data_hive_perist_city(spark, df, dfname, partitionBy, mode):
    try:
        loggers.warning(f'persisting the data into hive table for dataframe {dfname}')
        loggers.warning('creating database')
        spark.sql("""create database if not exists cities""")
        spark.sql("""use cities""")
        loggers.warning(f'no writing {df} into hive table {partitionBy}')
        df.write.saveAsTable(dfname, partitionBy= partitionBy, mode=mode)

    except Exception as e:
        loggers.error(f'error while executing data_hive_persist_city method {str(e)}')
        raise
    else:
        loggers.warning('successfully executed data_hive_persist_city method')


def data_hive_perist_presc(spark, df, dfname, partitionBy, mode):
    try:
        loggers.warning(f'persisting the data into hive table for dataframe {dfname}')
        loggers.warning('creating database')
        spark.sql("""create database if not exists presc""")
        spark.sql("""use presc""")
        loggers.warning(f'no writing {df} into hive table {partitionBy}')
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as e:
        loggers.error(f'error while executing data_hive_persist_presc method {str(e)}')
        raise
    else:
        loggers.warning('successfully executed data_hive_persist_presc method')



