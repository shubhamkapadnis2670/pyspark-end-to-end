from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Create_spark')


def get_spark_object(envn, appName):
    try:
        logger.info('get_spark_object_started')
        if envn == 'DEV':
            master = 'local[*]'
        else:
            master = 'yarn'

        logger.info(f'master is {format(master)}')

        spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()

    except Exception as err:
        logger.error(f'An error occurred for get_spark_object=== {str(err)}')
        raise
    else:
        logger.info('spark object created ')
    return spark
