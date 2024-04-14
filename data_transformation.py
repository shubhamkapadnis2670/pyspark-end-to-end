import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Data_transformation')
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from udfs import *


def data_report1(df_city_sel, df_presc_sel):
    try:
        loggers.warning('processing data_report1 method...')
        loggers.warning(f'calculationg total zip counts in {df_city_sel}')
        df_city_sel = df_city_sel.withColumn('zip_counts', column_split_count(df_city_sel.zips))
        loggers.warning('calculating distinct prescribers and total tx_cnt')
        df_presc_grp = df_presc_sel.groupBy(col('presc_state'), col('presc_city')).agg(
            countDistinct(col('presc_id')).alias('presc_count'), sum(col('tx_cnt')).alias('tx_counts'))
        loggers.warning('do not report a city if no prescriber is assigned to it...lets join df_city_sel and '
                        'df_presc_sel')
        df_city_join = df_city_sel.join(df_presc_grp, (df_city_sel.state_id == df_presc_grp.presc_state) & (
                    df_city_sel.city == df_presc_grp.presc_city), 'inner')
        df_final = df_city_join.select('city', 'state_name', 'county_name', 'population', 'zip_counts', 'presc_count')

    except Exception as e:
        loggers.error(f'An error occurred while dealing report!...{str(e)}')
        raise
    else:
        loggers.warning('DAta report1 successfully executed')

    return df_final


def data_report2(df_presc_sel):
    try:
        loggers.warning('executing data_report2 method')
        loggers.warning(
            'executing the task ::: consider the prescriber only from 20 to 50 years_of_exp and rank the prescribers based on their tx_cnt for each state')
        wspec = Window.partitionBy('presc_state').orderBy(col('tx_cnt').desc())
        df_presc_report = df_presc_sel.select(col('presc_id'), col('presc_fullname'), col('presc_state'),
                                              col('country_name'),
                                              col('years_of_exp'), col('tx_cnt'), col('total_day_supply'),
                                              col('total_drug_cost')) \
            .filter((col('years_of_exp') >= 20) & (col('years_of_exp') <= 50)).withColumn('dense_rank',
                                                                                          dense_rank().over(
                                                                                              wspec)).filter(
            col('dense_rank') <= 5)

    except Exception as e:
        loggers.error(f'An error occurred while executing data_report2 {str(e)}')
        raise
    else:
        loggers.warning('data_report2 method executed successfully')
    return df_presc_report
