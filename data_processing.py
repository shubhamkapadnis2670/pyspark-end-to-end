import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *


logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Data_processing')


def data_clean(df1, df2):
    try:
        loggers.warning('data_clean method started.....')
        loggers.warning('selecting required columns from olap dataset and converting some column into upper case')
        df_city_sel = df1.select(upper(col('city')).alias('city'), col('state_id'), upper(col('state_name')).alias('state_name'),
                                 upper(col('county_name')).alias('county_name'), col('population'), col('zips'))
        loggers.warning('working on oltp dataset and selecting couple of columns and rename..')
        df_presc_sel = df2.select(col('npi').alias('presc_id'), col('nppes_provider_last_org_name').alias('presc_lname'),
                                  col('nppes_provider_first_name').alias('presc_fname'),col('nppes_provider_city').alias('presc_city'),
                                  col('nppes_provider_state').alias('presc_state'),col('specialty_description').alias('presc_spclt'),
                                  col('drug_name'),col('total_claim_count').alias('tx_cnt'),col('total_day_supply'),col('total_drug_cost'),
                                  col('total_drug_cost'),col('years_of_exp'))

        loggers.warning('adding new column to df_presc_sel')
        df_presc_sel = df_presc_sel.withColumn('country_name',lit('USA'))
        loggers.warning('converting years_of_exp from string to int and replacing =')
        df_presc_sel = df_presc_sel.withColumn('years_of_exp',substring(col('years_of_exp'),3,8)) \
            .withColumn('years_of_exp',col('years_of_exp').cast('int'))
        loggers.warning('concat first and last name to form column presc_fullname and dropping those two columns')
        cols = ('presc_lname','presc_fname')
        df_presc_sel = df_presc_sel.withColumn('presc_fullname',concat_ws(" ",col('presc_lname'), col('presc_fname'))).drop(*cols)
        loggers.warning('check for null in all columns and dropping null values where null_values > 10')
        total_count = df_presc_sel.count()
        loggers.warning(f'before dropping null values, total number of rows are {total_count}')
        columns = df_presc_sel.columns
        drop_na_columns = []
        for i in columns:
            if df_presc_sel.select(col(i)).where(col(i).isNull()).count() > 10:
                drop_na_columns.append(i)

        df_presc_sel = df_presc_sel.dropna(subset=drop_na_columns)
        total_count = df_presc_sel.count()
        loggers.warning(f'after dropping null values, total number of rows are {total_count}')



    except Exception as err:
        loggers.error(f'error occurred in data_clean() method {str(err)}')
    else:
        loggers.warning('data_clean() methods execution done, go frwd')
        return df_city_sel, df_presc_sel

