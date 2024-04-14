import os
import sys
from time import perf_counter
import get_env_variables as gav
from create_spark import get_spark_object
from validate_spark import get_current_date, print_schema
import logging
import logging.config
from ingest import load_files, display_df, df_count
from data_processing import data_clean
from data_transformation import data_report1, data_report2
from extraction_file import *
from persist import  *

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_format, header, file_dir, inferSchema
    try:
        start_time = perf_counter()
        logging.info('i am the main method')
        # print(gav.header)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('validating spark object')
        get_current_date(spark)
        for file in os.listdir(gav.src_olap):
            if file.endswith('.parquet'):
                file_dir = gav.src_olap + '/' + file
                header = 'NA'
                file_format = 'parquet'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_dir = gav.src_olap + '/' + file
                header = gav.header
                file_format = 'csv'
                inferSchema = gav.inferSchema
        logging.info(f'reading file which is of {format(file_format)}')

        df_city = load_files(spark=spark, file_dir=file_dir, header=header, inferSchema=inferSchema,
                             file_format=file_format)
        logging.info(f'displaying the dataframe {df_city}')
        display_df(df_city, 'df_city')

        logging.info('validating the dataframe...')
        df_count(df_city, 'df_city')

        for file in os.listdir(gav.src_oltp):
            if file.endswith('.parquet'):
                file_dir = gav.src_oltp + '/' + file
                header = 'NA'
                file_format = 'parquet'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_dir = gav.src_oltp + '/' + file
                header = gav.header
                file_format = 'csv'
                inferSchema = gav.inferSchema
        logging.info(f'reading file which is of {format(file_format)}')

        df_fact = load_files(spark=spark, file_dir=file_dir, header=header, inferSchema=inferSchema,
                             file_format=file_format)
        logging.info(f'displaying the dataframe {df_fact}')
        display_df(df_fact, 'df_fact')

        logging.info('validating the dataframe...')
        df_count(df_fact, 'df_fact')
        logging.info('implementing data processing methods')
        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)
        display_df(df_city_sel,'df_city')
        display_df(df_presc_sel,'df_fact')

        logging.info('validating schema for dataframes')
        print_schema(df_city_sel, 'df_city')
        print_schema(df_presc_sel, 'df_fact')

        logging.info('data trsnformation executing')
        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        logging.info('displaying the data_report1')
        display_df(df_report_1,'data_report')
        df_report_2 = data_report2(df_presc_sel)
        logging.info('displaying the data_report_2')
        display_df(df_report_2,'data_report_2')
        city_path = gav.city_path
        extract_files(df_report_1, 'orc',city_path,1,False,'snappy')
        presc_path = gav.presc_path
        extract_files(df_report_2, 'parquet', presc_path, 2, False,'snappy')
        logging.info('writing into hive table')
        data_hive_perist_city(spark, df_report_1, 'df_city', partitionBy= 'state_name', mode='append')
        data_hive_perist_presc(spark, df_report_2, 'df_fact', partitionBy='presc_state', mode='append')
       
        end_time = perf_counter()
        logging.info(f'total amount time taken to execute application {end_time - start_time :.2f} seconds')




    except Exception as err:
        logging.error('An error occurred when calling main() please check the trace===', str(err))
        sys.exit(1)


if __name__ == '__main__':

    main()

    logging.info('Application done')
