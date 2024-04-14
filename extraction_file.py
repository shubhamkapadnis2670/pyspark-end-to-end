import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Extraction')


def extract_files(df, format, filepath, split_no, headerReq, compressionType):
    try:
        loggers.warning('extract_files method started successfully')
        df.coalesce(split_no).write.mode('overwrite').format(format).save(filepath, header= headerReq, compression= compressionType)

    except Exception as e:
        loggers.error(f"an error occurred while extracting files {str(e)}")
        raise
    else:
        loggers.warning('successfully extracted files...')