from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import configparser
from com.genereic_flattening import DataframeFlattenExplode

# Pass the arguments at run time and read used config parser
config = configparser.RawConfigParser()
print("My args are", sys.argv[2])
config_path = sys.argv[2]
config.read(config_path)

root_path = config['ReadFileParameterSection']['rootPath']
file_format = config['ReadFileParameterSection']['fileFormat']
file_header = config['ReadFileParameterSection']['fileHeader']
file_delimiter = config['ReadFileParameterSection']['fileDelimiter']
file_name = config['ReadFileParameterSection']['fileName']
root_tag = config['ReadFileParameterSection']['rootTag']
row_tag = config['ReadFileParameterSection']['rowTag']
json_multiline = config['ReadFileParameterSection']['jsonMultiline']
column_name_separator = config['ReadFileParameterSection']['columnNameSeparator']
adls_key = config['ReadFileParameterSection']['azureKey']
adls_value = config['ReadFileParameterSection']['azureValue']

file_path = root_path + file_name

if __name__ == '__main__':
    # Creating Spark Session object
    spark = (SparkSession.builder.appName("Flatten and Explode Dataframe").getOrCreate())

    # Configuration to access storage account
    spark._jsc.hadoopConfiguration().set(adls_key, adls_value)

    cls = DataframeFlattenExplode(root_tag, row_tag, json_multiline, column_name_separator)
    inputDF = cls.get_file_data(spark, file_format, file_delimiter, file_header, file_path)
    df_transform = cls.flattening_check(inputDF)
