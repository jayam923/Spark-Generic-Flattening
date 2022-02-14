from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f

class DataframeFlattenExplode():

    def __init__(self, root_tag: str, row_tag: str, json_multiline: str, column_name_separator: str) -> None:
        """
        This function get the xml root and row details as parameters which will be used by get_file_data function if the file format is xml.

        Args:
                root_tag ([string]): Root tag for XML file, otherwise null
                row_tag ([string]): Row tag for XML file, otherwise null.
                json_multiline ([string]): Boolean value to specify the multiline parameter in json
                column_name_separator ([string]): Special or any other character in column names which needs to be replaced
        """
        self.root_tag = root_tag
        self.row_tag = row_tag
        self.json_multiline = json_multiline
        self.column_name_separator = column_name_separator

    def get_file_data(self, spark, file_format: str, file_delimiter: str, file_header: str, file_path: str) -> DataFrame:
        """
        This function takes the file format, delimiter, header and path details as parameters and read the file of any format as a pyspark dataframe.

        Args:
                file_format ([string]): The file format to be read (csv, parquet, json, xml, delta)
                file_delimiter ([string]): This field seperator of a file, otherwise null
                file_header ([string]): Boolean value to specify the header for a file, otherwise null
                file_path ([string]): The file path from where the file is read.

            Returns:
                [DataFrame]: Returns the pyspark dataframe
        """
        if file_format == "csv":
            df = spark.read \
                .option("delimiter", file_delimiter) \
                .option("header", file_header) \
                .csv(file_path) \
                .cache()
        elif file_format == "parquet":
            df = spark.read \
                .parquet(file_path)
        elif file_format == "xml":
            df = spark.read.format("com.databricks.spark.xml") \
                .option("rootTag", self.root_tag).option("rowTag", self.row_tag) \
                .load(file_path)
        elif file_format == "json":
            df = spark.read.option("multiline", self.json_multiline) \
                .json(file_path)
        elif file_format == "delta":
            df = spark.read \
                .format("delta").load(file_path)
        elif file_format != "delta":
            df = "wrong file format specified"
        return df

    def flattening_check(self, unflattened_df: DataFrame) -> DataFrame:
        """
        This function will take the raw dataframe as an input and check for struct and array columns. If yes, execute the inner functions
        Args:
                unflattened_df ([Dataframe]): File as dataframe with nested columns

            Returns:
                [DataFrame]: Returns the dataframe with flattened and exploded columns
        """
        dit = {}
        dit['main_df'] = unflattened_df
        for col_name, col_type in unflattened_df.dtypes:

            if col_type[:6] == 'struct':
                dit['main_df'] = self.flatten_df(dit['main_df'])

            if col_type[:5] == 'array':
                dit['main_df'] = self.explode_array(dit['main_df'], col_name)

        print(dit['main_df'].printSchema())

        struct_types = [c[0] for c in dit['main_df'].dtypes if c[1][:6] == 'struct']
        array_types = [c[0] for c in dit['main_df'].dtypes if c[1][:5] == 'array']
        if len(struct_types) > 0 or len(array_types) > 0:
            dit["main_df"] = self.flattening_check(dit['main_df'])

        return dit['main_df']

    def flatten_df(self, nested_df: DataFrame) -> DataFrame:
        """
        Args:
                nested_df ([Dataframe]): Dataframe which has columns of struct type.

            Returns:
                [DataFrame]: Returns the dataframe with the flattened values included
        """
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

        flat_df = nested_df.select(flat_cols + [f.col(nc + '.' + c).alias(nc + '_' + c) for nc in nested_cols for c in
                                                nested_df.select(nc + '.*').columns])

        new_column_name_list = list(map(lambda x: x.replace(self.column_name_separator, "_"), flat_df.columns))

        flat_df = flat_df.toDF(*new_column_name_list)

        return flat_df

    def explode_array(self, nested_df: DataFrame, column: str) -> DataFrame:
        """
        Args:
                nested_df ([Dataframe]): Dataframe which has columns of array type.
                column ([string]): column with datatype as array and to be exploded

            Returns:
                [DataFrame]: Returns the dataframe with the exploded values included
        """
        explode_col_name = nested_df.select(column)
        exploded_df = nested_df.withColumn(column, f.explode_outer(explode_col_name[0]))

        return exploded_df
