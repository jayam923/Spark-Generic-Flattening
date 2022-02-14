# Generic Flattening 

    This pyspark code helps us to Flatten and Explode a pyspark dataframe with nested columns of multiple levels having column data types as struct and array 

### Required Components
1.	Datalake:

    Used to place the input/source files of file formats like csv, parquet, xml, json, delta

2.	Custom python code

    The logic is written here by considering different file formats (csv, parquet, json, xml, delta) and data with nested columns having data types as struct and array

3.	Environment Setup

    Azure Datalake Gen2
    Azure Databricks
    mount Datalake to databricks or Account key configuration
    For xml file format, we need to install cluster library com.databricks:spark-xml_2.12:0.9.0 (or higher version if available)

### Parameter Structure expected for the current implementation:
    1. rootPath : File root path in ADLS Gen2 [String]
    2. fileFormat : Any of the file formats mentioned above [String]
    3. fileHeader : true if available otherwise false [String]
    4. fileDelimiter : Field seperator if availble, otherwise null [String]
    5. fileName : Exact filename [String]
    6. rootTag : Root level tag(For xml file format) [String]
    7. rowTag : Row level tag(For xml file format)  [String]
    8. jsonMultiline : True if multiline otherwise false(for json file format) [String]
    9. azureKey : adls account key configuration [String]
    10.azureValue: adls access key [String]
    11.columnNameSeparator: Special characters or space in column names that needs to be replaced [String]

### Data Flow Diagram:
![FlowDiagram.png](https://github.com/jayam923/Spark-Generic-Flattening/blob/main/FlowDiagram.png)

### Process Diagram:
![ProcessDiagram.png]()

### Python Logic script File

    Refer /com/generic_flattening

### Execution script
   
    Refer main.py

### Configuration File

    Refer /config/parameterConfig.ini

