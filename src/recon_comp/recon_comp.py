from pyspark.sql import DataFrame
import uuid
from pyspark.sql.functions import lit,current_timestamp,current_user,col,struct,to_json,collect_list,when,explode
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType,ArrayType

class Recon:

    def __init__(self,spark:SparkSession,sql_comp:str,audit_table:str) -> None:

        """
            Initialize the DataComparator object.

            Parameters:
                spark (SparkSession): The Spark session object.
                sql_comp (str): A string indicating whether SQL comparison should be performed ('y' or 'n').
                audit_table (str): The name of the audit table to store comparison results.

            Raises:
                ValueError: If 'sql_comp' is not 'y' or 'n'.
        """
        self.spark = spark 
        self.sql_comp = sql_comp 
        self.audit_table = audit_table

        # Validate the 'sql_comp' input. It must be 'y' or 'n'.

        if self.sql_comp.lower() not in ['y','n']:
            raise ValueError("Invalid input: 'sql_comp' must be 'y or 'n'.")
        
        # Define the schema for the final results DataFrame
        
        self.final_schema = StructType([StructField('Table1', StringType(), False), 
                                    StructField('Table2', StringType(), False), 
                                    StructField('Results', StringType(), True),
                                    StructField('CheckType', StringType(), False),
                                    StructField('CheckStatus', StringType(), False)])

    def __validate_sql(self,sql):

        """
            Validate the provided SQL string by attempting to execute it.

            Parameters:
                sql (str): The SQL query string to validate.

            Returns:
                DataFrame: The DataFrame resulting from the SQL query execution if successful.

            Raises:
                ValueError: If 'sql' is an empty string.
                AnalysisException: If an error occurs while executing the SQL query and it is not related to unresolved columns.
        """
        print(F"Validation Sql")
        if not sql:
            raise ValueError("Invalid input: 'sql' must be a non-empty string.")
        try:
            df = self.spark.sql(sql)
            return df
        except AnalysisException as e:
            if "cannot resolve" in str(e):
                return False
            else:
                raise e
            
    def __validate_table_name(self,table_name):

        """
            Validate the provided table name string.

            Parameters:
            table_name (str): The table name to validate. It should include catalog and database name.

            Raises:
            ValueError: If 'table_name' is not a non-empty string with exactly two dots (catalog and database name).
       """
        print(F"Validation table name")
        if table_name:
            table_name_dot_cnt = table_name.count(".")
            if not (table_name_dot_cnt == 2 ) or not isinstance(table_name, str):
                raise ValueError(F"Invalid input: {table_name} must be a non-empty string and with catalog and database name")
        else:
                raise ValueError(F"Invalid input: table name must be a non-empty string and with catalog and database name")

# Function to identify column types
    def identify_column_types(self,df:DataFrame) -> dict:

        """
        Identify the types of columns in a DataFrame.

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            dict: A dictionary containing column names categorized by their types.
                The keys are 'arraytype', 'structtype', and 'arraytype_struct'.
                The values are lists of column names belonging to each category.
        """
        column_types = {
            "arraytype": [],
            "structtype": [],
            "arraytype_struct": []
        }

        for field in df.schema.fields:
            if isinstance(field.dataType, ArrayType):
                if isinstance(field.dataType.elementType, StructType):
                    column_types["arraytype_struct"].append(field.name)
                else:
                    column_types["arraytype"].append(field.name)
            elif isinstance(field.dataType, StructType):
                column_types["structtype"].append(field.name)
        
        return column_types


    def expand_complex_fields(self,df:DataFrame,type:str,primary_keys:list,field:str) -> DataFrame:

        """
        expands dataframe for complex fields

        Args:
            df (DataFrame): Input DataFrame.
            type (str): complex datatype of field. Either of these values 'arraytype', 'structtype', and 'arraytype_struct'
            primary_keys (list): primary_keys of table
            field (str): field name that has complex data type 

        Returns:
            tuple: dataframe: expanded datafrmae for the complex field and sub_fields in the complex field
        """      

        if type == 'arraytype':

            sub_fields = [field]

            df_exp = df.select(*primary_keys,explode(field).alias(F"{field}"))

        elif type == 'structtype':

            sub_fields = [subfield.name for subfield in df.schema[field].dataType]

            final_pks = list(set(primary_keys) - set(sub_fields))

            df_exp = df.select(*final_pks,col(F"{field}.*"))

        elif type == 'arraytype_struct':

            sub_fields = [subfield.name for subfield in df.schema[field].dataType.elementType]

            final_pks = list(set(primary_keys) - set(sub_fields))

            df_exp = df.select(*final_pks,explode(field).alias(F"{field}"))

            df_exp = df_exp.select(*final_pks,col(F"{field}.*"))

        else:

            raise ValueError("invalid datatype. type should either of these arraytype,structtype,arraytype_struct")

        return df_exp,sub_fields
       
       
    def sql_comps(self,table_name1:str,table_name2:str,sql1:str,sql2:str):
        """
            Validate and execute SQL queries for two tables.

            Parameters:
                table_name1 (str): The name of the first table, including catalog and database name.
                table_name2 (str): The name of the second table, including catalog and database name.
                sql1 (str): The SQL query to be executed on the first table.
                sql2 (str): The SQL query to be executed on the second table.

            Returns:
                tuple: A tuple containing two DataFrames resulting from the execution of sql1 and sql2.

            Raises:
                ValueError: If the table names or SQL queries are invalid.
        """
        print(F"Generating DFs for SQL Comps")
        self.__validate_table_name(table_name1)
        self.__validate_table_name(table_name2)
        df1 = self.__validate_sql(sql1)
        df2 = self.__validate_sql(sql2)

        return df1,df2
    
    def table_comps(self,table_name1:str,
                         table_name2:str,
                         where_clause:str=None
                         ):
        
        """
            Compare two tables by reading them into DataFrames, optionally applying a where clause.

            Parameters:
                table_name1 (str): The name of the first table, including catalog and database name.
                table_name2 (str): The name of the second table, including catalog and database name.
                where_clause (str, optional): A SQL where clause to filter the tables. Defaults to None.

            Returns:
                tuple: A tuple containing two DataFrames read from the specified tables.

            Raises:
                ValueError: If the table names are invalid.
                TypeError: If there is an error reading the tables with the specified where clause.
        """
        print(F"Generating DF for tables")
        #validate input parameters

        self.__validate_table_name(table_name1)
        self.__validate_table_name(table_name2)
            
        # read old and new table

        if where_clause:
            try:
                df1 = self.spark.read.table(table_name1).filter(where_clause)
            except TypeError as e:
                    raise F"Could not create df for {table_name1} with where clause {where_clause}. Below is error {e}"
            try:
                df2 = self.spark.read.table(table_name2).filter(where_clause)
            except TypeError as e:
                    raise F"Could not create df for {table_name2} with where clause {where_clause}. Below is error {e}"
        else:
            try:
                #print(F"spark.read.table({table_name1})")
                df1 = self.spark.read.table(table_name1)
            except TypeError as e:
                    raise F"Could not create df for {table_name1}. Below is error {e}"
            try:
                #print(F"spark.read.table({table_name2})")
                df2 = self.spark.read.table(table_name2)
            except TypeError as e:
                    raise F"Could not create df for {table_name2}. Below is error {e}"
            
        return df1,df2 
    
    def compare_dataframes(self,df1: DataFrame, df2: DataFrame, primary_keys: list, fields_to_compare: list,table_name1,table_name2,max_recs:int=1000,max_fields:int=50):
            
            """
                Compare two DataFrames based on the given primary keys and fields to compare.

                Parameters:
                    df1 (DataFrame): The first DataFrame to compare.
                    df2 (DataFrame): The second DataFrame to compare.
                    primary_keys (list): A list of primary key columns to join the DataFrames.
                    fields_to_compare (list): A list of fields to compare between the DataFrames.
                    table_name1 (str): The name of the first table.
                    table_name2 (str): The name of the second table.
                    max_recs (int, optional): The maximum number of records to compare. Defaults to 1000.
                    max_fields (int, optional): The maximum number of fields to compare. Defaults to 50.

                Returns:
                    DataFrame: A DataFrame containing the results of the comparison.

                Raises:
                    ValueError: If 'primary_keys' is not a non-empty list.
                    ValueError: If 'fields_to_compare' is not a non-empty list or exceeds the maximum allowed fields.
            """
            print(F"Comparing dataframes at field level")
            compare_sts = 'p'
            check_type = 'compare'
            if not isinstance(primary_keys, list) or len(primary_keys) == 0:
                    raise ValueError("Invalid input: 'primary_keys' must be non-empty list.")
            if fields_to_compare:
                if not isinstance(fields_to_compare, list) or len(fields_to_compare) == 0 or len(fields_to_compare) > max_fields:
                    raise ValueError("Invalid input: 'fields_to_compare' must be non-empty [] and less than 50 fields.")
            
            
            df_final = self.spark.createDataFrame([], self.final_schema)
            # Ensure primary_keys and fields_to_compare are in both DataFrames
            df1 = df1.select(primary_keys + fields_to_compare).limit(max_recs)
            df2 = df2.select(primary_keys + fields_to_compare).limit(max_recs)
            
            # Perform an inner join on the primary keys
            joined_df = df1.alias("df1").join(df2.alias("df2"), primary_keys, "inner")


            # Compare each field
            for field in df1.columns:
                df1_field = col(f"df1.{field}")
                df2_field = col(f"df2.{field}")
                
                #print(F"{df1_field} and {df2_field}")
                # Filter records where the field values are different
                mismatches_df = joined_df.filter(df1_field != df2_field)
                
                # Calculate the count of mismatched records
                mismatches_count = mismatches_df.count()
                
                
                # If there are mismatches, extract the primary key values
                if mismatches_count > 0:
                    # Collect primary key values for records with mismatched fields
                    compare_sts = 'f'
                    mismatches_df = (mismatches_df.select(*primary_keys,col(f"df1.{field}").alias(F"tb1_{field}")
                                    ,col(f"df2.{field}").alias(F"tb2_{field}"))
                                    .withColumn("total_mismatches",lit(mismatches_count))
                                    )
                    mismatches_df = (mismatches_df.withColumn("Results", to_json(struct(*primary_keys,F"tb1_{field}",F"tb2_{field}","total_mismatches")))
                                                .withColumn("Table1", lit(table_name1))
                                                .withColumn("Table2", lit(table_name2)).select("Table1","Table2","Results")
                                                .withColumn("CheckType", lit(check_type))
                                                .withColumn("CheckStatus", lit(compare_sts))
                                    )

                else:
                    compare_sts = 'p'
                    mismatches_df = self.spark.sql(F"select '{field}' as field, 0 as mismatches_count")
                    mismatches_df = (mismatches_df.withColumn("Results", to_json(struct(col("field"),col("mismatches_count"))))
                                            .withColumn("Table1", lit(table_name1))
                                            .withColumn("Table2", lit(table_name2)).select("Table1","Table2","Results")
                                            .withColumn("CheckType", lit("compare"))
                                            .withColumn("CheckStatus", lit(compare_sts)))


                df_final = df_final.union(mismatches_df)
            
            return df_final
    
    def compare_record_count(self,df1: DataFrame, df2: DataFrame, primary_keys: list, fields_to_compare: list,table_name1,table_name2,max_recs:int=1000,max_fields:int=50):

        """
            Compare the record counts of two DataFrames and identify mismatches.

            Parameters:
                df1 (DataFrame): The first DataFrame to compare.
                df2 (DataFrame): The second DataFrame to compare.
                primary_keys (list): A list of primary key columns to join the DataFrames.
                fields_to_compare (list): A list of fields to compare between the DataFrames.
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.
                max_recs (int, optional): The maximum number of records to compare. Defaults to 1000.
                max_fields (int, optional): The maximum number of fields to compare. Defaults to 50.

            Returns:
                DataFrame: A DataFrame containing the results of the record count comparison.
        """
        print(F"Comparing record count")
        count_sts = 'p'
        check_type = 'count'
        count1 = df1.count()
        count2 = df2.count()

        if count1 != count2:
            count_sts = 'f'
            if count1 > count2:
                joined_df = df1.alias("df1").join(df2.alias("df2"), primary_keys, "anti").withColumn("Table",lit("tb1")).limit(max_recs)
            else:
                joined_df = df2.alias("df2").join(df1.alias("df1"), primary_keys, "anti").withColumn("Table",lit("tb2")).limit(max_recs)

            # Apply collect_list dynamically to each column
            agg_exprs = [collect_list(col(column)).alias(column) for column in primary_keys]
            df_aggregated = joined_df.groupBy("Table").agg(*agg_exprs)
            df_final = (df_aggregated.withColumn("Table1", lit(table_name1))
                                                .withColumn("Table2", lit(table_name2))
                                                .withColumn("tb1_cnt",lit(count1))
                                                .withColumn("tb2_cnt",lit(count2))
                                                .withColumn("CheckType", lit(check_type))
                                                .withColumn("CheckStatus", lit(count_sts))
                                                .select("Table1","Table2",to_json(struct("Table",*primary_keys,"tb1_cnt","tb2_cnt")).alias("Results"),"CheckType","CheckStatus")
                        )
            # print(F"Record count mismatch: {table_name1} has {count1} records, while {table_name2} has {count2} records.") 
        else:
            # print(F"Record count matches between {table_name1} and {table_name2}.")
            df_final_data = [(table_name1,table_name2,{"tb1_cnt":count1,"tb2_cnt":count2},"count",count_sts)]
            df_final = self.spark.createDataFrame(df_final_data, self.final_schema)
        return df_final

    def compare_data_completeness(self,df1: DataFrame, df2: DataFrame, primary_keys: list, fields_to_compare: list,table_name1,table_name2,max_recs:int=1000,max_fields:int=50):

        """
            Compare the data completeness (i.e., count of non-null records) of two DataFrames.

            Parameters:
                df1 (DataFrame): The first DataFrame to compare.
                df2 (DataFrame): The second DataFrame to compare.
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.

            Returns:
                DataFrame: A DataFrame containing the results of the data completeness comparison.
        """
        print(F"comparing for non null counts")

        completeness_sts = 'p'
        check_type = 'completeness'

        df1 = df1.select(primary_keys + fields_to_compare)
        df2 = df2.select(primary_keys + fields_to_compare)

        completeness1 = df1.dropna().count() 
        completeness2 = df2.dropna().count() 

        if completeness1 != completeness2:
            # print(F"Record count for non null mismatch: {table_name1} has {completeness1} records, while {table_name2} has {completeness2} records.")
            completeness_sts = 'f'
        # else:
        #     print(F"Record count for non null matches between {table_name1} and {table_name2}.")
        df_final_data = [(table_name1,table_name2,{"tb1":completeness1,"tb2":completeness2},check_type,completeness_sts)]
        df_final = self.spark.createDataFrame(df_final_data, self.final_schema)
        return df_final
 
        
    def compare_data_consistency(self,df1: DataFrame, df2: DataFrame, primary_keys: list, fields_to_compare: list,table_name1,table_name2,max_recs:int=1000,max_fields:int=50):

        """
            Compare the data consistency of specified fields between two DataFrames.

            Parameters:
                df1 (DataFrame): The first DataFrame to compare.
                df2 (DataFrame): The second DataFrame to compare.
                primary_keys (list): A list of primary key fields.
                fields_to_compare (list): A list of fields to compare.
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.
                max_recs (int, optional): Maximum number of records to consider. Defaults to 1000.
                max_fields (int, optional): Maximum number of fields to compare. Defaults to 50.

            Returns:
                DataFrame: A DataFrame containing the results of the data consistency comparison.
        """
        print(F"comparing field level for distinct counts")

        consistency_sts = 'p'
        check_type = 'consistency'
        
        df_final = self.spark.createDataFrame([], self.final_schema)

        if not isinstance(primary_keys, list) or len(primary_keys) == 0:
                raise ValueError("Invalid input: 'primary_keys' must be non-empty list.")
        if fields_to_compare:
            if not isinstance(fields_to_compare, list) or len(fields_to_compare) == 0 or len(fields_to_compare) > max_fields:
                raise ValueError("Invalid input: 'fields_to_compare' must be non-empty [] and less than 50 fields.")
            
        # Ensure primary_keys and fields_to_compare are in both DataFrames
        df1 = df1.select(primary_keys + fields_to_compare).limit(max_recs)
        df2 = df2.select(primary_keys + fields_to_compare).limit(max_recs)

        for field in df1.columns:

            unique_values1 = df1.select(field).distinct().count()
            unique_values2 = df2.select(field).distinct().count()

            if unique_values1 != unique_values2:
                #consistency_results.update({field:{table_name1:unique_values1,table_name2:unique_values2}})
                # print(f"Data consistency mismatch for field '{field}': {table_name1} has {unique_values1} unique values, while {table_name2} has {unique_values2} unique values.")
                consistency_sts = 'f'
            else:
                consistency_sts = 'p'
                # print(f"Data consistency for field '{field}' matches between {table_name1} and {table_name2}.")

            df_field_data = [(table_name1,table_name2,{F"tb1_{field}":unique_values1,F"tb2_{field}":unique_values2},check_type,consistency_sts)]
            df_field = self.spark.createDataFrame(df_field_data, self.final_schema)
            df_final = df_final.union(df_field)

        return df_final

    def compare_data_distribution(self,df1: DataFrame, df2: DataFrame, primary_keys: list, fields_to_compare: list,table_name1,table_name2,max_recs:int=1000,max_fields:int=50):

        """
            Compare the data distribution of specified fields between two DataFrames.

            Parameters:
                df1 (DataFrame): The first DataFrame to compare.
                df2 (DataFrame): The second DataFrame to compare.
                primary_keys (list): A list of primary key fields.
                fields_to_compare (list): A list of fields to compare.
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.
                max_recs (int, optional): Maximum number of records to consider. Defaults to 1000.
                max_fields (int, optional): Maximum number of fields to compare. Defaults to 50.

            Returns:
                DataFrame: A DataFrame containing the results of the data distribution comparison.
        """
        print(F"comparing field level for data distribution")
        dist_sts = 'p'
        check_type = 'distribution'
        
        df_final = self.spark.createDataFrame([], self.final_schema)
        if not isinstance(primary_keys, list) or len(primary_keys) == 0:
                raise ValueError("Invalid input: 'primary_keys' must be non-empty list.")
        if fields_to_compare:
            if not isinstance(fields_to_compare, list) or len(fields_to_compare) == 0 or len(fields_to_compare) > max_fields:
                raise ValueError("Invalid input: 'fields_to_compare' must be non-empty [] and less than 50 fields.")
            
        # Ensure primary_keys and fields_to_compare are in both DataFrames
        df1 = df1.select(primary_keys + fields_to_compare).limit(max_recs)
        df2 = df2.select(primary_keys + fields_to_compare).limit(max_recs)


        for field in df1.columns:
            distribution1 = df1.groupBy(field).count().withColumnRenamed("count","tb1_count")
            distribution2 = df2.groupBy(field).count().withColumnRenamed("count","tb2_count")
          # Perform an inner join on the primary keys
            joined_df = distribution1.join(distribution2,[field], "inner")
          # Filter records where the field values are different
            mismatches_df = joined_df.filter("tb1_count != tb2_count")
            #distribution_results.append(mismatches_df.toPandas().to_dict(orient='records'))
            if mismatches_df.count() > 0:
                dist_sts = 'f'
                mismatches_df = (mismatches_df.withColumn("Results", to_json(struct(field,"tb1_count","tb2_count")))
                                                .withColumn("Table1", lit(table_name1))
                                                .withColumn("Table2", lit(table_name2)).select("Table1","Table2","Results")
                                                .withColumn("CheckType", lit(check_type))
                                                .withColumn("CheckStatus", lit(dist_sts)))
            else:
                dist_sts = 'p'
                mismatches_df = self.spark.sql(f"SELECT '{field}' as field,0 as mismatches_count")
                mismatches_df = (mismatches_df.withColumn("Results", to_json(struct(col("field"),col("mismatches_count"))))
                                                .withColumn("Table1", lit(table_name1))
                                                .withColumn("Table2", lit(table_name2)).select("Table1","Table2","Results")
                                                .withColumn("CheckType", lit(check_type))
                                                .withColumn("CheckStatus", lit(dist_sts)))
                             
            df_final = df_final.union(mismatches_df)

        return df_final

    def compare_schemas_with_details(self,df1: DataFrame, df2: DataFrame,table_name1,table_name2):


        """
            Compare the schemas of two DataFrames and provide details about the differences.

            Parameters:
                df1 (DataFrame): The first DataFrame to compare.
                df2 (DataFrame): The second DataFrame to compare.
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.

            Returns:
                DataFrame: A DataFrame containing the results of the schema comparison with details.
        """
        print(F"comparing schemas")
        schema_sts = 'p'
        
        df_final = self.spark.createDataFrame([], self.final_schema)

        schema1 = df1.schema
        schema2 = df2.schema
        
        fields1 = set((field.name, field.dataType) for field in schema1)
        fields2 = set((field.name, field.dataType) for field in schema2)
        
        # Check for mismatched fields
        missing_in_df2 = fields1 - fields2
        missing_in_df1 = fields2 - fields1
        
        if not missing_in_df2 and not missing_in_df1:
            schema_results = {"match": True, "details": None}
        else:
            schema_results = {
                "match": False,
                F"missing_in_tb1": F"{list(missing_in_df1)}",
                F"missing_in_tb2": F"{list(missing_in_df2)}"
            }
            schema_sts = 'f'
        df_final_data = [(table_name1,table_name2,schema_results,"schema",schema_sts)]
        df_final = self.spark.createDataFrame(df_final_data, self.final_schema)
        return df_final
    
    def identify_checks(self,df:DataFrame,fields_to_compare:list):
      """
        
        
      """

      #check for complex datatypes  
      complex_column_types = self.identify_column_types(df)
      array_or_struct_cols = [item for sublist in complex_column_types.values() for item in sublist]
      #remove complex datatypes from fields to compare
      fields_to_compare_non_complex = list(set(fields_to_compare) - set(array_or_struct_cols))
      fields_to_compare_dict = {"field_type":["non_complex"],"fields_to_compare":fields_to_compare_non_complex}
      return(complex_column_types,fields_to_compare_dict)

    def compare_all(self,table_name1:str,
                    table_name2:str,
                    primary_keys:list,
                    fields_to_compare:list, 
                    where_clause:str=None,
                    sql1:str=None,
                    sql2:str=None):
      
      """
            Perform all comparisons between two tables including schema, data, record count, completeness, consistency, and distribution.

            Parameters:
                table_name1 (str): The name of the first table.
                table_name2 (str): The name of the second table.
                primary_keys (list): List of primary keys for comparison.
                fields_to_compare (list): List of fields to compare.
                where_clause (str, optional): SQL WHERE clause for filtering data. Default is None.
                sql1 (str, optional): SQL query for the first table. Default is None.
                sql2 (str, optional): SQL query for the second table. Default is None.

            Returns:
                DataFrame: A DataFrame containing the results of all comparisons.
      """
      
      if self.sql_comp.lower() == 'y':
        df1,df2 = self.sql_comps(table_name1,table_name2,sql1,sql2)
      else:
        df1,df2 = self.table_comps(table_name1,table_name2,where_clause)

      complex_comps,non_complex_comps =  self.identify_checks(df1,fields_to_compare)


      for i in non_complex_comps["field_type"]:
            fields_to_compare = non_complex_comps["fields_to_compare"]

            #perform comparison at field level
            comparison_results = self.compare_dataframes(df1,df2,primary_keys,fields_to_compare,table_name1,table_name2)  
            #compare record count
            count_results = self.compare_record_count(df1,df2,primary_keys,fields_to_compare,table_name1,table_name2)
            #compare count for non null records
            completeness_results = self.compare_data_completeness(df1,df2,primary_keys,fields_to_compare,table_name1,table_name2)
            #compare distinct counts at field level
            consistency_results = self.compare_data_consistency(df1,df2,primary_keys,fields_to_compare,table_name1,table_name2)
            #compare group by at field level
            distribution_results = self.compare_data_distribution(df1,df2,primary_keys,fields_to_compare,table_name1,table_name2)
            #compare schemas
            schema_results = self.compare_schemas_with_details(df1,df2,table_name1,table_name2)
            #union all results
            df_results = comparison_results.union(count_results).union(completeness_results).union(consistency_results).union(distribution_results).union(schema_results)

            #add audit fields
            df_results_all = (df_results.withColumn("UniqueCheckID",lit(str(uuid.uuid4())))
                                .withColumn("PrimaryKeys",lit(primary_keys))
                                .withColumn("FieldsToCompare",lit(fields_to_compare))
                                .withColumn("WhereClause",when(lit(where_clause) == None,lit("NULL")).otherwise(lit(where_clause)))
                                .withColumn("Sql1",when(lit(sql1) == None,lit("NULL")).otherwise(lit(sql1)))
                                .withColumn("Sql2",when(lit(sql2) == None,lit("NULL")).otherwise(lit(sql2)))
                                .withColumn("CheckTimeStamp",current_timestamp())
                                .withColumn("CurrentUser",current_user())
                                .select("UniqueCheckID","Table1","Table2","PrimaryKeys","FieldsToCompare","CheckType","CheckStatus","Results","WhereClause","Sql1","Sql2","CheckTimeStamp","CurrentUser")
                            )
            #append to audit table
            df_results_all.write.mode("append").saveAsTable(self.audit_table)
            #return df_results_all
      for dtype in complex_comps:
          for field in complex_comps[dtype]:
                df1_expand,fields_to_compare_comp = self.expand_complex_fields(df1,dtype,primary_keys,field)
                df2_expand,fields_to_compare_comp = self.expand_complex_fields(df2,dtype,primary_keys,field)
                #perform comparison at field level
                comparison_results_exp = self.compare_dataframes(df1_expand,df2_expand,primary_keys,fields_to_compare_comp,table_name1,table_name2)  
                #compare record count
                count_results_exp = self.compare_record_count(df1_expand,df2_expand,primary_keys,fields_to_compare_comp,table_name1,table_name2)
                #compare count for non null records
                completeness_results_exp = self.compare_data_completeness(df1_expand,df2_expand,primary_keys,fields_to_compare_comp,table_name1,table_name2)
                #compare distinct counts at field level
                consistency_results_exp = self.compare_data_consistency(df1_expand,df2_expand,primary_keys,fields_to_compare_comp,table_name1,table_name2)
                #compare group by at field level
                distribution_results_exp = self.compare_data_distribution(df1_expand,df2_expand,primary_keys,fields_to_compare_comp,table_name1,table_name2)
                #compare schemas
                schema_results_exp = self.compare_schemas_with_details(df1_expand,df2_expand,table_name1,table_name2)
                #union all results
                df_results_exp = comparison_results_exp.union(count_results_exp).union(completeness_results_exp).union(consistency_results_exp).union(distribution_results_exp).union(schema_results_exp)

                #add audit fields
                df_results_all_exp = (df_results_exp.withColumn("UniqueCheckID",lit(str(uuid.uuid4())))
                                    .withColumn("PrimaryKeys",lit(primary_keys))
                                    .withColumn("FieldsToCompare",lit(fields_to_compare))
                                    .withColumn("WhereClause",when(lit(where_clause) == None,lit("NULL")).otherwise(lit(where_clause)))
                                    .withColumn("Sql1",when(lit(sql1) == None,lit("NULL")).otherwise(lit(sql1)))
                                    .withColumn("Sql2",when(lit(sql2) == None,lit("NULL")).otherwise(lit(sql2)))
                                    .withColumn("CheckTimeStamp",current_timestamp())
                                    .withColumn("CurrentUser",current_user())
                                    .select("UniqueCheckID","Table1","Table2","PrimaryKeys","FieldsToCompare","CheckType","CheckStatus","Results","WhereClause","Sql1","Sql2","CheckTimeStamp","CurrentUser")
                                )
                #append to audit table
                df_results_all_exp.write.mode("append").saveAsTable(self.audit_table)
                #return df_results_all