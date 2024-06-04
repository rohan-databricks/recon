# Recon Comp

### Overview

Recon is a Python class designed for comparing data between two tables in a Spark environment. It provides methods for performing various types of comparisons including schema, data, record count, completeness, consistency, and distribution.

### Installation

You can install Recon using pip:

pip install recon

### Usage

Import the Recon class:

from recon import Recon

Initialize a Recon object:


recon = Recon(spark, sql_comp, audit_table)

### Parameters:

spark: SparkSession object.
sql_comp: A string indicating whether SQL comparison is enabled ('y' or 'n').
audit_table: Name of the audit table to store comparison results.

### Call the comparison methods:


comparison_results = recon.compare_all(table_name1, table_name2, primary_keys, fields_to_compare, where_clause, sql1, sql2)

### Parameters:

table_name1(mandatory): Name of the first table.

table_name2(mandatory): Name of the second table.

primary_keys(mandatory): List of primary keys for comparison.

fields_to_compare(mandatory): List of fields to compare.

where_clause(recommeded when comparing tables and not providing sqls): SQL WHERE clause for filtering data.

sql1(Optional): SQL query for the first table.

sql2(Optional): SQL query for the second table.

### View results:

The comparison results are stored in an audit table specified during initialization.


### License
Open source

