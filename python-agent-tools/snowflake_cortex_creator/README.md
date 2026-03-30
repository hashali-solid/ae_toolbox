# Snowflake Cortex Creator

The Snowflake Cortex Creator is a Python agent tool designed to facilitate seamless integration with Snowflake. This tool helps users automate the process of managing and querying Snowflake data warehouses, making it easier to leverage Snowflake's powerful analytics capabilities.

## Purpose

The main purpose of the Snowflake Cortex Creator is to streamline the workflow of data operations in Snowflake. This includes the creation of database objects, executing SQL queries, and managing connections to Snowflake data warehouses.

## Parameters

- `user`: The username for connecting to the Snowflake database.
- `password`: The password associated with the Snowflake user account.
- `account`: The Snowflake account identifier (e.g., `myaccount.snowflakecomputing.com`).
- `warehouse`: The name of the Snowflake warehouse to use for executing queries.
- `database`: The name of the Snowflake database to connect to.
- `schema`: The schema within the database to be used for operations.

## Installation

To install this tool, you can use pip:

```bash
pip install snowflake-cortex-creator
```

To get started, you will also need to set up a Snowflake account and configure the connection parameters as described above.

## Usage

You can use the Snowflake Cortex Creator to execute SQL commands and interact with your Snowflake instance. Here is a basic usage example:

```python
from snowflake_cortex_creator import SnowflakeCortex

# Create an instance of the Snowflake Cortex
cortex = SnowflakeCortex(user='your_username', password='your_password', account='your_account', warehouse='your_warehouse', database='your_database', schema='your_schema')

# Execute a SQL query
cortex.execute_query("SELECT * FROM my_table;")
```

For more detailed documentation and examples, please refer to the official documentation or the project repository.