# ae-snowflake-search

## Purpose
The `ae-snowflake-search` is a Python agent tool designed to facilitate efficient and effective searching within Snowflake data warehouses. It aims to enhance user interaction with Snowflake's powerful data querying capabilities by providing an easy-to-use interface and robust parameter management.

## Parameters
- **query**: The SQL query string that defines the search criteria. This query is executed against the Snowflake database.
- **database**: The name of the database to connect to for executing the query.
- **schema**: The schema within the database that contains the relevant tables for the query.
- **params**: A dictionary of parameters to bind to the SQL query, ensuring safe and efficient execution of dynamic queries.
- **max_results**: Optional integer to define the maximum number of results to retrieve from the query execution.

## Installation
```bash
pip install ae-snowflake-search
```

## Usage
```python
from ae_snowflake_search import SnowflakeSearch

# Instantiate with your Snowflake configuration
search = SnowflakeSearch(database='my_database', schema='my_schema')

# Execute a query
results = search.execute(query='SELECT * FROM my_table WHERE condition', params={'condition': 'value'}, max_results=100)
```

## License
This project is licensed under the MIT License.