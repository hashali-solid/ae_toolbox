"""
- Configurable schema placement (output_path support)
- Handles quoted/lowercase source columns via aliasing
- Intelligent CREATE/ALTER logic to avoid unnecessary recreations
- Default TARGET_LAG = '24 hours'
- Logging of all operations
"""
from dataiku.customrecipe import (
    get_input_names_for_role,
    get_output_names_for_role,
    get_recipe_config,
)
import dataiku
from dataiku.core.sql import SQLExecutor2
import pandas as pd
import re
import logging


def quote_ident(name: str) -> str:
    """Double-quote an identifier unless already quoted."""
    if name.startswith('"') and name.endswith('"'):
        return name
    return f'"{name}"'


def sanitize_alias(col: str) -> str:
    """
    Make a Snowflake-safe *unquoted* alias from any column label:
    - uppercase
    - replace non [A-Z0-9_] with _
    - prefix with A_ if it would start with a digit
    """
    alias = re.sub(r'[^A-Za-z0-9_]', '_', col).upper()
    if re.match(r'^[0-9]', alias):
        alias = "A_" + alias
    if alias == "":
        alias = "COL_1"
    return alias


def table_fqn(dku_dataset: dataiku.Dataset) -> str:
    """Return fully-qualified quoted table name e.g. "DB"."SCHEMA"."TABLE"."""
    loc = dku_dataset.get_location_info().get("info", {})
    return loc.get("quotedResolvedTableName") or loc.get("table") or dku_dataset.name


def quote_path_regex(path: str) -> str:
    """
    Split a dot-separated path (e.g. PRS.ENG_ML) into parts and quote each for Snowflake.
    Handles extra spaces and missing quotes.
    """
    parts = re.findall(r'\w+', path)
    return '.'.join([f'"{p}"' for p in parts])


def service_exists(exec_: SQLExecutor2, service_fqn: str) -> bool:
    """
    Check existence by attempting to DESCRIBE the service.
    If it exists, DESCRIBE works; if not, it throws.
    This avoids depending on SHOW column names.
    """
    try:
        exec_.query_to_df(f"DESCRIBE CORTEX SEARCH SERVICE {service_fqn}")
        return True
    except Exception:
        return False


def main() -> None:
    cfg = get_recipe_config()
    raw_service_name: str = cfg["service_name"]
    on_column: str = cfg["on_column"]
    array_attribute_columns = cfg.get("array_attribute_columns", [])
    non_array_attribute_columns = cfg.get("non_array_attribute_columns", [])
    target_lag: str = cfg.get("target_lag", "24 hours")
    embedding_model: str = cfg["embedding_model"]
    output_path = str(cfg.get("output_path", "").strip())

    # Combine both attribute column lists
    all_attribute_columns = list(array_attribute_columns) + list(non_array_attribute_columns)
    array_attribute_columns_set = set(array_attribute_columns)

    if not all_attribute_columns:
        raise ValueError("Select at least one attribute column (array or non-array).")

    # Log column selections for debugging
    logging.info(f"Selected array_attribute_columns: {array_attribute_columns}")
    logging.info(f"Selected non_array_attribute_columns: {non_array_attribute_columns}")
    logging.info(f"Combined all_attribute_columns: {all_attribute_columns}")
    print(f"Selected array_attribute_columns: {array_attribute_columns}")
    print(f"Selected non_array_attribute_columns: {non_array_attribute_columns}")
    print(f"Combined all_attribute_columns: {all_attribute_columns}")

    # Datasets
    in_name = get_input_names_for_role("input_dataset")[0]
    log_name = get_output_names_for_role("log_dataset")[0]
    in_ds = dataiku.Dataset(in_name)
    out_ds = dataiku.Dataset(log_name)
    source_table = table_fqn(in_ds)

    # Build alias mapping (quoted source -> unquoted alias)
    search_src = quote_ident(on_column)
    search_alias = sanitize_alias(on_column)

    attr_src_cols = [quote_ident(c) for c in all_attribute_columns]
    attr_aliases = [sanitize_alias(c) for c in all_attribute_columns]

    # Determine service FQN: use output_path if provided, else default to DATAIKU.LLMS
    if output_path and output_path != '':
        service_fqn = f'{quote_path_regex(output_path)}.' + quote_ident(raw_service_name)
    else:
        service_fqn = '"DATAIKU"."LLMS".' + quote_ident(raw_service_name)

    # SQL fragments - ON/ATTRIBUTES must use the *unquoted* aliases from the subquery
    on_sql = search_alias
    attrs_sql = ", ".join(attr_aliases)

    # SELECT list quotes physical columns and aliases them to the simple names
    # For array columns, use AS_ARRAY(TRY_PARSE_JSON(...)) transformation
    select_cols = [f"{search_src} AS {search_alias}"]
    for col, src, alias in zip(all_attribute_columns, attr_src_cols, attr_aliases):
        if col in array_attribute_columns_set:
            select_cols.append(f'AS_ARRAY(TRY_PARSE_JSON({src})) AS {alias}')
            logging.info(f"Column '{col}' marked as array - using AS_ARRAY(TRY_PARSE_JSON(...))")
            print(f"Column '{col}' marked as array - using AS_ARRAY(TRY_PARSE_JSON(...))")
        else:
            select_cols.append(f"{src} AS {alias}")
            logging.info(f"Column '{col}' is regular attribute")
            print(f"Column '{col}' is regular attribute")
    select_sql = ", ".join(select_cols)

    # Use CREATE OR REPLACE for both new and existing services
    # This ensures all properties are updated: attributes, source table, embedding model, etc.
    create_or_replace_sql = f"""
    CREATE OR REPLACE CORTEX SEARCH SERVICE {service_fqn}
      ON {on_sql}
      ATTRIBUTES {attrs_sql}
      WAREHOUSE = MLW_WH
      TARGET_LAG = '{target_lag}'
      EMBEDDING_MODEL = '{embedding_model}'
    AS (
        SELECT {select_sql}
        FROM {source_table}
    );
    """.strip()

    # Execute with CREATE OR REPLACE (handles both new and existing services)
    exec_ = SQLExecutor2(dataset=in_ds)
    
    # Check if service exists before execution (for logging purposes)
    service_already_exists = service_exists(exec_, service_fqn)
    
    status = None
    executed_sql = None
    response_message = None

    try:
        executed_sql = create_or_replace_sql
        if service_already_exists:
            logging.info(f"Service exists - executing CREATE OR REPLACE SQL: {executed_sql}")
            print(f"Service exists - executing CREATE OR REPLACE SQL: {executed_sql}")
        else:
            logging.info(f"Service does not exist - executing CREATE OR REPLACE SQL: {executed_sql}")
            print(f"Service does not exist - executing CREATE OR REPLACE SQL: {executed_sql}")
        
        result_df = exec_.query_to_df(create_or_replace_sql)
        
        if service_already_exists:
            status = "REPLACED"
            response_message = f"CREATE OR REPLACE successful (updated existing service). Result rows: {len(result_df)}"
        else:
            status = "CREATED"
            response_message = f"CREATE OR REPLACE successful (created new service). Result rows: {len(result_df)}"
        
        logging.info(response_message)
        print(response_message)
    except Exception as e:
        status = f"FAILED: {str(e)}"
        executed_sql = create_or_replace_sql
        response_message = f"Error: {str(e)}"
        logging.error(f"SQL execution failed: {response_message}")
        logging.error(f"Failed SQL: {executed_sql}")
        print(f"SQL execution failed: {response_message}")
        print(f"Failed SQL: {executed_sql}")

    # Log operation details
    log_df = pd.DataFrame(
        [{
            "service_fqn": service_fqn.replace('"', ''),
            "source_table": source_table.replace('"', ''),
            "on_column_alias": search_alias,
            "attribute_aliases": ",".join(attr_aliases),
            "array_attribute_columns": ",".join(sorted(array_attribute_columns)) if array_attribute_columns else "",
            "non_array_attribute_columns": ",".join(sorted(non_array_attribute_columns)) if non_array_attribute_columns else "",
            "target_lag": target_lag,
            "embedding_model": embedding_model,
            "created_ts": pd.Timestamp.utcnow(),
            "status": status,
            "sql_executed": executed_sql.replace("\n", " ") if executed_sql else "",
            "response_message": response_message or "",
        }]
    )
    out_ds.write_with_schema(log_df)


if __name__ == "__main__":
    main()
