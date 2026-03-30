from dataiku.llm.agent_tools import BaseAgentTool
import dataiku
import snowflake
from dataiku.snowpark import DkuSnowpark
from snowflake.core import Root
import json
import logging

logger = logging.getLogger("snowflakecortexsearchtoolgeneric")
logging.basicConfig(level=logging.INFO,
                    format='Snowflake tools plugin %(levelname)s - %(message)s')

class SnowflakeCortexSearchToolGeneric(BaseAgentTool):
    def set_config(self, config, plugin_config):
        self.config = config

    def get_descriptor(self, tool):
        # Get configured runtime filters from tool config
        runtime_filters_config = self.config.get("runtime_filters", [])
        
        # Descriptions configurable via tool.json
        tool_description = self.config.get("tool_description", "Searches a Snowflake Cortex Search service. Returns an array of results. For each result, returns the content of all columns selected.")
        query_description = self.config.get("query_description", "The Cortex Search query string")
        
        # Build base schema
        schema = {
            "$id": "https://dataiku.com/agents/tools/cortexsearch/input",
            "title": "Input for the cortex search tool",
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": query_description
                }
            },
            "required": ["query"]
        }
        
        # Add each configured runtime filter as a separate property
        for filter_config in runtime_filters_config:
            key = filter_config.get("key", "")
            column = filter_config.get("column", "")
            column_type = filter_config.get("columnType", "")
            description = filter_config.get("description", f"Filter on {column}")
            
            if not key:
                continue
                
            # Build properties for this filter type - operator + value fields from agent
            if column_type == "string":
                properties = {
                    "stringOp": {"type": "string", "enum": ["equals", "not_equals"], "description": "String operator"},
                    "values": {"type": "array", "items": {"type": "string"}, "description": "Values to match"}
                }
            elif column_type == "string_array":
                properties = {
                    "arrayOp": {"type": "string", "enum": ["contains", "not_contains"], "description": "Array operator"},
                    "logic": {"type": "string", "enum": ["or", "and"], "description": "Combine values with OR or AND"},
                    "values": {"type": "array", "items": {"type": "string"}, "description": "Values to match"}
                }
            elif column_type == "date":
                properties = {
                    "dateOp": {"type": "string", "enum": ["equals", "gte", "lte", "between"], "description": "Date operator"},
                    "date": {"type": "string", "description": "Date in YYYY-MM-DD format for equals, gte, lte"},
                    "startDate": {"type": "string", "description": "Start date (YYYY-MM-DD) for between"},
                    "endDate": {"type": "string", "description": "End date (YYYY-MM-DD) for between"}
                }
            elif column_type == "number":
                properties = {
                    "numOp": {"type": "string", "enum": ["equals", "gte", "lte", "between"], "description": "Number operator"},
                    "value": {"type": "number", "description": "Numeric value to compare for equals, gte, lte"},
                    "min": {"type": "number", "description": "Min value for between"},
                    "max": {"type": "number", "description": "Max value for between"}
                }
            else:
                continue
            
            # Set minimal required fields per type (operators required; values validated at runtime)
            if column_type == "string":
                required_fields = ["stringOp", "values"]
            elif column_type == "string_array":
                required_fields = ["arrayOp", "logic", "values"]
            elif column_type == "date":
                required_fields = ["dateOp"]
            elif column_type == "number":
                required_fields = ["numOp"]
            else:
                required_fields = []
            
            schema["properties"][key] = {
                "type": "object",
                "description": description,
                "properties": properties,
                "required": required_fields
            }
        
        return {
            "description": tool_description,            
            "inputSchema": schema
        }

    def invoke(self, input, trace):
        # Get tool input arguments, inlcuding the user query 'q'
        args = input["input"]
        query = args["query"]
        
        # Collect runtime filters from individual properties
        runtime_filters = []
        runtime_filters_config = self.config.get("runtime_filters", [])
        for filter_config in runtime_filters_config:
            key = filter_config.get("key", "")
            if key and key in args:
                # Merge the runtime values with the configured filter settings
                runtime_filter = {
                    "column": filter_config.get("column", ""),
                    "columnType": filter_config.get("columnType", "")
                }
                # Add the runtime operator/value fields
                runtime_filter.update(args[key])
                runtime_filters.append(runtime_filter)

        # Log inputs and config to trace
        trace.span["name"] = "SNOWFLAKE_CORTEX_SEARCH_TOOL_CALL"
        for key, value in args.items():
            trace.inputs[key] = value
        trace.attributes["config"] = self.config

        # Get the tool parameters, set at the Dataiku project, tool level
        snowflake_connection_name = self.config.get("snowflake_connection", None)
        cortex_search_database = self.config.get("cortex_search_database", None)
        cortex_search_schema = self.config.get("cortex_search_schema", None)
        cortex_search_service = self.config.get("cortex_search_service", None)
        max_documents = self.config.get("max_documents", None)
        search_column = self.config.get("search_column", None)
        metadata_columns = self.config.get("metadata_columns", None)
        filters_config = self.config.get("filters", [])

        # Combine all search columns to send to Cortex Search
        # Handle case where metadata_columns might be None
        if metadata_columns is None:
            all_search_columns = []
        else:
            all_search_columns = metadata_columns.copy() if isinstance(metadata_columns, list) else list(metadata_columns)
        all_search_columns.append(search_column)

        # Connect to Snowflake
        logger.info("Connecting to Snowflake")
        dku_snowpark = DkuSnowpark()
        snowpark_session = dku_snowpark.get_session(connection_name=snowflake_connection_name)
        root = Root(snowpark_session)

        # Get the Cortex Search service
        cortex_search_service = root.databases[cortex_search_database].schemas[cortex_search_schema].cortex_search_services[cortex_search_service]

        # Build typed filters dynamically from config and runtime filters
        filters = None
        combined_filter_items = []
        if filters_config:
            combined_filter_items.extend(filters_config)
        if runtime_filters:
            combined_filter_items.extend(runtime_filters)
        if combined_filter_items:
            filters = {"@and": []}
            for filter_item in combined_filter_items:
                column = filter_item.get("column")
                if not column:
                    continue

                column_type = filter_item.get("columnType")  # string | string_array | date | number

                # Backward compatibility: legacy behavior when columnType omitted
                if not column_type:
                    values = filter_item.get("values", [])
                    if isinstance(values, list) and len(values) > 1:
                        or_conditions = [{"@eq": {column: v}} for v in values]
                        filters["@and"].append({"@or": or_conditions})
                    elif isinstance(values, list) and len(values) == 1:
                        filters["@and"].append({"@eq": {column: values[0]}})
                    # If 0 values, treat as no-op
                    continue

                # Typed handling
                if column_type == "string":
                    string_op = filter_item.get("stringOp", "equals")  # equals | not_equals
                    values = filter_item.get("values", []) or []

                    # 0 values: no-op
                    if len(values) == 0:
                        continue

                    if string_op == "equals":
                        # Do not lowercase for exact on TEXT; service compares as-is
                        if len(values) == 1:
                            filters["@and"].append({"@eq": {column: values[0]}})
                        else:
                            # Build OR of equality when multiple values (since @in may not be supported)
                            or_conditions = [{"@eq": {column: v}} for v in values]
                            filters["@and"].append({"@or": or_conditions})
                    elif string_op == "not_equals":
                        # All provided values must be not equal using @not with @eq
                        if len(values) == 1:
                            filters["@and"].append({"@not": {"@eq": {column: values[0]}}})
                        elif len(values) > 1:
                            and_conditions = [{"@not": {"@eq": {column: v}}} for v in values]
                            filters["@and"].append({"@and": and_conditions})

                elif column_type == "string_array":
                    values = filter_item.get("values", []) or []
                    logic = filter_item.get("logic", "or")  # or | and
                    array_op = filter_item.get("arrayOp", "contains")  # contains | not_contains
                    if len(values) == 0:
                        continue
                    op_key = "@contains" if array_op == "contains" else "@not_contains"
                    term_exprs = [{op_key: {column: v}} for v in values]
                    if logic == "and":
                        filters["@and"].append({"@and": term_exprs})
                    else:
                        filters["@and"].append({"@or": term_exprs})

                elif column_type == "date":
                    date_op = filter_item.get("dateOp")  # equals | gte | lte | between
                    date_val = filter_item.get("date")
                    # Normalize incoming date strings to YYYY-MM-DD per Cortex filter syntax
                    if isinstance(date_val, str) and date_val:
                        if "T" in date_val:
                            date_val = date_val.split("T")[0]
                        if len(date_val) > 10:
                            date_val = date_val[:10]
                    if date_op == "between":
                        start_date = filter_item.get("startDate")
                        end_date = filter_item.get("endDate")
                        # Normalize both
                        if isinstance(start_date, str) and start_date:
                            if "T" in start_date:
                                start_date = start_date.split("T")[0]
                            if len(start_date) > 10:
                                start_date = start_date[:10]
                        if isinstance(end_date, str) and end_date:
                            if "T" in end_date:
                                end_date = end_date.split("T")[0]
                            if len(end_date) > 10:
                                end_date = end_date[:10]
                        if start_date and end_date:
                            filters["@and"].append({"@and": [
                                {"@gte": {column: start_date}},
                                {"@lte": {column: end_date}}
                            ]})
                        else:
                            logger.warning(f"Date 'between' filter missing bounds for column {column}; skipping")
                        continue
                    if not date_op or date_val in (None, ""):
                        continue
                    op_map = {
                        "equals": "@eq",
                        "gte": "@gte",
                        "lte": "@lte"
                    }
                    cortex_op = op_map.get(date_op)
                    if cortex_op:
                        filters["@and"].append({cortex_op: {column: date_val}})

                elif column_type == "number":
                    num_op = filter_item.get("numOp")  # equals | gte | lte | between
                    if num_op == "between":
                        min_v = filter_item.get("min")
                        max_v = filter_item.get("max")
                        if min_v is not None and max_v is not None:
                            filters["@and"].append({"@and": [
                                {"@gte": {column: min_v}},
                                {"@lte": {column: max_v}}
                            ]})
                        else:
                            logger.warning(f"Number 'between' filter missing min/max for column {column}; skipping")
                        continue
                    value = filter_item.get("value")
                    if num_op and value is not None:
                        op_map = {
                            "equals": "@eq",
                            "gte": "@gte",
                            "lte": "@lte"
                        }
                        cortex_op = op_map.get(num_op)
                        if cortex_op:
                            filters["@and"].append({cortex_op: {column: value}})

            logger.info(f"Applying filters: {filters}")

        # Search the Cortex Search service for the user query
        if filters:
            response = cortex_search_service.search(
                query=query,
                columns=all_search_columns,
                filter=filters,
                limit=max_documents
            )
        else:
            response = cortex_search_service.search(query, all_search_columns, limit=max_documents)
        logger.info(f"Cortex Search response: {response.json()}")

        # Get the results
        results = json.loads(response.json())['results']

        # Check if results are empty
        if not results or len(results) == 0:
            logger.info(f"No results found for query: {query}")
            # Log outputs to trace
            trace.outputs["output"] = []
            # Return "no results found" payload
            # Convert output to JSON string as framework expects string, not array
            return {
                "output": json.dumps([]),
                "sources": [{
                    "toolCallDescription": f"Performed Snowflake Cortex Search using service: {cortex_search_service} for query: {query}. No results found.",
                    "items": []
                }]
            }

        # Load the results into source items that will citable by Dataiku Agent Connect
        source_items = []
        for result in results:
            title_list = []
            # Handle case where metadata_columns might be None
            if metadata_columns:
                for metadata_column in metadata_columns:
                    title_list.append(result[metadata_column])
            title = ", ".join(title_list) if title_list else ""
            source_item = {
                "type": "SIMPLE_DOCUMENT",
                "title": title,
                "textSnippet": result[search_column]
            }
            source_items.append(source_item)

        # Log outputs to trace
        trace.outputs["output"] = results

        # Return the Cortex Search service response
        # Convert output to JSON string as framework expects string, not array
        return {
            "output": json.dumps(results),
            "sources": [{
                "toolCallDescription": f"Performed Snowflake Cortex Search using service: {cortex_search_service} for query: {query}",
                "items" : source_items
            }]
        }