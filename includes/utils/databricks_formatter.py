from typing import Any, Dict


def format_query_results(result: Dict[str, Any]) -> str:
    """Format query results into a readable string."""

    # Check if result is empty or doesn't have the expected structure
    if not result or 'manifest' not in result or 'result' not in result:
        return "No results or invalid result format."
    
    # Extract column names from the manifest
    column_names = []
    if 'manifest' in result and 'schema' in result['manifest'] and 'columns' in result['manifest']['schema']:
        columns = result['manifest']['schema']['columns']
        column_names = [col['name'] for col in columns] if columns else []
    
    # If no column names were found, return early
    if not column_names:
        return "No columns found in the result."
    
    # Extract rows from the result
    rows = []
    if 'result' in result and 'data_array' in result['result']:
        rows = result['result']['data_array']
    
    # If no rows were found, return just the column headers
    if not rows:
        # Format as a table
        output = []
        
        # Add header
        output.append(" | ".join(column_names))
        output.append("-" * (sum(len(name) + 3 for name in column_names) - 1))
        output.append("No data rows found.")
        
        return "\n".join(output)
    
    # Format as a table
    output = []
    
    # Add header
    output.append(" | ".join(column_names))
    output.append("-" * (sum(len(name) + 3 for name in column_names) - 1))
    
    # Add rows
    for row in rows:
        row_values = []
        for value in row:
            if value is None:
                row_values.append("NULL")
            else:
                row_values.append(str(value))
        output.append(" | ".join(row_values))
    
    return "\n".join(output) 
