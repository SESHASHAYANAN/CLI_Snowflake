"""
DAX to SQL Transpiler.

Converts basic DAX expressions to Snowflake-compatible SQL.
"""

from __future__ import annotations


import re
from typing import Optional

class DaxToSqlTranspiler:
    """
    Simple rule-based DAX to SQL converter.
    
    This is not a full compiler but covers common patterns:
    - SUM(Table[Col]) -> SUM(Col) FROM Table
    - COUNT(Table[Col]) -> COUNT(Col) FROM Table
    - AVERAGE(Table[Col]) -> AVG(Col) FROM Table
    - DISTINCTCOUNT(Table[Col]) -> COUNT(DISTINCT Col) FROM Table
    - Simple arithmetic: Measure1 - Measure2
    """
    
    def __init__(self, table_mapping: dict = None):
        """
        Args:
           table_mapping: Optional map of logical table names to SQL table names
        """
        self.table_mapping = table_mapping or {}

    def transpile(self, dax_expression: str, source_table: Optional[str] = None) -> str:
        """
        Convert DAX expression to SQL SELECT statement.
        
        Args:
            dax_expression: The DAX formula string
            source_table: The context table (if any)
            
        Returns:
            SQL string or None if conversion not possible/supported
        """
        if not dax_expression:
            return None
            
        sql = dax_expression.strip()
        
        # 1. Handle basic aggregations
        # SUM('Table'[Column]) or SUM(Table[Column])
        # Regex to capture: Func(Table[Column]) or Func('Table'[Column])
        
        # SUM
        sql = self._replace_agg(sql, r"\bSUM\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "SUM", source_table)
        
        # AVERAGE -> AVG
        sql = self._replace_agg(sql, r"\bAVERAGE\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "AVG", source_table)
        
        # COUNT -> COUNT
        # Note: Do DISTINCTCOUNT first to avoid partial match if not using boundaries strictly?
        # But we use \b now so COUNT won't match DISTINCTCOUNT
        
        # DISTINCTCOUNT -> COUNT(DISTINCT ...)
        sql = self._replace_agg(sql, r"\bDISTINCTCOUNT\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "COUNT(DISTINCT", source_table, close_paren=True)

        # COUNT -> COUNT (safe now)
        sql = self._replace_agg(sql, r"\bCOUNT\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "COUNT", source_table)
        
        # MIN/MAX
        sql = self._replace_agg(sql, r"\bMIN\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "MIN", source_table)
        sql = self._replace_agg(sql, r"\bMAX\s*\(\s*'?([^']*)'?\[([^\]]+)\]\s*\)", "MAX", source_table)

        # 2. Handle simple arithmetic between simple aggregations
        # E.g. [Sales Amount] - [Cost]
        # This is tricky because we don't assume we know other measures. 
        # But if the expression is just "SUM(T[C]) - SUM(T[D])", it should work if we formatted it correctly above.
        
        # If the result looks like a valid SQL SELECT fragment, wrap it.
        # Check if we have FROM clauses.
        # This simple transpiler assumes a single table context or compatible joins.
        # For this MVP, we try to detect the table from the first aggregation and use that as the FROM.
        
        tables_found = set(re.findall(r"FROM\s+\"([^\"]+)\"", sql))
        if not tables_found:
             # Try to find table usage in aggregations if _replace_agg didn't add FROM (it doesn't yet)
             pass
             
        return sql

    def _replace_agg(self, sql: str, pattern: str, sql_func: str, default_table: str, close_paren: bool = False) -> str:
        """
        Replaces DAX aggregation with SQL equivalent.
        Returns the SQL fragment (e.g. SUM("Col")) BUT handling FROM is complex.
        
        Strategy: 
        We turn `SUM(Table[Column])` into `(SELECT SUM("Column") FROM "Table")` 
        This acts as a scalar subquery which is valid in many contexts.
        """
        
        def replace(match):
            table = match.group(1) or default_table
            col = match.group(2)
            
            # Map table name if needed
            sql_table = self.table_mapping.get(table, table)
            
            # Construct scalar subquery
            # closing paren needed if we injected 'COUNT(DISTINCT' which opened one but didn't close it fully?
            # actually match pattern consumes closing ) of DAX.
            # sql_func might be "SUM" or "COUNT(DISTINCT"
            
            inner_col = f'"{col}"'
            
            # Handle closing paren for distinct count
            closing = ")" if close_paren else ")"
            
            # If sql_func has open paren e.g. "COUNT(DISTINCT", we just need to add `inner_col + ")"` 
            # WAIT: "COUNT(DISTINCT" takes one arg.
            
            if "DISTINCT" in sql_func:
                 return f'(SELECT COUNT(DISTINCT {inner_col}) FROM "{sql_table}")'
            else:
                 return f'(SELECT {sql_func}({inner_col}) FROM "{sql_table}")'

        return re.sub(pattern, replace, sql, flags=re.IGNORECASE)

