# Query Builder Guide

The Query Builder page provides a menu-driven way to build SQL-like filters
without typing raw SQL. It operates on the preview data you have already
uploaded in the Streamlit app.

## Quick Start
1) Go to Upload and load a CSV or Excel file.
2) Open Query Builder in the sidebar.
3) Use the Source Canvas to select columns.
4) Add filters in the filter table and preview the result.
5) Copy the generated SQL from the Query Canvas.

## Source Canvas
The Source Canvas lists available columns. Selecting a row adds that column
to the query's SELECT list.

## Filters
Use the filter table to add conditions. Supported operators are:
`=`, `!=`, `>`, `>=`, `<`, `<=`, `contains`.

## Query Canvas
The SQL preview is generated automatically when "Auto-sync SQL" is enabled.
You can still edit the text manually if needed.

## Preview
Click "Apply Filters Preview" to see the filtered data inside Streamlit.
