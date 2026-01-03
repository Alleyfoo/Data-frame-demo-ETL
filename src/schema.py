"""Data validation schema using pandera.

This module defines contract-based validation for the provider data
pipeline, ensuring outgoing data matches the agreed structure.
"""

from __future__ import annotations

import pandera as pa

# Define the schema using the standard API (works on older and newer versions)
OutputSchema = pa.DataFrameSchema(
    {
        "provider_id": pa.Column(str, coerce=True, nullable=True, required=False),
        "article_sku": pa.Column(str, coerce=True, nullable=True, required=False),
        "report_date": pa.Column(
            pa.DateTime, coerce=True, nullable=True, required=False
        ),
        "sales_amount": pa.Column(float, coerce=True, nullable=True, required=False),
    },
    strict=False,
)  # strict=False allows extra columns

# Note: In this style, 'OutputSchema' is an object, not a class.
# The usage in pipeline.py remains the same: OutputSchema.validate(df)
