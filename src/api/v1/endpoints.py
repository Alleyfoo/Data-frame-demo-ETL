"""Pydantic schemas for headless API requests/responses."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ValidationConfig(BaseModel):
    """Validation settings for engine execution."""

    level: str = "coerce"


class ValidationResponse(BaseModel):
    """Validation result payload."""

    is_valid: bool
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0


class ProcessResult(BaseModel):
    """High-level processing outcome."""

    success: bool
    message: str
    output_path: Optional[str] = None
    row_count: int = 0
    metrics: Dict[str, Any] = Field(default_factory=dict)


class IngestRequest(BaseModel):
    """Ingest request for a headless run."""

    template: Dict[str, Any] = Field(default_factory=dict)
    rows: List[Dict[str, Any]] = Field(default_factory=list)


class TransformRequest(BaseModel):
    """Transform request payload."""

    template: Dict[str, Any] = Field(default_factory=dict)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    validation_level: str = "coerce"


class ErrorResponse(BaseModel):
    """Standardized error response."""

    error: str
    details: Optional[str] = None
