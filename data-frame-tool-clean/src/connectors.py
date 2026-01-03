"""Connector utilities for external sources (SQL first)."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import os
import pandas as pd
import yaml

try:
    import sqlalchemy
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - optional dependency
    sqlalchemy = None
    create_engine = None
    text = None

CONNECTIONS_PATH = Path("connections.yaml")


@dataclass
class ConnectionConfig:
    name: str
    type: str = "sql"  # e.g., sql, azure, s3
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None  # e.g., postgresql+psycopg2, mssql+pyodbc
    extras: dict = None
    # For future extensions (e.g., SSL, params)

    def to_dict(self) -> dict:
        payload = asdict(self)
        # Avoid writing None values
        return {k: v for k, v in payload.items() if v not in (None, "", {}, [])}

    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionConfig":
        return cls(
            name=str(data.get("name") or data.get("Name")),
            type=str(data.get("type", data.get("Type", "sql"))),
            host=data.get("host"),
            port=int(data["port"]) if "port" in data and data["port"] is not None else None,
            database=data.get("database") or data.get("db"),
            user=data.get("user"),
            password=data.get("password"),
            driver=data.get("driver"),
            extras=data.get("extras") or {},
        )


def load_connections(path: Path | None = None) -> List[ConnectionConfig]:
    target = path or CONNECTIONS_PATH
    if not target.exists():
        return []
    data = yaml.safe_load(target.read_text(encoding="utf-8")) or []
    if isinstance(data, dict):
        # legacy or single-entry form
        data = [data]
    conns: List[ConnectionConfig] = []
    for item in data:
        if isinstance(item, dict):
            try:
                conns.append(ConnectionConfig.from_dict(item))
            except Exception:
                continue
    return conns


def save_connections(conns: List[ConnectionConfig], path: Path | None = None) -> None:
    target = path or CONNECTIONS_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = [c.to_dict() for c in conns]
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def check_sqlalchemy_available() -> None:
    if sqlalchemy is None or create_engine is None or text is None:
        raise RuntimeError(
            "SQLAlchemy is required for SQL connections. Install with:\n"
            "  pip install sqlalchemy psycopg2-binary  # Postgres\n"
            "  pip install sqlalchemy pyodbc           # SQL Server (with ODBC driver)\n"
            "For SQL Server on Windows, also install ODBC Driver 18: https://aka.ms/msodbcsql\n"
        )


def _sqlalchemy_url(conn: ConnectionConfig) -> str:
    # Choose driver; default to Postgres
    driver = conn.driver or "postgresql+psycopg2"
    host = conn.host or "localhost"
    port = f":{conn.port}" if conn.port else ""
    db = f"/{conn.database}" if conn.database else ""
    user = conn.user or ""
    pwd = conn.password or os.getenv(f"{conn.name.upper()}_PASSWORD", "")
    auth = f"{user}:{pwd}@" if user or pwd else ""
    return f"{driver}://{auth}{host}{port}{db}"


def fetch_sql_preview(
    conn: ConnectionConfig, table: str | None = None, query: str | None = None, limit: int = 50
) -> pd.DataFrame:
    check_sqlalchemy_available()
    engine = create_engine(_sqlalchemy_url(conn))
    limit_clause = f" LIMIT {limit}" if limit and limit > 0 else ""
    if query:
        low = query.lower()
        if "limit " in low or "fetch" in low:
            sql_text = query
        else:
            sql_text = f"{query}\n{limit_clause}"
    elif table:
        sql_text = f"SELECT * FROM {table}{limit_clause}"
    else:
        raise ValueError("Provide either a table or a query for SQL preview.")
    with engine.connect() as connection:
        return pd.read_sql(text(sql_text), con=connection)


def read_sql_with_template(
    template, connections_path: Path | None = None, limit: Optional[int] = None
) -> pd.DataFrame:
    conns = load_connections(connections_path)
    match = next((c for c in conns if c.name == template.connection_name), None)
    if not match:
        raise ValueError(f"Connection '{template.connection_name}' not found.")
    check_sqlalchemy_available()
    engine = create_engine(_sqlalchemy_url(match))
    if template.sql_query:
        sql_text = template.sql_query
    elif template.sql_table:
        sql_text = f"SELECT * FROM {template.sql_table}"
    else:
        raise ValueError("Template missing sql_table or sql_query for SQL source.")
    if limit:
        sql_text = f"{sql_text}\nLIMIT {int(limit)}"
    with engine.connect() as connection:
        return pd.read_sql(text(sql_text), con=connection)


def test_connection(conn: ConnectionConfig) -> str:
    """Return a short success message or raise with details."""
    check_sqlalchemy_available()
    engine = create_engine(_sqlalchemy_url(conn))
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return f"Connection '{conn.name}' OK"
