"""Reportes sobre datos ya persistidos en SQLite."""

from tasa_estadistica.report.ic_tasa_report import query_ic_tasa_rows, write_ic_tasa_csv

__all__ = ["query_ic_tasa_rows", "write_ic_tasa_csv"]
