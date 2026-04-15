"""Supplier / archive rules for paths we intentionally skip in ETL."""


def skip_logistics_supplier_id(supplier_id: str) -> bool:
    """True when ``supplier_id`` contains ``_logistics`` (case-insensitive), e.g. ``0010_logistics``."""
    if not supplier_id:
        return False
    return "_logistics" in supplier_id.lower()


def skip_logistics_archive_basename(name: str) -> bool:
    """Same rule for ``*.7z`` basenames under DATA_DIR or SFTP."""
    return skip_logistics_supplier_id(name)
