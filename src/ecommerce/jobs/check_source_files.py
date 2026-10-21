"""Task 1 - check_source_files (no Spark).

Verifies that all four source files for the batch date exist, are readable
and contain the contracted columns. Writes a manifest (row counts + md5) so
it is visible later which exact files were processed.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from ecommerce.config import Settings
from ecommerce.schemas import SOURCES

log = logging.getLogger(__name__)


class SourceFileError(RuntimeError):
    pass


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_source_files(settings: Settings, batch_date: str) -> Dict:
    landing = Path(settings.landing_dir(batch_date))
    errors: List[str] = []
    files: Dict[str, Dict] = {}

    for spec in SOURCES.values():
        path = landing / spec.file_name
        if not path.is_file():
            errors.append(f"{spec.file_name}: missing ({path})")
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = [h.strip() for h in next(reader, [])]
            rows = sum(1 for _ in reader)
        missing = [c for c in spec.columns if c not in header]
        extra = [c for c in header if c not in spec.columns]
        if not header:
            errors.append(f"{spec.file_name}: empty file (no header)")
        elif missing:
            errors.append(f"{spec.file_name}: missing columns {missing}")
        if extra:
            log.warning("%s: unexpected columns %s will be ignored", spec.file_name, extra)
        files[spec.name] = {
            "file": str(path),
            "rows": rows,
            "md5": _md5(path),
            "extra_columns": extra,
        }

    if errors:
        raise SourceFileError(f"Source check failed for {batch_date}: " + "; ".join(errors))

    manifest = {
        "batch_date": batch_date,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
    }
    if settings.is_local_lake:
        out = Path(settings.ops("manifests").replace("file://", "")) / f"{batch_date}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(manifest, indent=2))
    log.info("Source files OK for %s: %s", batch_date, {k: v["rows"] for k, v in files.items()})
    return manifest


def run(settings: Settings, batch_date: str) -> Dict:
    return check_source_files(settings, batch_date)


if __name__ == "__main__":
    from ecommerce.jobs.common import main

    main("check_source_files", run, needs_spark=False)
