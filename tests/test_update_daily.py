import sys
from pathlib import Path

import pytest
from huggingface_hub.utils import EntryNotFoundError
try:
    from huggingface_hub.errors import LocalEntryNotFoundError
except ImportError:
    from huggingface_hub.utils import LocalEntryNotFoundError
try:
    from huggingface_hub.errors import RemoteEntryNotFoundError as RealNotFound
except ImportError:  # huggingface_hub 0.x: the real 404 is EntryNotFoundError itself
    RealNotFound = EntryNotFoundError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import update_daily


def _raiser(exc):
    def fake(*a, **k):
        raise exc
    return fake


def _make(cls):
    # Constructors differ across huggingface_hub versions; the message is enough.
    if cls is LocalEntryNotFoundError or RealNotFound is EntryNotFoundError:
        return cls("simulated")
    import httpx  # v1.x RemoteEntryNotFoundError wants an HTTP response
    return cls("simulated", response=httpx.Response(404, request=httpx.Request("GET", "https://huggingface.co/x")))


def test_transient_failure_propagates(monkeypatch):
    monkeypatch.setattr(update_daily, "hf_hub_download",
                        _raiser(_make(LocalEntryNotFoundError)))
    with pytest.raises(LocalEntryNotFoundError):
        update_daily.load_existing_month("r/d", "data/2026_01.parquet", None)


def test_real_not_found_is_first_run(monkeypatch):
    monkeypatch.setattr(update_daily, "hf_hub_download",
                        _raiser(_make(RealNotFound)))
    df = update_daily.load_existing_month("r/d", "data/2026_01.parquet", None)
    assert len(df) == 0
    assert list(df.columns) == ["usajobsControlNumber", "text"]
