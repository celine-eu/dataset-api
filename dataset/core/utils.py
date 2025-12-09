# dataset_api/core/utils.py

from dataset.core.config import settings


def url_str(url) -> str:
    return "" if url is None else str(url)


def get_dataset_uri(dataset_id: str) -> str:
    # dataset_base_uri is expected to be something like "https://example.org/dataset"
    base = str(settings.dataset_base_uri).rstrip("/")
    return f"{base}/{dataset_id}"
