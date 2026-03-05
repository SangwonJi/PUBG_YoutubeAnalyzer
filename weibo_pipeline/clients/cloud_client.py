"""
Cloud storage upload client (placeholder).
실제 Cloud API 스펙에 맞게 _do_upload()를 수정하세요.
"""
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import requests

import config

logger = logging.getLogger(__name__)


@dataclass
class UploadMetadata:
    filename: str
    content_type: str = "text/csv"
    description: str = ""


@dataclass
class UploadResult:
    success: bool
    url: str = ""
    error: str = ""


class CloudClient:
    """Cloud storage upload client."""

    def __init__(self):
        self.api_key = config.CLOUD_API_KEY
        self.upload_url = config.CLOUD_UPLOAD_URL

    def upload(self, file_path: Path, description: str = "") -> UploadResult:
        if not self.api_key or not self.upload_url:
            logger.warning("[Cloud] API key or URL not configured. Skipping upload.")
            return UploadResult(success=False, error="Not configured")

        metadata = UploadMetadata(
            filename=file_path.name,
            description=description,
        )
        return self._do_upload(file_path, metadata)

    def _do_upload(self, file_path: Path, metadata: UploadMetadata) -> UploadResult:
        """
        Placeholder: 실제 Cloud API에 맞게 수정하세요.
        """
        try:
            # Example implementation:
            # with open(file_path, "rb") as f:
            #     resp = requests.post(
            #         self.upload_url,
            #         headers={"Authorization": f"Bearer {self.api_key}"},
            #         files={"file": (metadata.filename, f, metadata.content_type)},
            #         data={"description": metadata.description},
            #     )
            #     resp.raise_for_status()
            #     return UploadResult(success=True, url=resp.json().get("url", ""))
            logger.info(f"[Cloud] Upload placeholder: {file_path.name}")
            return UploadResult(success=False, error="Placeholder - implement _do_upload()")
        except Exception as e:
            logger.error(f"[Cloud] Upload failed: {e}")
            return UploadResult(success=False, error=str(e))
