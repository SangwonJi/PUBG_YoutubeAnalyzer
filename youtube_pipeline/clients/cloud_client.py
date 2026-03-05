"""
Cloud Storage API Client.
Abstract interface for uploading files to cloud storage.
Designed for easy replacement when actual API spec is provided.
"""

import httpx
from pathlib import Path
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel

from config import get_config


class UploadMetadata(BaseModel):
    """Metadata for file upload."""
    filename: str
    content_type: str = "application/octet-stream"
    description: Optional[str] = None
    tags: list[str] = []
    uploaded_at: datetime = None
    
    def __init__(self, **data):
        if data.get("uploaded_at") is None:
            data["uploaded_at"] = datetime.now()
        super().__init__(**data)


class UploadResult(BaseModel):
    """Result of file upload."""
    success: bool
    message: str
    url: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[dict] = None


class CloudClient:
    """
    Cloud Storage API client.
    
    This is an abstract implementation that can be easily replaced
    when the actual Cloud API specification is provided.
    
    Current implementation:
    - Uses multipart/form-data upload to CLOUD_UPLOAD_URL
    - Includes API key in Authorization header
    - Graceful degradation on failure
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        upload_url: Optional[str] = None
    ):
        config = get_config()
        self.api_key = api_key or config.cloud.api_key
        self.upload_url = upload_url or config.cloud.upload_url
        self.timeout = 60.0  # seconds
        
        self._configured = bool(self.api_key and self.upload_url)
    
    @property
    def is_configured(self) -> bool:
        """Check if cloud client is properly configured."""
        return self._configured
    
    def upload_file(
        self,
        file_path: Path | str,
        metadata: Optional[UploadMetadata] = None
    ) -> UploadResult:
        """
        Upload a file to cloud storage.
        
        Args:
            file_path: Path to the file to upload
            metadata: Optional upload metadata
        
        Returns:
            UploadResult with success status and details
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            return UploadResult(
                success=False,
                message="File not found",
                error=f"File does not exist: {file_path}"
            )
        
        if not self._configured:
            return UploadResult(
                success=False,
                message="Cloud client not configured",
                error="Set CLOUD_API_KEY and CLOUD_UPLOAD_URL in .env"
            )
        
        # Prepare metadata
        if metadata is None:
            content_type = self._guess_content_type(file_path)
            metadata = UploadMetadata(
                filename=file_path.name,
                content_type=content_type
            )
        
        try:
            return self._do_upload(file_path, metadata)
        except Exception as e:
            return UploadResult(
                success=False,
                message="Upload failed",
                error=str(e)
            )
    
    def _do_upload(
        self,
        file_path: Path,
        metadata: UploadMetadata
    ) -> UploadResult:
        """
        Perform the actual upload.
        
        Override this method when actual API spec is provided.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Upload-Timestamp": metadata.uploaded_at.isoformat()
        }
        
        files = {
            "file": (
                metadata.filename,
                open(file_path, "rb"),
                metadata.content_type
            )
        }
        
        data = {
            "description": metadata.description or "",
            "tags": ",".join(metadata.tags) if metadata.tags else ""
        }
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                self.upload_url,
                headers=headers,
                files=files,
                data=data
            )
        
        if response.status_code in (200, 201):
            response_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            return UploadResult(
                success=True,
                message="Upload successful",
                url=response_data.get("url"),
                metadata=response_data
            )
        else:
            return UploadResult(
                success=False,
                message=f"Upload failed with status {response.status_code}",
                error=response.text
            )
    
    def _guess_content_type(self, file_path: Path) -> str:
        """Guess content type from file extension."""
        suffix = file_path.suffix.lower()
        content_types = {
            ".csv": "text/csv",
            ".json": "application/json",
            ".db": "application/x-sqlite3",
            ".sqlite": "application/x-sqlite3",
            ".sqlite3": "application/x-sqlite3",
            ".txt": "text/plain",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        return content_types.get(suffix, "application/octet-stream")
    
    def upload_csv(
        self,
        file_path: Path | str,
        description: Optional[str] = None,
        tags: Optional[list[str]] = None
    ) -> UploadResult:
        """
        Convenience method for uploading CSV files.
        
        Args:
            file_path: Path to CSV file
            description: Optional description
            tags: Optional tags
        
        Returns:
            UploadResult
        """
        file_path = Path(file_path)
        metadata = UploadMetadata(
            filename=file_path.name,
            content_type="text/csv",
            description=description or f"PUBG Collab Pipeline Report - {datetime.now().strftime('%Y-%m-%d')}",
            tags=tags or ["pubg", "collab", "report"]
        )
        return self.upload_file(file_path, metadata)
    
    def upload_database(
        self,
        file_path: Path | str,
        description: Optional[str] = None
    ) -> UploadResult:
        """
        Convenience method for uploading SQLite database.
        
        Args:
            file_path: Path to SQLite database file
            description: Optional description
        
        Returns:
            UploadResult
        """
        file_path = Path(file_path)
        metadata = UploadMetadata(
            filename=file_path.name,
            content_type="application/x-sqlite3",
            description=description or f"PUBG Collab Pipeline Database Backup - {datetime.now().strftime('%Y-%m-%d')}",
            tags=["pubg", "collab", "database", "backup"]
        )
        return self.upload_file(file_path, metadata)
    
    def test_connection(self) -> bool:
        """
        Test if the cloud service is reachable.
        
        Returns:
            True if connection is successful
        """
        if not self._configured:
            return False
        
        try:
            with httpx.Client(timeout=10.0) as client:
                # Try a HEAD request to check if endpoint is reachable
                response = client.head(
                    self.upload_url,
                    headers={"Authorization": f"Bearer {self.api_key}"}
                )
                return response.status_code < 500
        except Exception:
            return False


# Factory function for easy instantiation
def create_cloud_client() -> CloudClient:
    """Create a CloudClient instance from environment configuration."""
    return CloudClient()
