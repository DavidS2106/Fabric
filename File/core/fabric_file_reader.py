"""List and fetch Microsoft Fabric Lakehouse (OneLake) target files.

Two access modes, both configuration:

* ``params.local_mount_path`` — the lakehouse is already mounted on this host
  (a Fabric notebook mounts OneLake under ``/lakehouse/default``). Files are
  read straight off the mount.
* OneLake — ``params.onelake_account_url`` plus the workspace and lakehouse
  names. Files are listed and fetched through the Azure Data Lake client.

Neither the workspace, the lakehouse, the base path nor any folder or file
name appears in this module: every one of them comes from
``File/config/connection/target.yml``.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

from .file_config_loader import FileConfigError, FileConnection, FileFrameworkSettings

logger = logging.getLogger(__name__)

#: OneLake addresses a lakehouse as ``<name>.Lakehouse`` inside the workspace
#: filesystem. The suffix is part of the OneLake address scheme, not a naming
#: convention imposed on the deployment.
LAKEHOUSE_SUFFIX = ".Lakehouse"

#: Credential classes selected by the configured ``authentication`` value.
CREDENTIAL_FACTORIES = {
    "activedirectoryinteractive": "InteractiveBrowserCredential",
    "active_directory_interactive": "InteractiveBrowserCredential",
    "interactive": "InteractiveBrowserCredential",
    "activedirectoryserviceprincipal": "ClientSecretCredential",
    "active_directory_service_principal": "ClientSecretCredential",
    "service_principal": "ClientSecretCredential",
    "default": "DefaultAzureCredential",
    "defaultazurecredential": "DefaultAzureCredential",
    "managed_identity": "ManagedIdentityCredential",
    "azure_cli": "AzureCliCredential",
}


class FabricFileAccessError(RuntimeError):
    """Raised when a configured Lakehouse file target cannot be reached."""


@dataclass(frozen=True)
class FabricFile:
    """One file in the configured Lakehouse target folder."""

    name: str
    stem: str
    extension: str
    #: Path relative to the configured target folder.
    relative_path: str
    #: Full, human-readable location for reports and reconciliation.
    location: str
    size_bytes: int


class FabricFileReader:
    """List and fetch the target files of a ``fabric_lakehouse_file`` target."""

    def __init__(
        self,
        connection: FileConnection,
        settings: FileFrameworkSettings,
        cache_directory: Path,
    ):
        self.connection = connection
        self.settings = settings
        self.cache_directory = Path(cache_directory)
        self._service = None

    # ── configuration ─────────────────────────────────────────────────

    @property
    def base_path(self) -> str:
        """The folder inside the lakehouse targets are resolved from.

        Optional: unset, a target's ``relative_path`` resolves from the
        lakehouse root.
        """
        configured = self.connection.file_storage.get("base_path")
        text = None if configured is None else str(configured).strip()
        if not text or text.startswith("${"):
            return ""
        return text.strip("/")

    @property
    def mount_path(self) -> Optional[Path]:
        configured = self.connection.param("local_mount_path")
        return Path(configured) if configured else None

    def describe(self, relative_path: str) -> str:
        """A log/report-safe description of the configured target folder."""
        if self.mount_path is not None:
            return str(self._mounted_folder(relative_path))
        workspace = self.connection.param("workspace", required=True)
        lakehouse = self.connection.param("lakehouse", required=True)
        return (
            f"{workspace}/{lakehouse}{LAKEHOUSE_SUFFIX}/"
            f"{self._remote_folder(relative_path)}"
        )

    # ── listing ───────────────────────────────────────────────────────

    def list_files(self, relative_path: str) -> List[FabricFile]:
        if self.mount_path is not None:
            return self._list_mounted(relative_path)
        return self._list_onelake(relative_path)

    def fetch(self, target: FabricFile, relative_path: str) -> Path:
        """Return a local path holding *target*'s bytes."""
        if self.mount_path is not None:
            return self._mounted_folder(relative_path) / target.relative_path
        return self._download(target, relative_path)

    # ── mounted lakehouse ─────────────────────────────────────────────

    def _mounted_folder(self, relative_path: str) -> Path:
        folder = self.mount_path / self.base_path
        return folder / relative_path.strip("/") if relative_path else folder

    def _list_mounted(self, relative_path: str) -> List[FabricFile]:
        folder = self._mounted_folder(relative_path)
        if not folder.is_dir():
            raise FabricFileAccessError(
                f"Configured Lakehouse target folder does not exist: {folder}"
            )
        candidates = (
            folder.rglob("*") if self.settings.discovery.recursive
            else folder.glob("*")
        )
        found = []
        for candidate in sorted(candidates):
            if not candidate.is_file():
                continue
            relative = candidate.relative_to(folder).as_posix()
            if self.settings.discovery.ignore_hidden and any(
                part.startswith(".") for part in PurePosixPath(relative).parts
            ):
                continue
            found.append(FabricFile(
                name=candidate.name,
                stem=candidate.stem,
                extension=candidate.suffix.lower().lstrip("."),
                relative_path=relative,
                location=str(candidate),
                size_bytes=candidate.stat().st_size,
            ))
        return found

    # ── OneLake ───────────────────────────────────────────────────────

    def _remote_folder(self, relative_path: str) -> str:
        lakehouse = self.connection.param("lakehouse", required=True)
        parts = [f"{lakehouse}{LAKEHOUSE_SUFFIX}", self.base_path]
        if relative_path:
            parts.append(relative_path.strip("/"))
        return "/".join(part for part in parts if part)

    def _filesystem_client(self):
        if self._service is None:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError as exc:  # pragma: no cover - deployment issue
                raise FabricFileAccessError(
                    "Reading a Fabric Lakehouse file target over OneLake "
                    "requires azure-storage-file-datalake; install it, or set "
                    "params.local_mount_path to read through a mount"
                ) from exc
            account_url = self.connection.param(
                "onelake_account_url", required=True
            )
            self._service = DataLakeServiceClient(
                account_url=account_url, credential=self._credential()
            )
        workspace = self.connection.param("workspace", required=True)
        return self._service.get_file_system_client(workspace)

    def _credential(self):
        configured = (self.connection.param("authentication") or "").strip()
        key = configured.lower().replace(" ", "")
        factory = CREDENTIAL_FACTORIES.get(key)
        if factory is None:
            raise FileConfigError(
                f"File target connection '{self.connection.name}' declares an "
                f"unsupported authentication '{configured}'. Supported: "
                + ", ".join(sorted(set(CREDENTIAL_FACTORIES.values())))
            )
        try:
            import azure.identity as identity
        except ImportError as exc:  # pragma: no cover - deployment issue
            raise FabricFileAccessError(
                "Reading a Fabric Lakehouse file target over OneLake requires "
                "azure-identity"
            ) from exc
        credential_class = getattr(identity, factory)
        if factory == "ClientSecretCredential":
            return credential_class(
                tenant_id=self.connection.param("tenant_id", required=True),
                client_id=self.connection.param("client_id", required=True),
                client_secret=self.connection.param(
                    "client_secret", required=True
                ),
            )
        return credential_class()

    def _list_onelake(self, relative_path: str) -> List[FabricFile]:
        folder = self._remote_folder(relative_path)
        client = self._filesystem_client()
        try:
            paths = list(client.get_paths(
                path=folder, recursive=self.settings.discovery.recursive
            ))
        except Exception as exc:
            raise FabricFileAccessError(
                f"Configured Lakehouse target folder could not be listed: "
                f"{folder} ({type(exc).__name__})"
            ) from exc
        found = []
        for entry in paths:
            if getattr(entry, "is_directory", False):
                continue
            full = str(entry.name)
            relative = full[len(folder):].lstrip("/") if full.startswith(folder) else full
            if self.settings.discovery.ignore_hidden and any(
                part.startswith(".") for part in PurePosixPath(relative).parts
            ):
                continue
            name = PurePosixPath(full).name
            found.append(FabricFile(
                name=name,
                stem=PurePosixPath(name).stem,
                extension=PurePosixPath(name).suffix.lower().lstrip("."),
                relative_path=relative,
                location=f"{self.connection.param('workspace')}/{full}",
                size_bytes=int(getattr(entry, "content_length", 0) or 0),
            ))
        return found

    def _download(self, target: FabricFile, relative_path: str) -> Path:
        folder = self._remote_folder(relative_path)
        client = self._filesystem_client()
        destination = self.cache_directory / relative_path.strip("/") / target.relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        remote = f"{folder}/{target.relative_path}".replace("//", "/")
        try:
            downloader = client.get_file_client(remote).download_file()
            with destination.open("wb") as sink:
                shutil.copyfileobj(downloader, sink, length=4 * 1024 * 1024)
        except AttributeError:
            # Older client versions expose readinto only.
            with destination.open("wb") as sink:
                client.get_file_client(remote).download_file().readinto(sink)
        except Exception as exc:
            raise FabricFileAccessError(
                f"Configured Lakehouse target file could not be read: "
                f"{remote} ({type(exc).__name__})"
            ) from exc
        return destination

    def as_details(self, relative_path: str) -> Dict[str, Any]:
        return {
            "target_access_mode": (
                "local_mount" if self.mount_path is not None else "onelake"
            ),
            "target_folder": self.describe(relative_path),
        }
