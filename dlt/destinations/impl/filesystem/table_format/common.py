import os
from typing import Final, Protocol
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop

DLT_PYARROW_MODULE_NAME: Final[str] = "pyarrow"
PREPARED_TABLE_SCHEMA_PROPERTY_PARTITION: Final[str] = "partition"
LOAD_TABLE_WRITE_DISPOSITION_KEY: Final[str] = "write_disposition"


class HasSourceFilePaths(Protocol):
    file_paths: list[str]


class HasNamedLoadTable(Protocol):
    load_table_name: str
    _load_table: PreparedTableSchema


class HasTableDirectoryGetter(Protocol):
    def get_table_dir(self, table_name: str) -> str:
        """Retrieves the directory in a filesystem for a table object by name.
        Args:
            table_name: name of a table in a filesystem
        Returns:
            directory of table in filesystem as a string
        """
        raise NotImplementedError


class HasRemotePathBuilder(Protocol):
    def make_remote_path(self) -> os.PathLike:
        """Builds remote path for a job or other object.
        Returns:
            remote path for references to systems like S3, GCS, etc
        """
        raise NotImplementedError


class HasRemoteUrlBuilder(Protocol):
    def make_remote_url(self, remote_path: os.PathLike) -> str:
        """Take a remote path and qualify it as needed for use as a url.
        Args:
            remote_path: path-like value to qualify as url
        Returns:
            remote url for use in systems like DuckDB
        """
        raise NotImplementedError


def make_remote_path_for_filesystem_destination(
    job: HasNamedLoadTable, job_client: HasTableDirectoryGetter
) -> os.PathLike:
    """Builds a remote path for a named load table from a table directory getter.
    Args:
        job: load job with a named destination table
        job_client: client with a table directory getter
    Returns:
        remote path housing files/objects for a table
    """
    return job.make_remote_path(job_client.get_table_dir(job.load_table_name))


def make_remote_url_for_filesystem_destination(
    job: HasRemotePathBuilder, job_client: HasRemoteUrlBuilder
) -> str:
    """Executes a remote url builder on a remote path builder.
    Args:
        job: remote path builder
        job_client: remote url builder
    Returns:
        remote url for use in systems like DuckDB
    """

    return job_client.make_remote_url(job.make_remote_path())


def get_arrow_dataset_for_filesystem_source(job: HasSourceFilePaths) -> "Dataset":
    """Imports common pyarrow library when needed to produce dataset for source file paths.
    Args:
        job: job with a source table spread across one or more files
    Returns:
        pyarrow dataset for further processing
    """

    if DLT_PYARROW_MODULE_NAME not in dir():
        from dlt.common.libs.pyarrow import pyarrow
    return pyarrow.dataset.dataset(job.file_paths)


def get_partition_column_names_for_named_load_table(job: HasNamedLoadTable) -> list[str]:
    """Gets column names to use for partitioning a destination table.
    Args:
        job: job or other object with a destination table possessing a name and prepared schema
    Returns:
        list of column names to use for partitioning the destination table
    """
    return get_columns_names_with_prop(job._load_table, PREPARED_TABLE_SCHEMA_PROPERTY_PARTITION)
