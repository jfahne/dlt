import os
from typing import Protocol, Final
from dlt.common.destination.client import RunnableLoadJob
from dlt.common.metrics import LoadJobMetrics
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.destinations.job_impl import ReferenceFollowupJobRequest

PREPARED_TABLE_SCHEMA_PROPERTY_PARTITION: Final[str] = "partition"


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

    if "pyarrow" not in dir():
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


class DeltaLoadFilesystemJob(RunnableLoadJob):
    def __init__(self, file_path: os.PathLike):
        super().__init__(file_path)
        self._job_client: "FilesystemClient" = None
        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)

    def make_remote_path(self) -> os.PathLike:
        """Builds a remote path for a Delta table destination.
        Returns:
            remote path housing Delta table files/objects
        """

        return make_remote_path_for_filesystem_destination(self, self._job_client)

    def make_remote_url(self) -> str:
        """Builds a remote url for a Delta table destination
        Returns: remote url for use in systems like DuckDB
        """
        return make_remote_url_for_filesystem_destination(self, self._job_client)

    @property
    def arrow_dataset(self):
        return get_arrow_dataset_for_filesystem_source(self)

    @property
    def _partition_columns(self) -> List[str]:
        return get_partition_column_names_for_named_load_table(self)

    def run(self) -> None:
        # create Arrow dataset from Parquet files
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.deltalake import write_delta_table, merge_delta_table

        logger.info(
            f"Will copy file(s) {self.file_paths} to delta table {self.make_remote_url()} [arrow"
            f" buffer: {pa.total_allocated_bytes()}]"
        )
        source_ds = self.arrow_dataset
        delta_table = self._delta_table()

        # explicitly check if there is data
        # (https://github.com/delta-io/delta-rs/issues/2686)
        if source_ds.head(1).num_rows == 0:
            delta_table = self._create_or_evolve_delta_table(source_ds, delta_table)
        else:
            with source_ds.scanner().to_reader() as arrow_rbr:  # RecordBatchReader
                if self._load_table["write_disposition"] == "merge" and delta_table is not None:
                    merge_delta_table(
                        table=delta_table,
                        data=arrow_rbr,
                        schema=self._load_table,
                    )
                else:
                    write_delta_table(
                        table_or_uri=(
                            self.make_remote_url() if delta_table is None else delta_table
                        ),
                        data=arrow_rbr,
                        write_disposition=self._load_table["write_disposition"],
                        partition_by=self._partition_columns,
                        storage_options=self._storage_options,
                    )
        # release memory ASAP by deleting objects explicitly
        del source_ds
        del delta_table
        logger.info(
            f"Copied {self.file_paths} to delta table {self.make_remote_url()} [arrow buffer:"
            f" {pa.total_allocated_bytes()}]"
        )

    @property
    def _storage_options(self) -> Dict[str, str]:
        from dlt.common.libs.deltalake import _deltalake_storage_options

        return _deltalake_storage_options(self._job_client.config)

    def _delta_table(self) -> Optional["DeltaTable"]:  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.deltalake import DeltaTable

        if DeltaTable.is_deltatable(self.make_remote_url(), storage_options=self._storage_options):
            return DeltaTable(self.make_remote_url(), storage_options=self._storage_options)
        else:
            return None

    def _create_or_evolve_delta_table(self, arrow_ds: "Dataset", delta_table: "DeltaTable") -> "DeltaTable":  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.deltalake import (
            DeltaTable,
            ensure_delta_compatible_arrow_schema,
            _evolve_delta_table_schema,
        )

        if delta_table is None:
            return DeltaTable.create(
                table_uri=self.make_remote_url(),
                schema=ensure_delta_compatible_arrow_schema(arrow_ds.schema),
                mode="overwrite",
                partition_by=self._partition_columns,
                storage_options=self._storage_options,
            )
        else:
            return _evolve_delta_table_schema(delta_table, arrow_ds.schema)

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        return m._replace(remote_url=self.make_remote_url())


class IcebergLoadFilesystemJob(RunnableLoadJob):
    def __init__(self, file_path: os.PathLike):
        super().__init__(file_path)
        self._job_client: "FilesystemClient" = None
        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)

    def make_remote_path(self) -> os.PathLike:
        """Builds a remote path for an Iceberg table destination.
        Returns:
            remote path housing Delta table files/objects
        """

        return make_remote_path_for_filesystem_destination(self, self._job_client)

    def make_remote_url(self) -> str:
        """Builds a remote url for an Iceberg table destination
        Returns:
            remote url for use in systems like DuckDB
        """
        return make_remote_url_for_filesystem_destination(self, self._job_client)

    @property
    def arrow_dataset(self):
        return get_arrow_dataset_for_filesystem_source(self)

    @property
    def _partition_columns(self) -> List[str]:
        return get_partition_column_names_for_named_load_table(self)

    def run(self) -> None:
        from dlt.common.libs.pyiceberg import write_iceberg_table

        write_iceberg_table(
            table=self._iceberg_table(),
            data=self.arrow_dataset.to_table(),
            write_disposition=self._load_table["write_disposition"],
        )

    def _iceberg_table(self) -> "pyiceberg.table.Table":  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.pyiceberg import get_catalog

        catalog = get_catalog(
            client=self._job_client,
            table_name=self.load_table_name,
            schema=self.arrow_dataset.schema,
            partition_columns=self._partition_columns,
        )
        return catalog.load_table(self.table_identifier)

    @property
    def table_identifier(self) -> str:
        return f"{self._job_client.dataset_name}.{self.load_table_name}"

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        return m._replace(remote_url=self.make_remote_url())
