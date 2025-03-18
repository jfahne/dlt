## 2025-03-17

DONE:

- [x] Move table format filesystem load job definitions out of `filesystem.py`
- [x] Lifted `TableFormatFilesystemLoadJob` methods to module methods
- [x] Used protocols to make module level implementations better factored
- [x] Reinstrumented Delta and Iceberg load jobs to use module level methods
- [x] Update `filesystem.py` references to use `table_format_filesystem.py` or derivatives
- [x] Further factor table format load jobs into delta and iceberg specific module
- [x] Fully comment code

TODO:

- [ ] Factor `filesystem.py` further by extracting local and remote bare filesystem jobs similarly to table format
- [ ] Factor `filesystem.py` further by extracting client implementation into separate clients per table format and local/remote bare filesystem
