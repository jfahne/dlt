## 2025-03-17

DONE:

- [x] Move table format filesystem load job definitions out of `filesystem.py`
- [x] Lifted `TableFormatFilesystemLoadJob` methods to module methods
- [x] Used protocols to make module level implementations better factored
- [x] Reinstrumented Delta and Iceberg load jobs to use module level methods

TODO:

- [ ] Update `filesystem.py` references to use `table_format_filesystem.py` or derivatives
- [ ] Further factor table format load jobs into delta and iceberg specific module
- [ ] Factor `filesystem.py` further by extracting local and remote bare filesystem jobs similarly to table format
- [ ] Fully comment code
