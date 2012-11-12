import os


class DirectoryReadDataSource(object):
    """Data source represented as a directory of directories"""

    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)

    def _make_path(self, *args):
        out_path = os.path.join(self.base_dir, *args)
        if os.path.abspath(out_path) != out_path:
            raise ValueError('Invalid path [%s]' % out_path)
        if not os.path.exists(out_path):
            raise OSError('Path not found [%s]' % out_path)
        return out_path

    def rows(self):
        return iter(os.listdir(self.base_dir))

    def columns(self, row):
        return iter(os.listdir(self._make_path(row)))

    def row_columns(self):
        for row in self.rows():
            yield row, self.columns(row)

    def value(self, row, column):
        return open(self._make_path(row, column)).read()

    def row_columns_values(self):
        for row, columns in self.row_columns():
            yield row, ((column, self.value(row, column)) for column in columns)


class HBaseReadDataSource(object):

    def __init__(self, table, host='localhost', port=9090, columns=None):
        import hadoopy_hbase
        self._hbase = hadoopy_hbase.connect(host, port)
        self._table = table
        self._columns = columns
        self._scanner = lambda *args, **kw: hadoopy_hbase.scanner(self._hbase, self._table, *args, **kw)

    def rows(self):
        return (row for row, _ in self.row_columns_values())

    def columns(self, row):
        out = self._hbase.getRow(self._table, row)
        if not out:
            raise ValueError('Unknown row [%s]' % row)
        return ((x, y.value) for x, y in out[0].columns.items())

    def row_columns(self):
        for row, columns in self.row_columns_values():
            yield row, (column for column, _ in columns)

    def value(self, row, column):
        out = self._hbase.get(self._table, row, column)
        if not out:
            raise ValueError('Unknown row/column [%s][%s]' % (row, column))
        return out[0].value

    def row_columns_values(self):
        return ((row, columns.iteritems())
                for row, columns in self._scanner(columns=self._columns))
