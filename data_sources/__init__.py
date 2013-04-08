import os
import re
import urllib
import base64


def data_source_from_uri(data_source_path):
    try:
        data_source_path, query_string = data_source_path.split('?', 1)
    except ValueError:
        columns = {}
    else:
        columns = dict(map(base64.urlsafe_b64decode, x.split('=', 1))
                       for x in re.findall('[^&\?]+=[^&\?]+', query_string))
    res = re.search('dir://(.+)', data_source_path)
    if res:
        groups = map(urllib.unquote, res.groups())
        return DirectoryDataSource(columns=columns, base_dir=groups[0])
    res = re.search('hbase://(.+):([0-9]+)/([^/]+)/([^/]*)/([^/]*)', data_source_path)
    if res:
        groups = map(urllib.unquote, res.groups())
        print(groups)
        return HBaseDataSource(columns=columns, host=groups[0], port=int(groups[1]), table=groups[2],
                               start_row=groups[3], stop_row=groups[4])
    raise ValueError('Unknown data source uri [%s]' % data_source_path)


class BaseDataSource(object):
    """Base DataSource, must extend at least _columns, _row_column_values, and _value"""

    def __init__(self, uri, columns):
        self._pretty_to_raw_columns = columns
        self._raw_to_pretty_columns = dict(x[::-1] for x in columns.items())
        if len(self._pretty_to_raw_columns) != len(self._raw_to_pretty_columns):
            raise ValueError('Column mapping must be 1-to-1!')
        if columns:
            query_string = '&'.join(['%s=%s' % (urllib.quote(x), urllib.quote(y)) for x, y in columns.items()])
            self.uri = uri + '?' + query_string
        else:
            self.uri = uri

    def columns(self, row):
        return (self._raw_to_pretty_columns[x] for x in self._columns(row))

    def column_values(self, row, columns=None):
        if columns is None:
            columns = self._raw_columns
        else:
            columns = [self._pretty_to_raw_columns[column] for column in columns]
        return ((self._raw_to_pretty_columns[x], y) for x, y in self._column_values(row, columns))

    def _column_values(self, row, columns):
        # Fallback implementation
        return ((x, self._value(row, x)) for x in self._columns(row, columns))

    def rows(self):
        return self._rows()  # Allows for future modification

    def row_columns(self):
        for row, columns in self._row_columns():
            pretty_columns = (self._raw_to_pretty_columns[column]
                              for column in columns)
            yield row, pretty_columns

    def row_column_values(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        else:
            columns = [self._pretty_to_raw_columns[column] for column in columns]
        for row, column_values in self._row_column_values(columns=columns):
            pretty_column_values = ((self._raw_to_pretty_columns[column], value)
                                    for column, value in column_values)
            yield row, pretty_column_values

    def value(self, row, column):
        return self._value(row, self._pretty_to_raw_columns[column])

    def _rows(self):
        # Fallback implementation
        return (row for row, _ in self._row_column_values())

    def _row_columns(self, columns=None):
        # Fallback implementation
        if columns is None:
            columns = self._raw_columns
        for row, cur_columns in self._row_column_values(columns):
            yield row, (column for column, _ in cur_columns)


class DirectoryDataSource(BaseDataSource):
    """Data source represented as a directory of directories"""

    def __init__(self, columns, base_dir):
        super(DirectoryDataSource, self).__init__('dir://' + urllib.quote(base_dir), columns)
        self._hbase_columns = columns.values()
        self._raw_columns = columns.values()
        self.base_dir = os.path.abspath(base_dir)

    def _make_path(self, *args):
        out_path = os.path.join(self.base_dir, *args)
        if os.path.abspath(out_path) != out_path:
            raise ValueError('Invalid path [%s]' % out_path)
        if not os.path.exists(out_path):
            raise OSError('Path not found [%s]' % out_path)
        return out_path

    def _rows(self):
        return (x for x in os.listdir(self.base_dir)
                if os.path.isdir(os.path.join(self.base_dir, x)))

    def _columns(self, row, columns=None):
        if columns is None:
            columns = self._raw_columns
        row_dir = self._make_path(row)
        return (x for x in os.listdir(row_dir)
                if os.path.isfile(os.path.join(row_dir, x)) and x in columns)

    def _row_columns(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        for row in self._rows():
            yield row, self._columns(row, columns)

    def _value(self, row, column):
        return open(self._make_path(row, column)).read()

    def _row_column_values(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        for cur_row, cur_columns in self._row_columns(columns):
            yield cur_row, ((column, self._value(cur_row, column))
                            for column in set(columns).intersection(cur_columns))


class HBaseDataSource(BaseDataSource):

    def __init__(self, columns, table, host, port, start_row, stop_row):
        super(HBaseDataSource, self).__init__('hbase://%s:%d/%s/%s/%s' % (urllib.quote(host), port, urllib.quote(table), start_row, stop_row), columns)
        import hadoopy_hbase
        self._hbase = hadoopy_hbase.connect(host, port)
        self._table = table
        self._raw_columns = columns.values()
        self._scanner = lambda *args, **kw: hadoopy_hbase.scanner(self._hbase, self._table,
                                                                  start_row=base64.urlsafe_b64decode(start_row),
                                                                  stop_row=base64.urlsafe_b64decode(stop_row),
                                                                  *args, **kw)

    def _columns(self, row, columns):
        out = self._hbase.getRowWithColumns(self._table, row, columns)
        if not out:
            raise ValueError('Unknown row [%s]' % row)
        return (x for x in out[0].columns)

    def _column_values(self, row, columns):
        out = self._hbase.getRowWithColumns(self._table, row, columns)
        if not out:
            raise ValueError('Unknown row [%s]' % row)
        return ((x, y.value) for x, y in out[0].columns.items())

    def _value(self, row, column):
        out = self._hbase.get(self._table, row, column)
        if not out:
            raise ValueError('Unknown row/column [%s][%s]' % (row, column))
        return out[0].value

    def _row_column_values(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        return ((cur_row, cur_columns.iteritems())
                for cur_row, cur_columns in self._scanner(columns=columns))
