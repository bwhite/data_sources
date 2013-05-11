import re
import urllib
import base64


def data_source_from_uri(data_source_path):
    try:
        data_source_path, query_string = data_source_path.split('?', 1)
    except ValueError:
        columns = {}
    else:
        columns = [x.split('=', 1) for x in re.findall('[^&\?]+=[^&\?]+', query_string)]
        columns = {x: base64.urlsafe_b64decode(y) for x, y in columns}
    res = re.search('hbase://(.+):([0-9]+)/([^/]+)/(.*)', data_source_path)
    if res:
        groups = map(urllib.unquote, res.groups())
        print(groups)
        slices = map(base64.urlsafe_b64decode, groups[3].split('/'))
        assert len(slices) % 2 == 0
        slices = [(slices[x * 2], slices[x * 2 + 1]) for x in range(len(slices) / 2)]
        return HBaseDataSource(columns=columns, host=groups[0], port=int(groups[1]), table=groups[2], slices=slices)
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


class HBaseDataSource(BaseDataSource):

    def __init__(self, columns, table, host, port, slices):
        suffix = '/'.join(base64.urlsafe_b64encode(x) + '/' + base64.urlsafe_b64encode(y) for x, y in slices)
        super(HBaseDataSource, self).__init__('hbase://%s:%d/%s/%s' % (urllib.quote(host), port, urllib.quote(table), suffix), columns)
        import hadoopy_hbase
        self._hbase = hadoopy_hbase.connect(host, port)
        self._table = table
        self._raw_columns = columns.values()
        self._slices = slices

    def _row_validate(self, row):
        # TODO: Verify this
        if not self._slices:
            return
        for x, y in self._slices:
            if x <= row < y:
                return
        raise ValueError('Row is not valid!')

    def _scanner(self, *args, **kw):
        import hadoopy_hbase
        for x, y in self._slices:
            for z in hadoopy_hbase.scanner(self._hbase, self._table,
                                           start_row=x,
                                           stop_row=y,
                                           *args, **kw):
                yield z

    def _columns(self, row, columns):
        self._row_validate(row)
        out = self._hbase.getRowWithColumns(self._table, row, columns)
        if not out:
            raise ValueError('Unknown row [%s]' % row)
        return (x for x in out[0].columns)

    def _column_values(self, row, columns):
        self._row_validate(row)
        out = self._hbase.getRowWithColumns(self._table, row, columns)
        if not out:
            raise ValueError('Unknown row [%s]' % row)
        return ((x, y.value) for x, y in out[0].columns.items())

    def _value(self, row, column):
        self._row_validate(row)
        out = self._hbase.get(self._table, row, column)
        if not out:
            raise ValueError('Unknown row/column [%s][%s]' % (row, column))
        return out[0].value

    def _row_column_values(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        return ((cur_row, cur_columns.iteritems())
                for cur_row, cur_columns in self._scanner(columns=columns))

    def _rows(self):
        return (row for row, _ in self._row_column_values())

    def _row_columns(self, columns=None):
        if columns is None:
            columns = self._raw_columns
        return ((cur_row, cur_columns.iterkeys())
                for cur_row, cur_columns in self._scanner(columns=columns, filter="KeyOnlyFilter ()"))
