try:
    import unittest2 as unittest
except ImportError:
    import unittest

import data_sources

class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_hbase(self):
        import hadoopy_hbase
        client = hadoopy_hbase.connect()
        try:
            client.createTable('testtable', [hadoopy_hbase.ColumnDescriptor('colfam1:')])
        except:
            pass
        for x in xrange(100):
            client.mutateRow('testtable', str(x), [hadoopy_hbase.Mutation(column='colfam1:col%d' % y, value=str(x)) for y in range(10)])
        ds = data_sources.HBaseDataSource({'mydata': 'colfam1:col0'}, 'testtable')
        print list(ds.rows())
        print list(ds.columns(list(ds.rows())[0]))
        print [(x, list(y)) for x, y in ds.row_columns()]
        print [(x, dict(y)) for x, y in ds.row_column_values()]
        print ds.uri
        ds = data_sources.data_source_from_uri(ds.uri)
        print list(ds.rows())
        print list(ds.columns(list(ds.rows())[0]))
        print [(x, list(y)) for x, y in ds.row_columns()]
        print [(x, dict(y)) for x, y in ds.row_column_values()]
        print ds.uri

    def test_dir(self):
        import tempfile
        import shutil
        import os
        temp_dir = None
        try:
            temp_dir = tempfile.mkdtemp()
            for x in xrange(100):
                d = os.path.join(temp_dir, str(x))
                try:
                    os.mkdir(d)
                except OSError:
                    pass
                for y in range(10):
                    open(os.path.join(d, 'colfam1:col%d' % y), 'w').write(str(x))
            ds = data_sources.DirectoryDataSource({'mydata': 'colfam1:col0'}, temp_dir)
            print list(ds.rows())
            print list(ds.columns(list(ds.rows())[0]))
            print [(x, list(y)) for x, y in ds.row_columns()]
            print [(x, dict(y)) for x, y in ds.row_column_values()]
            print ds.uri
            ds = data_sources.data_source_from_uri(ds.uri)
            print list(ds.rows())
            print list(ds.columns(list(ds.rows())[0]))
            print [(x, list(y)) for x, y in ds.row_columns()]
            print [(x, dict(y)) for x, y in ds.row_column_values()]
            print ds.uri

        finally:
            if temp_dir is not None:
                shutil.rmtree(temp_dir)

if __name__ == '__main__':
    unittest.main()
