from gevent import monkey
monkey.patch_all()
import bottle
import argparse
from static_server.auth import verify
import data_sources
import itertools
import ast


@bottle.route('/:auth_key#[a-zA-Z0-9\_\-]+#/')
@verify
def dir_page(auth_key):
    render_rows = []
    for row, columns in itertools.islice(DATA.row_column_values(), ARGS.rows):
        render_columns = []
        for x, y in columns:
            try:
                conv = CONVERT[x]
                print(conv)
                if conv == 'npdouble':
                    import numpy as np
                    y = str(np.fromstring(y, dtype=np.double).tolist())
                elif conv == 'str':
                    pass
                elif conv == 'image':
                    import base64
                    y = '<img src="data:image/jpeg;base64,%s" />' % base64.b64encode(y)
                else:
                    raise ValueError('Unsupported: ' + conv)
                render_columns.append([x, y])
            except KeyError:
                render_columns.append([x, str(len(y))])
        render_rows.append(row + ' | ' + ' '.join([x + '|' + y for x, y in render_columns]))
    return '<br>'.join(render_rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve data source visualization")
    parser.add_argument('data_source')
    parser.add_argument('--port',
                        default='8080')
    parser.add_argument('--rows', default=10, type=int)
    parser.add_argument('--convert', action='append')
    ARGS = parser.parse_args()
    print(ARGS.convert)
    CONVERT = {}
    if ARGS.convert:
        CONVERT = dict(x.split('=', 1) for x in ARGS.convert)
    DATA = data_sources.data_source_from_uri(ARGS.data_source)
    bottle.run(host='0.0.0.0', port=ARGS.port, server='gevent')
