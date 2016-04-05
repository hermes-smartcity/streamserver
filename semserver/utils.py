import logging
import os
import os.path
import cStringIO
import gzip
import json


DIRNAME_LOGGING = 'logs-semserver'

def configure_logging(module_name):
    if not os.path.exists(DIRNAME_LOGGING):
        os.makedirs(DIRNAME_LOGGING)
    filename = os.path.join(DIRNAME_LOGGING, module_name + '.log')
    log_format = '%(asctime)-15s %(levelname)s %(message)s'
    date_format = '%Y%m%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        datefmt=date_format)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    file_handler = logging.handlers.WatchedFileHandler(filename)
    file_handler.setFormatter(logging.Formatter(fmt=log_format,
                                                datefmt=date_format))
    file_handler.setLevel(logging.INFO)
    logging.getLogger('').addHandler(file_handler)

def add_server_options(parser, default_port):
    parser.add_argument('-p', '--port', type=int, dest='port',
                        default=default_port, help='TCP port to use')
    parser.add_argument('-b', '--buffer', type=float, dest='buffer',
                        default=2.0,
                        help='Buffer time in seconds (0 for no buffering)')

def serialize_object_json(data, compress=False):
    serialized = json.dumps(data.as_dict())
    if compress:
        serialized = compress_gzip(serialized)
    return serialized

def compress_gzip(data):
    output = cStringIO.StringIO()
    with gzip.GzipFile(fileobj=output, mode='wb') as gzip_file:
        gzip_file.write(data)
    compressed_data = output.getvalue()
    output.close()
    return compressed_data
