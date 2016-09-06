import sys
import logging

import tornado.options

import ztreamy.client


class EventHandler(object):
    def __init__(self, filename):
        self.filename = filename

    def handle_events(self, events):
        serialized = ''.join(str(event) for event in events)
        self._write(serialized)
        sys.stdout.write(serialized)
        sys.stdout.flush()

    def handle_error(message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)

    def _write(self, data):
        with open(self.filename, mode='a') as f:
            f.write(data)


def read_cmd_options():
    import optparse
    tornado.options.define('filename', default='log-hermes.txt',
                           help='file to where the events are saved',
                           type=str)
    tornado.options.define('label', default='collector',
                           help='define a client label',
                           type=str)
    tornado.options.define('missing', default=True,
                           help=('retrieve missing events '
                                 '(requires a client label)'),
                           type=bool)
    tornado.options.define('deflate', default=True,
                           help='Accept compressed data with deflate',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = optparse.Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        options.stream_urls = ['http://localhost:9109/backend/stream']
    return options

def main():
    options = read_cmd_options()
    disable_compression = not tornado.options.options.deflate
    retrieve_missing_events = tornado.options.options.missing
    client_label = tornado.options.options.label
    stream_urls = options.stream_urls
    filename = tornado.options.options.filename
    handler = EventHandler(filename)
    client = ztreamy.client.Client( \
                stream_urls,
                separate_events=False,
                event_callback=handler.handle_events,
                error_callback=handler.handle_error,
                disable_compression=disable_compression,
                label=client_label,
                retrieve_missing_events=retrieve_missing_events)
    try:
        client.start(loop=True)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
