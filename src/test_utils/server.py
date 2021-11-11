import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from sys import argv


class Server(BaseHTTPRequestHandler):

    def _set_headers(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers(200)

    def do_POST(self, code=200, separate_product_responses=False):

        root = argv[1]

        folder = self._get_product_folder(separate_product_responses)
        self.path = self.path.split('?')[0] + folder

        # Connection refused if request not read
        if 'Content-Length' in self.headers:
            content_length = int(self.headers['Content-Length'])
            self.rfile.read(content_length)

        try:
            file = open(f'{root}{self.path}', 'rb')
        except IsADirectoryError:
            file = self.try_to_open_file(f'{root}{self.path}/index')
        except FileNotFoundError:
            file = self.try_to_open_file(f'{root}{os.path.dirname(self.path)}/any')
        if not file:
            self._set_headers(404)
            return

        self._set_headers(code)
        self.wfile.write(file.read())

    def do_PUT(self):
        return self.do_POST(code=201)

    def do_GET(self):
        return self.do_POST(separate_product_responses=True)

    def do_DELETE(self):
        return self.do_POST()

    def handle_error(self, request, client_address):
        pass

    @staticmethod
    def try_to_open_file(path):
        try:
            file = open(path, 'rb')
        except FileNotFoundError:
            return None
        return file

    def _get_product_folder(self, separate_product_responses: bool):
        prefix = '&product='
        if separate_product_responses and prefix in self.path:
            ind1 = self.path.index(prefix) + len(prefix)
            try:
                ind2 = self.path[ind1:].index('&')
            except ValueError:
                ind2 = len(self.path[ind1:])
            product = self.path[ind1:ind1+ind2]
            return f'/?product={product}'
        return ''


def run(server_class=HTTPServer, handler_class=Server):
    _, root, port = argv
    server_address = ('localhost', int(port))
    httpd = server_class(server_address, handler_class)

    print(f'Serve from {root} on port {port}...')
    httpd.serve_forever()


if __name__ == "__main__":
    if len(argv) == 3:
        run()
    else:
        print('Usage: server.py [root path] [port]')
