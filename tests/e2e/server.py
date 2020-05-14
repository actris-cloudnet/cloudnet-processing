import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from sys import argv


class Server(BaseHTTPRequestHandler):

    def _set_headers(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers(200)

    def do_POST(self, code=200):
        self.root = argv[1]

        # Connection refused if request not read
        content_length = int(self.headers['Content-Length'])
        self.rfile.read(content_length)

        try:
            file = open(f'{self.root}{self.path}', 'rb')
        except IsADirectoryError:
            file = self.try_to_open_file(f'{self.root}{self.path}/index')
        except FileNotFoundError:
            file = self.try_to_open_file(f'{self.root}{os.path.dirname(self.path)}/any')
        if not file:
            self._set_headers(404)
            return

        self._set_headers(code)
        self.wfile.write(file.read())


    def do_PUT(self):
        return self.do_POST(code=201)

    def do_GET(self):
        return self.do_POST()

    def handle_error(self, request, client_address):
        pass

    def try_to_open_file(self, path):
        try:
            file = open(path, 'rb')
        except FileNotFoundError:
            return None
        return file


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
