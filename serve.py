import http.server, socketserver

class H(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()
    def __init__(self, *a, **kw):
        super().__init__(*a, directory='web_dashboard', **kw)
    def log_message(self, format, *args):
        print(format % args)

with socketserver.TCPServer(('', 8080), H) as s:
    print('Serving on http://localhost:8080')
    s.serve_forever()