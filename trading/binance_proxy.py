import select
import socket
import socketserver


LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 18080
ALLOWED_HOSTS = {
    "api.binance.com",
    "api1.binance.com",
    "api2.binance.com",
    "api3.binance.com",
    "api4.binance.com",
    "fapi.binance.com",
    "fapi1.binance.com",
    "fapi2.binance.com",
    "fapi3.binance.com",
    "fapi4.binance.com",
}


class BinanceConnectHandler(socketserver.StreamRequestHandler):
    def handle(self):
        upstream = None
        tunnel_open = False
        try:
            self.connection.settimeout(15)
            request_line = self.rfile.readline(4096).decode("ascii", "replace").strip()
            parts = request_line.split()
            if len(parts) != 3 or parts[0].upper() != "CONNECT":
                self.connection.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return

            host, separator, port_text = parts[1].rpartition(":")
            if separator != ":" or host.lower() not in ALLOWED_HOSTS or port_text != "443":
                self.connection.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                return

            for _ in range(100):
                header = self.rfile.readline(8192)
                if header in {b"\r\n", b"\n", b""}:
                    break

            upstream = socket.create_connection((host, 443), timeout=15)
            upstream.settimeout(None)
            self.connection.settimeout(None)
            self.connection.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            tunnel_open = True

            sockets = [self.connection, upstream]
            while True:
                readable, _, _ = select.select(sockets, [], [], 60)
                if not readable:
                    return
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    destination = upstream if source is self.connection else self.connection
                    destination.sendall(data)
        except (OSError, ValueError):
            if not tunnel_open:
                try:
                    self.connection.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                except OSError:
                    pass
        finally:
            if upstream is not None:
                upstream.close()


class ThreadingServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with ThreadingServer((LISTEN_HOST, LISTEN_PORT), BinanceConnectHandler) as server:
        server.serve_forever()
