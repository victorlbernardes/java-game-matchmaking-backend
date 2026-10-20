#!/usr/bin/env python3
"""A one-way delay in front of an endpoint, so "distant" is a real number and not a config value.

W21 (ADR 0039) proves that a player is placed in a region only when the distance to it fits the
player's own ceiling, and that failing over costs that distance. On one machine every region answers
in a millisecond, so the distance has to be produced: this proxy sits in front of one region's API,
waits `--delay-ms` before forwarding the request and the same again before returning the answer.
A round trip through it therefore costs 2 x delay, which is exactly what the server measures when
the client probes that region - the number in the scenario is the proxy's, but the measurement is
the production one (two requests, timed by the server).

It forwards HTTP only. The game socket is out of scope for this sprint: what is being decided is
*where* the player goes, not how the socket behaves once it is there.

Usage:
  python3 delay_proxy.py --listen 8099 --upstream 8096 --delay-ms 40
"""

import argparse
import http.client
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
              "trailers", "transfer-encoding", "upgrade"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream_host = "localhost"
    upstream_port = 8096
    delay_seconds = 0.04

    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_DELETE(self):
        self._proxy()

    def _proxy(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else None
        # The delay on the way in: the request reaches the region after the cable.
        time.sleep(self.delay_seconds)
        connection = http.client.HTTPConnection(self.upstream_host, self.upstream_port, timeout=15)
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in HOP_BY_HOP and key.lower() not in ("host", "content-length")}
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            payload = response.read()
            status = response.status
            response_headers = [(key, value) for key, value in response.getheaders()
                                if key.lower() not in HOP_BY_HOP and key.lower() != "content-length"]
        except OSError as failure:
            self.send_response(502)
            payload = json.dumps({"error": "upstream unavailable", "reason": str(failure)}).encode()
            status = 502
            response_headers = [("content-type", "application/json")]
        finally:
            connection.close()
        # And the delay on the way out: the answer comes back across the same cable.
        time.sleep(self.delay_seconds)
        self.send_response(status)
        for key, value in response_headers:
            self.send_header(key, value)
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        sys.stderr.write("proxy %s\n" % (fmt % args))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--upstream", type=int, required=True)
    parser.add_argument("--upstream-host", default="localhost")
    parser.add_argument("--delay-ms", type=int, default=40)
    args = parser.parse_args()

    Handler.upstream_host = args.upstream_host
    Handler.upstream_port = args.upstream
    Handler.delay_seconds = args.delay_ms / 1000.0
    server = ThreadingHTTPServer(("0.0.0.0", args.listen), Handler)
    server.daemon_threads = True
    print(json.dumps({"listening": args.listen, "upstream": args.upstream,
                      "delay_ms_each_way": args.delay_ms}), flush=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
