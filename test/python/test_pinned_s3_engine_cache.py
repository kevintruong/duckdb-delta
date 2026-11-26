"""Run with DUCKDB_DELTA_EXTENSION set to the built extension path."""

import ctypes
import hashlib
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

import duckdb

TABLE_PATH = (
    Path(__file__).resolve().parents[2] / "data/inlined/simple_table/delta_lake"
)
S3_PATH = "s3://fixture/table"
LAST_MODIFIED = "Tue, 14 Nov 2023 22:13:20 GMT"
PROXY_VARIABLES = (
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "NO_PROXY",
    "no_proxy",
    "REQUEST_METHOD",
)


class S3Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        url = urlsplit(self.path)
        key = unquote(url.path).strip("/").partition("/")[2]
        if f"Credential={self.server.access_key}/" not in self.headers.get(
            "Authorization", ""
        ):
            self.respond(
                403, b"<Error><Code>AccessDenied</Code></Error>", "application/xml"
            )
            return

        if key.startswith("table/_delta_log/"):
            self.server.log_requests.append(self.client_address[1])
        query = parse_qs(url.query, keep_blank_values=True)
        if "list-type" in query:
            prefix = query.get("prefix", [""])[0]
            entries = "".join(
                f"<Contents><Key>{escape(name)}</Key><LastModified>2023-11-14T22:13:20Z</LastModified>"
                f'<ETag>"{hashlib.md5(data).hexdigest()}"</ETag><Size>{len(data)}</Size>'
                "<StorageClass>STANDARD</StorageClass></Contents>"
                for name, data in self.server.objects.items()
                if name.startswith(prefix)
            )
            listing = (
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Name>fixture</Name><Prefix>{escape(prefix)}</Prefix>"
                f"<IsTruncated>false</IsTruncated>{entries}</ListBucketResult>"
            )
            self.respond(200, listing.encode(), "application/xml")
            return

        data = self.server.objects.get(key)
        if data is None:
            self.respond(
                404, b"<Error><Code>NoSuchKey</Code></Error>", "application/xml"
            )
            return

        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        byte_range = self.headers.get("Range")
        if byte_range:
            start, _, end = byte_range.removeprefix("bytes=").partition("-")
            first = int(start) if start else max(len(data) - int(end), 0)
            last = min(int(end), len(data) - 1) if start and end else len(data) - 1
            self.respond(
                206,
                data[first : last + 1],
                etag=etag,
                content_range=f"bytes {first}-{last}/{len(data)}",
            )
            return
        self.respond(200, data, etag=etag)

    def respond(
        self,
        code,
        body,
        content_type="application/octet-stream",
        etag=None,
        content_range=None,
    ):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", content_type)
        self.send_header("Last-Modified", LAST_MODIFIED)
        self.send_header("Accept-Ranges", "bytes")
        if etag:
            self.send_header("ETag", etag)
        if content_range:
            self.send_header("Content-Range", content_range)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


class PinnedS3EngineCacheTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.extension_path = str(
            Path(os.environ["DUCKDB_DELTA_EXTENSION"]).resolve()
        ).replace("'", "''")
        if os.name == "posix":
            import _duckdb

            ctypes.CDLL(_duckdb.__file__, mode=os.RTLD_GLOBAL | os.RTLD_NOW)
        cls.objects = {
            f"table/{path.relative_to(TABLE_PATH).as_posix()}": path.read_bytes()
            for path in sorted(TABLE_PATH.rglob("*"))
            if path.is_file()
        }

    def setUp(self):
        self.enterContext(patch.dict(os.environ))
        for name in PROXY_VARIABLES:
            os.environ.pop(name, None)
        os.environ["NO_PROXY"] = "127.0.0.1"

        self.origin = self.start_server()
        self.conn = self.enterContext(
            duckdb.connect(config={"allow_unsigned_extensions": "true", "threads": 2})
        )
        self.conn.execute("LOAD httpfs")
        self.conn.execute(f"LOAD '{self.extension_path}'")
        self.create_secret(self.conn, "first")

    def start_server(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), S3Handler)
        server.objects = self.objects
        server.access_key = "first"
        server.log_requests = []
        Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def create_secret(self, conn, access_key):
        self.origin.access_key = access_key
        conn.execute(f"""CREATE OR REPLACE SECRET fixture (
                TYPE S3, PROVIDER config, KEY_ID '{access_key}', SECRET 'test-placeholder',
                REGION 'us-east-1', ENDPOINT '127.0.0.1:{self.origin.server_port}',
                USE_SSL false, URL_STYLE 'path', SCOPE 's3://fixture'
            )""")

    @staticmethod
    def attach(conn):
        conn.execute(
            f"ATTACH '{S3_PATH}' AS pinned (TYPE delta, PIN_SNAPSHOT true, READ_ONLY)"
        )

    def read(self, conn, row):
        self.assertEqual(
            conn.execute(f"SELECT i FROM pinned WHERE i = {row}").fetchall(), [(row,)]
        )

    def engine_drops(self):
        return self.conn.execute(
            """SELECT count(*) FROM duckdb_logs
               WHERE type = 'DeltaKernel' AND message LIKE '%dropping engine interface%'"""
        ).fetchone()[0]

    def test_proxy_change_rebuilds_engine(self):
        proxy = self.start_server()
        self.attach(self.conn)
        self.read(self.conn, 1)
        self.origin.log_requests.clear()

        os.environ["HTTP_PROXY"] = f"http://127.0.0.1:{proxy.server_port}"
        del os.environ["NO_PROXY"]
        self.read(self.conn, 2)

        self.assertFalse(
            self.origin.log_requests,
            "the Delta-log read still went directly to the origin",
        )
        self.assertTrue(
            proxy.log_requests, "the new proxy received no Delta-log request"
        )

        os.environ["NO_PROXY"] = "127.0.0.1"
        proxy.log_requests.clear()
        self.read(self.conn, 3)

        self.assertTrue(
            self.origin.log_requests,
            "the NO_PROXY change did not restore the direct route",
        )
        self.assertFalse(
            proxy.log_requests, "the Delta-log read still went through the proxy"
        )

    def test_owner_close_releases_rotated_engine(self):
        self.conn.execute("CALL enable_logging(level='debug', storage='memory')")
        self.conn.execute("SET delta_kernel_logging=true")
        self.addCleanup(lambda: self.conn.execute("RESET delta_kernel_logging"))

        with self.conn.cursor() as owner:
            self.attach(owner)
            self.read(owner, 1)
            self.create_secret(owner, "second")
            self.read(owner, 2)
            self.conn.execute("CALL truncate_duckdb_logs()")

        self.assertEqual(
            self.engine_drops(), 1, "the rotated engine outlived its owner"
        )
        self.read(self.conn, 3)
        self.assertEqual(
            self.engine_drops(),
            2,
            "the surviving connection did not use its own engine",
        )
        self.conn.execute("DETACH pinned")
        self.assertEqual(
            self.engine_drops(),
            3,
            "detach did not release the original snapshot engine",
        )


if __name__ == "__main__":
    unittest.main()
