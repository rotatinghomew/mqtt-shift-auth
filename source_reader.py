"""读取本机演练端的实际观测。"""
import argparse
import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit


class Source:
    """用来源名和来源自己的记录编号检索观测。"""

    def __init__(self, directory):
        self.directory = Path(directory)

    def names(self):
        return sorted(path.stem for path in self.directory.glob("*.json"))

    def records(self, name):
        if name not in self.names():
            raise KeyError(name)
        with (self.directory / (name + ".json")).open(encoding="utf-8") as stream:
            return json.load(stream)

    def fetch(self, name=None, record=None):
        if name is None:
            return self.names()
        data = self.records(name)
        return data if record is None else data[record]


def handler_for(source):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/observations":
                fields = []
            elif path.startswith("/observations/"):
                fields = [unquote(part) for part in path[len("/observations/"):].split("/")]
            else:
                fields = ["", "", ""]
            try:
                if len(fields) > 2 or any(not field for field in fields):
                    raise KeyError(path)
                code, value = 200, source.fetch(*fields)
            except KeyError:
                code, value = 404, {"error": "找不到此来源或记录"}
            payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
    return Handler


def main():
    parser = argparse.ArgumentParser(description="读取本机联调观测")
    parser.add_argument("--inputs", type=Path, default=Path(__file__).with_name("inputs"),
                        help="合成来源所在目录")
    commands = parser.add_subparsers(dest="operation", required=True)
    show = commands.add_parser("show", help="列出来源或读取原始观测")
    show.add_argument("source", nargs="?")
    show.add_argument("record", nargs="?")
    serve = commands.add_parser("serve", help="在回环地址提供原始观测")
    serve.add_argument("--port", type=int, default=8172)
    args = parser.parse_args()
    source = Source(args.inputs)
    if args.operation == "serve":
        with ThreadingHTTPServer(("127.0.0.1", args.port), handler_for(source)) as server:
            print(f"演练来源已就绪 http://127.0.0.1:{server.server_port}/observations", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                return
    else:
        try:
            value = source.fetch(args.source, args.record)
        except KeyError as error:
            parser.error(f"找不到来源或记录 {error}")
        print(json.dumps(value, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
