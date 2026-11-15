import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .origin import serve
from .subscriptions import DEFAULT_REPUBLISH_ATTEMPTS, Receiver


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def subscribe(base_url, state_path, feed_path, envelope_file, max_attempts):
    from .reception import subscribe_once

    if not base_url and not envelope_file:
        raise ValueError("subscribe 需要 --base-url 或 --from-file")
    receiver = Receiver(state_path)
    try:
        client = Client(base_url) if base_url else None
        return subscribe_once(
            receiver, client, feed_path=feed_path, envelope_file=envelope_file, max_attempts=max_attempts
        )
    finally:
        receiver.close()


def status(state_path, gateway):
    receiver = Receiver(state_path)
    try:
        gateways = [gateway] if gateway else receiver.stations()
        return [receiver.station_view(name) for name in gateways]
    finally:
        receiver.close()


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与归档")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    subscriber = subcommands.add_parser("subscribe", help="接收一轮订阅通知：落库、确认、补收")
    subscriber.add_argument("--base-url")
    subscriber.add_argument("--state", type=Path, required=True)
    subscriber.add_argument("--feed-path", default="/feed")
    subscriber.add_argument("--from-file", type=Path)
    subscriber.add_argument("--max-attempts", type=int, default=DEFAULT_REPUBLISH_ATTEMPTS)
    viewer = subcommands.add_parser("status", help="按站点查看已收观测、可补收序号与永久缺口")
    viewer.add_argument("--state", type=Path, required=True)
    viewer.add_argument("--gateway")
    subcommands.add_parser("drill", help="本地演练可靠接收全流程（不连接真实设备）")
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "subscribe":
        result = subscribe(args.base_url, args.state, args.feed_path, args.from_file, args.max_attempts)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "status":
        if not args.state.is_file():
            parser.error(f"state 不存在：{args.state}")
        print(json.dumps(status(args.state, args.gateway), ensure_ascii=False, indent=2))
    elif args.command == "drill":
        from .reception import drill
        print(json.dumps(drill(), ensure_ascii=False, indent=2))
    else:
        from .origin import load_scenario
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
