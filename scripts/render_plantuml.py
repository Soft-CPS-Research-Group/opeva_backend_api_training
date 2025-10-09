#!/usr/bin/env python3
"""Render PlantUML diagrams via the Kroki API.

Example:
    python scripts/render_plantuml.py docs/domain_model.puml docs/domain_model.svg
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import urllib.error
import urllib.request

DEFAULT_SERVER = "https://kroki.io"
DEFAULT_FORMAT = "svg"


def render_plantuml(
    source: pathlib.Path,
    target: pathlib.Path,
    *,
    server: str = DEFAULT_SERVER,
    output_format: str = DEFAULT_FORMAT,
) -> None:
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    endpoint = f"{server.rstrip('/')}/plantuml/{output_format.lower()}"
    data = source.read_bytes()
    request = urllib.request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "text/plain"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"PlantUML rendering failed with status {exc.code}: {exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Unable to reach PlantUML server {server}: {exc.reason}") from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=pathlib.Path, help="Path to the .puml file")
    parser.add_argument(
        "target",
        type=pathlib.Path,
        nargs="?",
        help="Output file path (defaults to source with .svg extension)",
    )
    parser.add_argument(
        "--server",
        default=DEFAULT_SERVER,
        help=f"PlantUML render server (default: {DEFAULT_SERVER})",
    )
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT,
        choices=("svg", "png"),
        help=f"Output image format (default: {DEFAULT_FORMAT})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source: pathlib.Path = args.source
    target: pathlib.Path = args.target or source.with_suffix(f".{args.format}")

    try:
        render_plantuml(source, target, server=args.server, output_format=args.format)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Rendered {source} -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
