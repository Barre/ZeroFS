import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..catalog import fail


RFC3339_UTC = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)


class SystemRunner:
    def run(self, arguments: list[str]) -> str:
        result = subprocess.run(
            arguments,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C"},
            check=False,
        )
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip()
            suffix = f": {detail}" if detail else ""
            fail(f"{arguments[0]} failed with status {result.returncode}{suffix}")
        return result.stdout

    def status(self, arguments: list[str]) -> int:
        result = subprocess.run(
            arguments,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C"},
            check=False,
        )
        if result.returncode not in (0, 1):
            detail = result.stderr.decode(errors="replace").strip()
            suffix = f": {detail}" if detail else ""
            fail(f"{arguments[0]} failed with status {result.returncode}{suffix}")
        return result.returncode

    def replace_apt_sources(self, filename: str, contents: str) -> None:
        sources = Path("/etc/apt")
        list_directory = sources / "sources.list.d"
        (sources / "sources.list").unlink(missing_ok=True)
        if list_directory.exists():
            for path in list_directory.iterdir():
                if path.is_dir() and not path.is_symlink():
                    fail(f"unexpected APT source directory: {path}")
                path.unlink()
        list_directory.mkdir(parents=True, exist_ok=True)
        destination = (
            list_directory / filename
            if filename != "sources.list"
            else sources / filename
        )
        destination.write_text(contents, encoding="utf-8")

    def download(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        last_error: Exception | None = None
        for attempt in range(5):
            temporary = destination.with_name(f".{destination.name}.download")
            temporary.unlink(missing_ok=True)
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "ZeroFS-kernel-targets/1"},
                )
                with urllib.request.urlopen(request, timeout=60) as response:
                    with temporary.open("wb") as output:
                        while block := response.read(1024 * 1024):
                            output.write(block)
                temporary.replace(destination)
                return
            except (OSError, urllib.error.URLError) as error:
                last_error = error
                temporary.unlink(missing_ok=True)
                if attempt != 4:
                    time.sleep(2**attempt)
        fail(f"cannot download {url}: {last_error}")

    def url_exists(self, url: str) -> bool:
        request = urllib.request.Request(
            url,
            method="HEAD",
            headers={"User-Agent": "ZeroFS-kernel-targets/1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30):
                return True
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return False
            fail(f"cannot probe {url}: HTTP {error.code}")
        except urllib.error.URLError as error:
            fail(f"cannot probe {url}: {error}")

    def splice_rpm_sighdr(
        self,
        sighdr: Path,
        package: Path,
        destination: Path,
    ) -> None:
        self.run(
            [
                "python3",
                "-c",
                "import sys, koji; koji.splice_rpm_sighdr("
                "open(sys.argv[1], 'rb').read(), sys.argv[2], sys.argv[3])",
                str(sighdr),
                str(package),
                str(destination),
            ]
        )

    def kernel_auto_conf(self, package: Path, destination: Path) -> str:
        destination.mkdir(parents=True, exist_ok=True)
        first = subprocess.Popen(
            ["rpm2cpio", str(package)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C"},
        )
        assert first.stdout is not None
        second = subprocess.run(
            [
                "cpio",
                "--quiet",
                "-idmu",
                "./usr/src/kernels/*/include/config/auto.conf",
            ],
            cwd=destination,
            stdin=first.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C"},
            check=False,
        )
        first.stdout.close()
        first_stderr = first.communicate()[1]
        if first.returncode or second.returncode:
            detail = (first_stderr + second.stderr).decode(
                errors="replace"
            ).strip()
            fail(f"cannot extract kernel-devel configuration: {detail}")
        matches = list(
            destination.glob("usr/src/kernels/*/include/config/auto.conf")
        )
        if len(matches) != 1:
            fail("kernel-devel contains an unexpected auto.conf layout")
        return matches[0].read_text(encoding="utf-8")


def parse_as_of(value: str) -> datetime:
    if not RFC3339_UTC.fullmatch(value):
        fail("--as-of must be UTC RFC3339 (YYYY-MM-DDTHH:MM:SSZ)")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        fail(f"invalid --as-of timestamp: {error}")
    if parsed > datetime.now(timezone.utc) + timedelta(minutes=5):
        fail("--as-of must not be in the future")
    return parsed
