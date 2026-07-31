import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from upstream_kernels import (  # noqa: E402
    ResolutionError,
    parse_tag_refs,
    resolve_matrix,
)


class UpstreamKernelTests(unittest.TestCase):
    def test_selects_latest_final_release(self):
        tags = {"v6.18", "v7.2-rc5", "v7.2"}

        matrix = resolve_matrix(tags)["include"]

        self.assertEqual(
            [(entry["kernel"], entry["arch"]) for entry in matrix],
            [
                ("7.2", "x86_64"),
                ("7.2", "aarch64"),
            ],
        )

    def test_selects_latest_release_candidate_from_newer_series(self):
        tags = {
            "v6.12.99",
            "v6.18.12",
            "v6.19.4",
            "v7.1",
            "v7.1.5",
            "v7.2-rc4",
            "v7.2-rc5",
        }

        matrix = resolve_matrix(tags)["include"]

        selected = [
            (entry["kernel"], entry["arch"])
            for entry in matrix
        ]
        self.assertEqual(
            selected,
            [
                ("7.1.5", "x86_64"),
                ("7.1.5", "aarch64"),
                ("7.2-rc5", "x86_64"),
                ("7.2-rc5", "aarch64"),
            ],
        )

    def test_rejects_tags_without_final_release(self):
        with self.assertRaisesRegex(
            ResolutionError, "no final release tag was found"
        ):
            resolve_matrix({"v6.18-rc7", "v6.19-rc1"})

    def test_parses_only_release_tags(self):
        refs = "\n".join(
            [
                "a" * 40 + "\trefs/tags/v6.18",
                "b" * 40 + "\trefs/tags/v7.2-rc5",
                "c" * 40 + "\trefs/tags/v7.2-rc5^{}",
                "d" * 40 + "\trefs/tags/testing",
            ]
        )

        self.assertEqual(parse_tag_refs(refs), {"v6.18", "v7.2-rc5"})


if __name__ == "__main__":
    unittest.main()
