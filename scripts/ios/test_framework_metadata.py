import plistlib
import stat
import tempfile
import unittest
from pathlib import Path

from normalize_framework_metadata import normalize_framework_metadata


ROOT = Path(__file__).resolve().parents[2]


class FrameworkMetadataTest(unittest.TestCase):
    def _framework(self, root: Path, name: str, metadata: dict) -> Path:
        framework = root / f"{name}.xcframework" / "ios-arm64" / f"{name}.framework"
        framework.mkdir(parents=True)
        with (framework / "Info.plist").open("wb") as output:
            plistlib.dump(metadata, output)
        return framework

    def test_copies_bundle_version_to_missing_short_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            framework = self._framework(root, "libEGL", {"CFBundleVersion": "1.0"})

            changed = normalize_framework_metadata([root / "libEGL.xcframework"])

            with (framework / "Info.plist").open("rb") as source:
                metadata = plistlib.load(source)
            self.assertEqual("1.0", metadata["CFBundleShortVersionString"])
            self.assertEqual([framework / "Info.plist"], changed)

    def test_preserves_existing_short_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            framework = self._framework(
                root,
                "libGLESv2",
                {
                    "CFBundleVersion": "1.0",
                    "CFBundleShortVersionString": "2.3",
                },
            )

            changed = normalize_framework_metadata([root / "libGLESv2.xcframework"])

            with (framework / "Info.plist").open("rb") as source:
                metadata = plistlib.load(source)
            self.assertEqual("2.3", metadata["CFBundleShortVersionString"])
            self.assertEqual([], changed)

    def test_normalizes_read_only_plist_and_preserves_its_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            framework = self._framework(root, "libEGL", {"CFBundleVersion": "1.0"})
            plist = framework / "Info.plist"
            plist.chmod(0o444)

            normalize_framework_metadata([root / "libEGL.xcframework"])

            with plist.open("rb") as source:
                metadata = plistlib.load(source)
            self.assertEqual("1.0", metadata["CFBundleShortVersionString"])
            self.assertEqual(0o444, stat.S_IMODE(plist.stat().st_mode))

    def test_check_mode_rejects_missing_short_version_without_modifying_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            framework = self._framework(root, "libEGL", {"CFBundleVersion": "1.0"})

            with self.assertRaisesRegex(ValueError, "CFBundleShortVersionString"):
                normalize_framework_metadata(
                    [root / "libEGL.xcframework"], check_only=True
                )

            with (framework / "Info.plist").open("rb") as source:
                metadata = plistlib.load(source)
            self.assertNotIn("CFBundleShortVersionString", metadata)

    def test_rejects_framework_without_any_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._framework(root, "libEGL", {})

            with self.assertRaisesRegex(ValueError, "CFBundleVersion"):
                normalize_framework_metadata([root / "libEGL.xcframework"])

    def test_rejects_input_without_framework_plists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            with self.assertRaisesRegex(ValueError, "no framework Info.plist"):
                normalize_framework_metadata([root])

    def test_spm_publish_normalizes_angle_and_ios_builds_validate_archives(self) -> None:
        publish = (ROOT / ".github/workflows/publish-spm.yml").read_text()
        self.assertIn(
            "python3 - spm-repo/libEGL.xcframework spm-repo/libGLESv2.xcframework",
            publish,
        )

        validation = "python3 - --check build/App.xcarchive"
        for name in ("ios-build-debug.yml", "ios-build-release.yml"):
            with self.subTest(workflow=name):
                source = (ROOT / ".github/workflows" / name).read_text()
                self.assertIn(validation, source)


if __name__ == "__main__":
    unittest.main()
