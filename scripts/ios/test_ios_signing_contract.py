import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = (
    ROOT / ".github/workflows/ios-build-debug.yml",
    ROOT / ".github/workflows/ios-build-release.yml",
)


class WidgetSigningWorkflowContractTest(unittest.TestCase):
    def test_all_native_ios_exports_include_the_widget_profile(self) -> None:
        guard = (
            'if [ -n "${{ env.WIDGET_PROFILE_BUNDLE_ID }}" ] '
            '&& [ -n "${{ env.WIDGET_PROFILE_UUID }}" ]; then'
        )
        mapping = (
            'Add :provisioningProfiles:${{ env.WIDGET_PROFILE_BUNDLE_ID }} '
            'string ${{ env.WIDGET_PROFILE_UUID }}'
        )

        for workflow in WORKFLOWS:
            with self.subTest(workflow=workflow.name):
                source = workflow.read_text()
                self.assertIn(guard, source)
                self.assertIn(mapping, source)


if __name__ == "__main__":
    unittest.main()
