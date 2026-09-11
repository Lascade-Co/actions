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


class TestFlightVersionWorkflowContractTest(unittest.TestCase):
    def test_marketing_version_is_shared_and_build_number_is_unique(self) -> None:
        source = WORKFLOWS[0].read_text()

        self.assertIn('BUILD=$(( 1000 + ${{ github.run_number }} ))', source)
        self.assertIn(
            'CURRENT_PROJECT_VERSION="${{ steps.build_number.outputs.build }}"',
            source,
        )
        self.assertIn(
            'MARKETING_VERSION="${{ steps.marketing.outputs.version }}"',
            source,
        )


if __name__ == "__main__":
    unittest.main()
