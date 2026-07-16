#!/usr/bin/env python3
"""
Switch an Xcode project to manual code signing and assign per-target
provisioning profiles, selecting the profile by PRODUCT_BUNDLE_IDENTIFIER.

Required environment variables:
    APP_PROFILE_NAME  – Provisioning profile name for the main app target

Optional (each required only when the matching target exists in the project):
    NSE_PROFILE_NAME           – Profile for the OneSignal NSE target
    WIDGET_PROFILE_NAME        – Profile for a widget / live-activity extension
    LIVE_ACTIVITY_PROFILE_NAME – Legacy alias for WIDGET_PROFILE_NAME (backward compat)
    WATCH_PROFILE_NAME         – Profile for a watchOS companion app
    IOS_TEAM_ID                – Development team; rewritten on every target when set
    PBXPROJ_PATH               – Path to project.pbxproj
                                 (default: ios/Runner.xcodeproj/project.pbxproj)

Behaviour is backward compatible: apps whose project has no widget/watch targets
never reference WIDGET_/WATCH_ vars, so existing pipelines are unaffected.

Usage:
    python3 fix_ios_signing.py
"""

import os
import re
import sys


def main():
    path = os.environ.get("PBXPROJ_PATH", "ios/Runner.xcodeproj/project.pbxproj")
    team_id = os.environ.get("IOS_TEAM_ID")

    app_name = os.environ.get("APP_PROFILE_NAME")
    nse_name = os.environ.get("NSE_PROFILE_NAME")
    widget_name = os.environ.get("WIDGET_PROFILE_NAME") or os.environ.get(
        "LIVE_ACTIVITY_PROFILE_NAME"
    )
    watch_name = os.environ.get("WATCH_PROFILE_NAME")

    if not app_name:
        print("ERROR: APP_PROFILE_NAME must be set", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(path):
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)

    def profile_for(bundle):
        """Choose a provisioning profile name from the target's bundle id."""
        if bundle.endswith(".OneSignalNotificationServiceExtension"):
            if not nse_name:
                print(f"ERROR: NSE_PROFILE_NAME required for target {bundle}", file=sys.stderr)
                sys.exit(1)
            return nse_name
        if ".watchkit" in bundle:  # watch app or watch extension
            if not watch_name:
                print(f"ERROR: WATCH_PROFILE_NAME required for target {bundle}", file=sys.stderr)
                sys.exit(1)
            return watch_name
        # Extension bundle ids append the extension name as the final component,
        # so match on that alone — never on the full id (avoids false positives
        # like a main app id of com.example.WidgetApp).
        last = bundle.split(".")[-1].lower()  # case-insensitive suffix match
        if (
            last.endswith("widget")
            # ...Widget / WidgetExtension / widget-extension / widget_extension —
            # a "widget" component ending in "extension" (never plain "widgetapp").
            or ("widget" in last and last.endswith("extension"))
            or "liveactivity" in last
        ):
            if not widget_name:
                print(
                    f"ERROR: WIDGET_PROFILE_NAME (or LIVE_ACTIVITY_PROFILE_NAME) "
                    f"required for target {bundle}",
                    file=sys.stderr,
                )
                sys.exit(1)
            return widget_name
        return app_name  # main app + test targets

    def defines(bl, key):
        """True if line `bl` defines `key` (plain, quoted, or KEY[sdk=...] form).

        Anchored at line start (allowing leading indent / opening quote) so it
        never matches the key inside a value or a commented-out setting.
        """
        return re.match(rf'\s*"?{re.escape(key)}(\[[^\]]*\])?"?\s*=', bl)

    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    result = []
    block = []
    in_settings = False
    depth = 0

    for line in lines:
        if not in_settings:
            if "buildSettings = {" not in line:
                result.append(line)
                continue
            in_settings = True
            depth = line.count("{") - line.count("}")
            block = [line]
            if depth > 0:
                continue
            # single-line block (e.g. "buildSettings = {};") — process below.
        else:
            depth += line.count("{") - line.count("}")
            block.append(line)
            if depth > 0:
                continue

        # Block complete — only touch target build configs (those with a bundle id).
        text = "".join(block)
        m = re.search(r'"?PRODUCT_BUNDLE_IDENTIFIER(?:\[[^\]]*\])?"?\s*=\s*"?([^";]+)"?;', text)
        if not m:
            result.extend(block)
            in_settings = False
            block = []
            continue

        name = profile_for(m.group(1).strip())

        desired = {
            "CODE_SIGN_STYLE": "Manual",
            "CODE_SIGN_IDENTITY": '"Apple Distribution"',
            "PROVISIONING_PROFILE_SPECIFIER": f'"{name}"',
        }
        if team_id:
            desired["DEVELOPMENT_TEAM"] = team_id

        seen = set()
        processed = []
        for bl in block:
            for key, val in desired.items():
                # No `key not in seen` guard: a block may hold both the plain
                # setting and an SDK-conditional variant (e.g. CODE_SIGN_STYLE and
                # "CODE_SIGN_STYLE[sdk=iphoneos*]") — both must be rewritten.
                if defines(bl, key):
                    # Capture the LHS (key + any [sdk=...] conditional + optional
                    # quote) so we replace the value after *its* `=`, never an `=`
                    # inside the brackets. lambda replacement keeps backslashes /
                    # group refs in profile names / team ids literal.
                    bl = re.sub(
                        rf'(\s*"?{re.escape(key)}(?:\[[^\]]*\])?"?)\s*=\s*'
                        r'(?:"[^"\\]*(?:\\.[^"\\]*)*"|[^;]*);',
                        lambda m: f"{m.group(1)} = {val};",
                        bl,
                        count=1,
                    )
                    seen.add(key)
                    break
            processed.append(bl)

        # Add any settings that were absent, before the closing brace.
        missing = [k for k in desired if k not in seen]
        if missing:
            indent = "\t\t\t\t"
            for bl in processed:
                mm = re.match(r"^(\s+)\S", bl)
                if mm and "buildSettings" not in bl:
                    indent = mm.group(1)
                    break
            if len(processed) > 1:
                # Multi-line block: insert each setting on its own line before "};".
                closing = processed.pop()
                processed.extend(f"{indent}{k} = {desired[k]};\n" for k in missing)
                processed.append(closing)
            else:
                # Single-line block (e.g. "buildSettings = { … };"): inject the
                # settings inside the braces so they stay within the block.
                settings_str = " ".join(f"{k} = {desired[k]};" for k in missing)
                processed[0] = re.sub(
                    r"\s*\}\s*;\s*$",
                    f" {settings_str} }};\n",
                    processed[0],
                )

        result.extend(processed)
        in_settings = False
        block = []

    result.extend(block)

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(result)

    print(f"Updated signing in {path}")


if __name__ == "__main__":
    main()
