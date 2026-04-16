#!/usr/bin/env python3
"""
Switch an Xcode project to manual code signing and assign per-target
provisioning profiles.

Required environment variables:
    APP_PROFILE_NAME  – Provisioning profile name for the main app target
    NSE_PROFILE_NAME  – Provisioning profile name for the NSE target

Optional:
    LIVE_ACTIVITY_PROFILE_NAME – Provisioning profile name for the LiveActivityWidget target
    PBXPROJ_PATH               – Path to project.pbxproj (default: ios/Runner.xcodeproj/project.pbxproj)

Usage:
    python3 fix_ios_signing.py
"""

import os
import re
import sys


def main():
    path = os.environ.get("PBXPROJ_PATH", "ios/Runner.xcodeproj/project.pbxproj")
    app_name = os.environ.get("APP_PROFILE_NAME")
    nse_name = os.environ.get("NSE_PROFILE_NAME")
    live_activity_name = os.environ.get("LIVE_ACTIVITY_PROFILE_NAME")
    team_id = os.environ.get("IOS_TEAM_ID")

    if not app_name or not nse_name:
        print("ERROR: APP_PROFILE_NAME and NSE_PROFILE_NAME must be set", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(path):
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)

    with open(path) as f:
        lines = f.readlines()

    in_settings = False
    depth = 0
    block = []
    is_nse = False
    is_live_activity = False
    result = []

    for line in lines:
        if not in_settings:
            if "buildSettings = {" in line:
                in_settings = True
                depth = line.count("{") - line.count("}")
                block = [line]
                is_nse = False
                is_live_activity = False
                continue
            result.append(line)
        else:
            depth += line.count("{") - line.count("}")
            block.append(line)
            if "OneSignalNotificationServiceExtension" in line:
                is_nse = True
            if "LiveActivityWidget" in line:
                is_live_activity = True
            if depth <= 0:
                # Only modify target build configs (those with PRODUCT_BUNDLE_IDENTIFIER)
                is_target = any("PRODUCT_BUNDLE_IDENTIFIER" in bl for bl in block)

                if is_target:
                    if is_nse:
                        name = nse_name
                    elif is_live_activity and live_activity_name:
                        name = live_activity_name
                    else:
                        name = app_name
                    has_specifier = False
                    processed = []

                    for bl in block:
                        # Replace CODE_SIGN_STYLE
                        if "CODE_SIGN_STYLE" in bl:
                            bl = re.sub(
                                r"CODE_SIGN_STYLE = \w+",
                                "CODE_SIGN_STYLE = Manual",
                                bl,
                            )
                        # Replace CODE_SIGN_IDENTITY (handle any existing value)
                        if "CODE_SIGN_IDENTITY" in bl:
                            bl = re.sub(
                                r'CODE_SIGN_IDENTITY = ".*?"',
                                'CODE_SIGN_IDENTITY = "Apple Distribution"',
                                bl,
                            )
                        # Replace DEVELOPMENT_TEAM if IOS_TEAM_ID is set
                        if "DEVELOPMENT_TEAM" in bl and team_id:
                            bl = re.sub(
                                r"DEVELOPMENT_TEAM = \w+",
                                f"DEVELOPMENT_TEAM = {team_id}",
                                bl,
                            )
                        # Replace PROVISIONING_PROFILE_SPECIFIER
                        if "PROVISIONING_PROFILE_SPECIFIER" in bl:
                            has_specifier = True
                            bl = re.sub(
                                r'PROVISIONING_PROFILE_SPECIFIER = ".*?"',
                                f'PROVISIONING_PROFILE_SPECIFIER = "{name}"',
                                bl,
                            )
                        processed.append(bl)

                    if not has_specifier:
                        # Detect indentation from existing settings
                        indent = "\t\t\t\t"
                        for bl in processed:
                            m = re.match(r"^(\s+)\w", bl)
                            if m and "buildSettings" not in bl:
                                indent = m.group(1)
                                break
                        closing = processed.pop()
                        processed.append(
                            f'{indent}PROVISIONING_PROFILE_SPECIFIER = "{name}";\n'
                        )
                        processed.append(closing)

                    result.extend(processed)
                else:
                    result.extend(block)

                in_settings = False
                block = []

    result.extend(block)

    with open(path, "w") as f:
        f.writelines(result)

    print(f"Updated signing in {path}")


if __name__ == "__main__":
    main()