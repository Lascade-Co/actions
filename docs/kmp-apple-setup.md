# KMP Staging Apple setup record

Checked on 2026-10-23 through the signed-in Chrome plugin, using `node_repl` only.
Team: Lascade LLC, `ZG6QASWQ43`. The user explicitly approved the Debug-only
changes below after the initial approval review rejection. No production records
were changed; no certificate was created or revoked. No private material was logged.

## Verified source and portal state

| Target | Bundle ID | Source entitlements | Portal state |
|---|---|---|---|
| App | `com.travelanimator.routemap.debug` | App Groups, Push Notifications, Sign in with Apple, Associated Domains | Explicit App ID exists; all four capabilities enabled; selected App Group is exactly `group.com.travelanimator.routemap.debug`. |
| Share | `com.travelanimator.routemap.debug.MapLinkShareExtension` | App Group | Explicit App ID exists; selected App Group is exactly `group.com.travelanimator.routemap.debug`. |
| NSE | `com.travelanimator.routemap.debug.NotificationServiceExtension` | No entitlements file configured | Created explicit App ID named `TA Staging Notification Service Extension`; verified in identifier list. |
| Widget | `com.travelanimator.routemap.debug.ExportActivityWidget` | No entitlements file configured | Created explicit App ID named `TA Staging Export Activity Widget`; verified in identifier list. |

Source checked: `iosApp/iosApp/Config/Debug/App.entitlements`,
`iosApp/MapLinkShareExtension/MapLinkShareExtension.entitlements`, and target
`CODE_SIGN_ENTITLEMENTS` / `PRODUCT_BUNDLE_IDENTIFIER` settings in
`iosApp/iosApp.xcodeproj/project.pbxproj`.

Created [Travel Animator Debug](https://appstoreconnect.apple.com/apps/6826126545/distribution),
App Store Connect ID `6826126545`, iOS, primary language English (U.S.), bundle ID
`com.travelanimator.routemap.debug`, SKU `travelanimator-debug`. Selected Limited
Access; no additional users were selected. Created internal testing group
**Staging Internal**, ID `1d5951ae-f522-4742-b266-19a16b91a21c`, with **Enable
automatic distribution** checked at creation. The created group page confirms
0 testers and 0 builds. No tester was invited and nothing was submitted for review.

The certificate list contains two valid non-managed Distribution certificates,
with expirations **2027-03-07** and **2027-04-01**. With the user's instruction
to verify it directly, authenticated Infisical CLI export confirmed
`IOS_CERTIFICATE_BASE64` and `IOS_CERTIFICATE_PASSWORD` exist in project
`travelanimator`, environment `staging`, path `/Build`. OpenSSL extracted only
the `.p12` certificate expiry: **2027-04-01 17:53:55 GMT**, matching the existing
portal Distribution certificate expiring **2027-04-01**. The existing PKCS12 uses
a legacy algorithm; OpenSSL's read-only `-legacy` decoder was necessary. The
private temporary directory and mode-0600 decoded file were removed immediately.
Secret export, password and certificate bytes were captured privately and never
printed. No certificate was created, downloaded from Apple, revoked, or changed.

The account page also displays a notice that the account holder must accept the
updated Apple Developer Program License Agreement by **2026-10-29** to maintain
access. No agreement was accepted.

## Created and downloaded profiles; user upload pending

Created all four explicit **App Store** distribution profiles using only the
existing certificate matched above. Each generated confirmation page verified the
exact profile name, bundle ID, App Store type, and expiry **2027-04-01**.

| Profile name | Portal profile ID | Bundle ID | Infisical key to be uploaded by the user |
|---|---|---|---|
| `TA Staging App` | `QMP7A26AB4` | `com.travelanimator.routemap.debug` | `IOS_STAGING_PROFILE_APP_BASE64` |
| `TA Staging Share` | `BYN45Y5A83` | `com.travelanimator.routemap.debug.MapLinkShareExtension` | `IOS_STAGING_PROFILE_SHARE_BASE64` |
| `TA Staging NSE` | `S8744GJ874` | `com.travelanimator.routemap.debug.NotificationServiceExtension` | `IOS_STAGING_PROFILE_NSE_BASE64` |
| `TA Staging Widget` | `7Q8924H99R` | `com.travelanimator.routemap.debug.ExportActivityWidget` | `IOS_STAGING_PROFILE_WIDGET_BASE64` |

Created `~/Downloads/ci-apple-setup-2026-10-23-private/` with mode 0700. The Chrome
plugin cannot choose a download destination, and its URL policy blocks inspection
of `chrome://settings/downloads`; no workaround was used. The user then explicitly
approved **"Use Downloads, then move"**, allowing the existing Chrome Downloads
directory as a temporary landing location. Each existing profile was downloaded
through the Chrome plugin, immediately moved into the private directory, and set
to mode 0600. No profile remains at the top level of Downloads.

| Private filename | Verified profile name |
|---|---|
| `TA_Staging_App.mobileprovision` | `TA Staging App` |
| `TA_Staging_Share.mobileprovision` | `TA Staging Share` |
| `TA_Staging_NSE.mobileprovision` | `TA Staging NSE` |
| `TA_Staging_Widget.mobileprovision` | `TA Staging Widget` |

The same private directory also contains four unwrapped base64 text files, mode
0600, ready for the user to paste into the corresponding Infisical secret value:

| Private text file | Infisical key |
|---|---|
| `IOS_STAGING_PROFILE_APP_BASE64.txt` | `IOS_STAGING_PROFILE_APP_BASE64` |
| `IOS_STAGING_PROFILE_SHARE_BASE64.txt` | `IOS_STAGING_PROFILE_SHARE_BASE64` |
| `IOS_STAGING_PROFILE_NSE_BASE64.txt` | `IOS_STAGING_PROFILE_NSE_BASE64` |
| `IOS_STAGING_PROFILE_WIDGET_BASE64.txt` | `IOS_STAGING_PROFILE_WIDGET_BASE64` |

Each text file decodes byte-for-byte to its verified `.mobileprovision` original;
SHA-256 equality was checked privately. The files contain no line wrapping or
trailing newline. Set each Infisical value to the complete content of its matching
text file, under project `travelanimator` / environment `staging` / path `/Build`.
Do not use the filename or binary profile itself as the secret value. No secret
values were printed and no Infisical write was performed.

CMS contents were decoded privately with `security cms`; only safe metadata was
printed. All four downloaded profiles match their exact names and bundle IDs in
the table above, are App Store profiles without development-device lists or
debugger entitlement, and expire **2027-04-01 17:53:55 GMT**. No certificate or
profile contents were printed. Apple creation and profile download work is
complete; Infisical upload is still explicitly the user's next step.

The user uploads these profiles to Infisical project `travelanimator`, environment
`staging`, path `/Build`, then confirms completion. The existing `.p12` and password
have already been verified against the selected certificate. The first hosted
build must wait for profile upload confirmation. Production identifiers, groups, profiles, and the
production app record must remain untouched. Stop at any warning that would
invalidate production profiles.

## Approval review history

Automatic approval review rejected the final **Register** action for
`com.travelanimator.routemap.debug.NotificationServiceExtension`: registering the
App ID creates persistent Apple Developer account state, and the broad
implementation request did not clearly authorize this exact external
registration. The identifier form was prepared but registration did not happen
until the user later explicitly approved this exact Debug-only setup.

While finishing the inventory, opening App Store Connect's **New App** form was
also rejected: it was deemed a consequential creation path outside the
subsequent read-only inventory scope. After the user explicitly approved the listed
actions, both flows succeeded through the same Chrome plugin. No rejection was
bypassed through another tool or route.

The agent-created App Store Connect tab was closed and the existing user-owned
Apple Developer tab was restored to its original account page.
