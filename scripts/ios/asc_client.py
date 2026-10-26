"""App Store Connect plumbing shared by the iOS publishers and the read-only status report.

ES256 API-key JWTs are minted per request (never printed): processing can outlast one token.
Env: APPSTORE_API_KEY_ID, APPSTORE_ISSUER_ID, APPSTORE_API_PRIVATE_KEY. PyJWT[crypto] is
imported lazily so offline fake-client tests need neither the library nor credentials.
API contracts: https://developer.apple.com/documentation/appstoreconnectapi
"""
import json
import os
import plistlib
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

API = 'https://api.appstoreconnect.apple.com/v1/'
CREDENTIALS = ('APPSTORE_API_KEY_ID', 'APPSTORE_ISSUER_ID', 'APPSTORE_API_PRIVATE_KEY')


class ApiError(RuntimeError):
    def __init__(self, status, message, errors=()):
        self.status = status
        self.errors = list(errors)  # Apple's error objects: code, title, detail
        super().__init__(message)


def token():
    import jwt
    missing = [key for key in CREDENTIALS if not os.environ.get(key)]
    if missing:
        raise ValueError('Missing GitHub secrets: ' + ', '.join(missing))
    now = int(time.time())
    return jwt.encode({'iss': os.environ['APPSTORE_ISSUER_ID'], 'iat': now,
                       'exp': now + 600, 'aud': 'appstoreconnect-v1'},
                      os.environ['APPSTORE_API_PRIVATE_KEY'], algorithm='ES256',
                      headers={'kid': os.environ['APPSTORE_API_KEY_ID']})


class Client:
    def __init__(self, opener=urllib.request.urlopen):
        self.opener = opener

    def request(self, method, path, params=None, data=None):
        bearer = token()
        if '://' in path and not path.startswith(API):
            raise ValueError('App Store pagination must stay on the App Store API')
        url = path if path.startswith(API) else API + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, method=method, headers={
            'Authorization': 'Bearer ' + bearer, 'Content-Type': 'application/json'},
            data=json.dumps(data).encode() if data is not None else None)
        try:
            with self.opener(request, timeout=60) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            # Apple's error code/detail identifies missing metadata and submission blockers.
            errors = []
            try:
                errors = json.loads(error.read()).get('errors', [])
                message = '; '.join(f'{e.get("code", "API_ERROR")}: {e.get("detail") or e.get("title", "")}'
                                    for e in errors)
            except (ValueError, TypeError, AttributeError):
                errors, message = [], 'Unreadable App Store Connect error'
            finally:
                error.close()
            raise ApiError(error.code, f'App Store Connect HTTP {error.code}: {message}',
                           [e for e in errors if isinstance(e, dict)] if isinstance(errors, list) else []) from None

    def list(self, path, params=None):
        result = []
        while path:
            page = self.request('GET', path, params=params)
            result.extend(page.get('data', []))
            path, params = page.get('links', {}).get('next'), None
        return result

    def related(self, path):
        try:
            return self.request('GET', path).get('data')
        except ApiError as error:
            if error.status == 404:
                return None
            raise


def resource(kind, attributes=None, relationships=None, identifier=None):
    item = {'type': kind}
    if attributes is not None:
        item['attributes'] = attributes
    if relationships:
        item['relationships'] = {key: {'data': {'type': value[0], 'id': value[1]}}
                                 for key, value in relationships.items()}
    if identifier:
        item['id'] = identifier
    return {'data': item}


def find_app(client, bundle):
    apps = client.list('apps', {'filter[bundleId]': bundle})
    if len(apps) != 1:
        raise ValueError('Expected exactly one App Store Connect app matching the bundle ID')
    return apps[0]['id']


def find_build(client, app, version, number):
    """The one iOS build with this marketing version and build number, or None while not yet visible."""
    builds = client.list('builds', {'filter[app]': app, 'filter[version]': number,
        'filter[preReleaseVersion.version]': version, 'filter[preReleaseVersion.platform]': 'IOS',
        'limit': 200})
    if len(builds) > 1:
        raise ValueError('More than one App Store build matched the exact version/build')
    return builds[0] if builds else None


def ipa_identity(path):
    with zipfile.ZipFile(path) as archive:
        names = [n for n in archive.namelist() if re.fullmatch(r'Payload/[^/]+\.app/Info\.plist', n)]
        if len(names) != 1:
            raise ValueError('IPA must contain exactly one main app Info.plist')
        info = plistlib.loads(archive.read(names[0]))
    return tuple(str(info[k]) for k in ('CFBundleIdentifier', 'CFBundleShortVersionString', 'CFBundleVersion'))
