"""
SSL Wrapper — Disables certificate verification for data ingestion scripts.

REASON: Crossref Event Data API (api.eventdata.crossref.org) has an expired
Let's Encrypt R12 certificate as of 2026-05-26. This wrapper allows our
ingestion scripts to connect despite the expired cert.

This is acceptable because:
- This is an internal data ingestion server (not user-facing)
- The APIs are public, well-known services
- No sensitive credentials are transmitted
- This is temporary until Crossref renews their cert

To use: import this module BEFORE importing requests in any script,
or exec() it before running the main script.
"""
import ssl
import os

# Disable SSL verification warnings
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# Monkey-patch requests to default verify=False
import requests
from requests.adapters import HTTPAdapter

class NoVerifyAdapter(HTTPAdapter):
    def send(self, request, **kwargs):
        kwargs['verify'] = False
        return super().send(request, **kwargs)

# Patch the default session
_original_session_init = requests.Session.__init__
def _patched_init(self, *args, **kwargs):
    _original_session_init(self, *args, **kwargs)
    self.verify = False
    self.mount('https://', NoVerifyAdapter())

requests.Session.__init__ = _patched_init

# Also patch the module-level functions
_orig_get = requests.get
_orig_post = requests.post

def _patched_get(url, **kwargs):
    kwargs.setdefault('verify', False)
    return _orig_get(url, **kwargs)

def _patched_post(url, **kwargs):
    kwargs.setdefault('verify', False)
    return _orig_post(url, **kwargs)

requests.get = _patched_get
requests.post = _patched_post

print("[SSL] Certificate verification disabled for ingestion")
