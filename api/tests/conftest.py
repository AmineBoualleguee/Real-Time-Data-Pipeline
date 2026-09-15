import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Most route tests exercise response shape/status, not auth - keep it disabled
# by default so they don't all need to carry an X-API-Key header. test_auth.py
# overrides app.auth.API_KEY directly to test the enforcement path itself.
os.environ["API_KEY"] = ""
