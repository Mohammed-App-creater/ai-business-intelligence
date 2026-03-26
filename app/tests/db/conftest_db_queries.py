# conftest.py  (project root — ai-business-intelligence/conftest.py)
#
# This file ensures the project root is always on sys.path,
# so imports like `from app.services.llm...` work in every test.
#
# Place this file at the ROOT of the project, NOT inside app/tests/.
# With pytest.ini having `pythonpath = .` this is usually enough on its own,
# but this file acts as a belt-and-suspenders guarantee.

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))