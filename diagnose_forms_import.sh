#!/usr/bin/env bash
# diagnose_forms_import.sh
# Quick diagnostic for the "cannot import name 'generate_forms_docs'" error.
# Run from project root (LEO-Ai_Layer/ai-business-intelligence/)

set -e

echo "============================================================"
echo "  Forms domain — import error diagnostic"
echo "============================================================"

FORMS_PY=app/services/doc_generators/domains/forms.py
INIT_PY=app/services/doc_generators/__init__.py

# 1. Confirm the file exists at all
if [ ! -f "$FORMS_PY" ]; then
    echo "✗ File missing: $FORMS_PY"
    echo "  → Copy the shipped forms.py to that path."
    exit 1
fi

# 2. Confirm the function is defined in the source
if grep -q "^async def generate_forms_docs" "$FORMS_PY"; then
    LINE=$(grep -n "^async def generate_forms_docs" "$FORMS_PY" | head -1)
    echo "✓ generate_forms_docs defined at $FORMS_PY:$LINE"
else
    echo "✗ generate_forms_docs NOT defined in $FORMS_PY"
    echo "  The file on disk is the wrong version."
    echo "  → Re-copy the shipped forms.py to that path."
    exit 1
fi

# 3. Confirm __all__ exports it
if grep -q '"generate_forms_docs"' "$FORMS_PY"; then
    echo "✓ generate_forms_docs in __all__"
else
    echo "⚠ generate_forms_docs missing from __all__ — adding will help"
fi

# 4. Confirm __init__.py imports it correctly
if grep -q "from app.services.doc_generators.domains.forms import generate_forms_docs" "$INIT_PY"; then
    LINE=$(grep -n "from app.services.doc_generators.domains.forms import" "$INIT_PY" | head -1)
    echo "✓ __init__.py imports generate_forms_docs at $INIT_PY:$LINE"
else
    echo "⚠ __init__.py doesn't have the expected import line"
    echo "  Recheck doc_generator_forms_patch.py edits"
fi

# 5. Try importing the module standalone (bypassing __init__.py)
echo ""
echo "── Standalone import test ──────────────────────────────"
python3 -c "
import sys, importlib.util
spec = importlib.util.spec_from_file_location('forms_test', '$FORMS_PY')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print('✓ Loaded forms.py standalone')
print('  has generate_forms_docs:', hasattr(mod, 'generate_forms_docs'))
print('  has CHUNK_GENERATORS keys:', list(mod.CHUNK_GENERATORS.keys()) if hasattr(mod, 'CHUNK_GENERATORS') else 'MISSING')
"

# 6. Try the package import the way the test does it
echo ""
echo "── Package import test (simulates the test harness) ────"
python3 -c "
from app.services.doc_generators.domains.forms import generate_forms_docs, CHUNK_GENERATORS
print('✓ Package import works')
print('  doc_types:', list(CHUNK_GENERATORS.keys()))
" || echo "✗ Package import failed — see error above"

# 7. Nuke stale .pyc files (the most common cause when "it worked yesterday")
echo ""
echo "── Clearing stale .pyc files ───────────────────────────"
find app -name '*.pyc' -delete 2>/dev/null && echo "✓ .pyc files deleted"
find app -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null && echo "✓ __pycache__ directories cleared"

# 8. Retry the import
echo ""
echo "── Retry package import after cache clear ─────────────"
python3 -c "
from app.services.doc_generators.domains.forms import generate_forms_docs, CHUNK_GENERATORS
print('✓ Package import works after cache clear')
print('  doc_types:', list(CHUNK_GENERATORS.keys()))
" && echo "" && echo "✓ FIXED — re-run Step 5 verification now"