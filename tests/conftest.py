"""Root conftest — shared pytest configuration for all tests."""
import sys
from pathlib import Path

# Make `src/` importable via `from service.xxx import ...` and `from src.xxx import ...`
_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root / "src"))
