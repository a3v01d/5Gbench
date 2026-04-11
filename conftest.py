"""pytest configuration — add src/ to sys.path so tests can import fivegbench."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
