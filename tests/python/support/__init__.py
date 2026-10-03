"""Shared paths and imports for the Python test suite."""
import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "scripts"
SUITE = ROOT / "tests" / "python"


def load_script(filename):
    """Import an operational tool without executing its command-line entrypoint."""
    name = "hubuum_tool_" + Path(filename).stem.replace("-", "_")
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load repository tool: {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[name]
        raise
    return module
