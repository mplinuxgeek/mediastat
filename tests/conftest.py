import sys
from pathlib import Path

# Add the app directory to sys.path so tests can import modules directly
app_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(app_dir))
