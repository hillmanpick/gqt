"""GQT's isolated vn.py research integration."""

from pathlib import Path


# vn.py resolves its database directory from the current working directory
# when this marker exists; keep research data out of the user's home directory.
Path.cwd().joinpath(".vntrader").mkdir(exist_ok=True)

__version__ = "0.1.0"
