"""Generate local credentials once, without printing or overwriting them."""
import os
import secrets
from pathlib import Path

target = Path(__file__).resolve().parents[1] / ".env"
try:
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    print(".env already exists; left unchanged.")
else:
    with os.fdopen(fd, "w") as out:
        out.write(f"POSTGRES_PASSWORD={secrets.token_hex(24)}\n")
        out.write(f"CDC_PASSWORD={secrets.token_hex(24)}\n")
    print("Created .env with local credentials (mode 0600).")
