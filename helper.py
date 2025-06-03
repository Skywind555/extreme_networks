import os
from dotenv import load_dotenv

load_dotenv()

def get(var: str, required: bool = True) -> str | None:
    val = os.getenv(var)
    if required and val is None:
        raise RuntimeError(f"Environment variable '{var}' not set")
    return val
