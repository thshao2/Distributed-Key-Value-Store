import sys
import subprocess
from typing import List, Dict, Any, Optional


def log(*args):
    # log to stderr
    print(*args, file=sys.stderr)


def get_logger(prefix: str):
    return lambda *args: log(f"{prefix}: ", *args)


def run_cmd_bg(
    cmd: list[str], verbose=False, error_prefix: str = "command failed", **kwargs
) -> subprocess.CompletedProcess:
    # default capture opts
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("text", True)
    kwargs.setdefault("check", True)

    if verbose:
        log(f"$ {cmd[0]} {' '.join(cmd[1:])}")

    try:
        return subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{error_prefix}: {e}:\nstdout: {e.stdout}\nstderr: {e.stderr}"
        )
