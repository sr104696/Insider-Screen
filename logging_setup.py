# logging_setup.py
import logging, sys

root = logging.getLogger()
if root.handlers:  # avoid double logging
    for h in list(root.handlers): root.removeHandler(h)

handler = logging.StreamHandler(sys.stderr)
fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
handler.setFormatter(fmt)
root.addHandler(handler)
root.setLevel(logging.DEBUG)

def critical(msg, *args, **kw):
    # enforce tracebacks everywhere CRITICAL is used
    kw.setdefault("exc_info", True)
    logging.getLogger("app").critical(msg, *args, **kw)