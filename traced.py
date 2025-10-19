# pipeline.py
import logging, time
log = logging.getLogger("pipeline")

class traced:
    def __init__(self, stage): self.stage = stage
    def __enter__(self): log.info(f"▶ {self.stage}"); self.t0=time.time(); return self
    def __exit__(self, et, ev, tb):
        if et:
            log.exception(f"✖ {self.stage} failed")  # traceback!
            return False  # DO NOT swallow
        log.info(f"✓ {self.stage} ok in {time.time()-self.t0:.3f}s")