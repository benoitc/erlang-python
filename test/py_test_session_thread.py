"""Starts a thread at import time: a template importing it cannot be forked."""

import threading
import time

threading.Thread(target=time.sleep, args=(60,), name='import-time-thread', daemon=True).start()
