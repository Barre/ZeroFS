"""Back-compat alias for the low-level uniffi module.

The generated bindings ship inside the ``zerofs`` package (as
``zerofs._zerofs_ffi``) so a single ``py.typed`` covers them; this re-export
keeps ``import zerofs_ffi`` working for code that wants the raw surface.
"""

from zerofs._zerofs_ffi import *  # noqa: F401,F403
