# Integration guide: external bot adapter

This document explains the small integration that replaces the simulated
message-sending path with a thin adapter that delegates to an external bot
implementation placed under the `integraciones` folder (for example,
GramAddict).

What changed
- New file: `integraciones/adapter.py` — exposes `send_message(account, recipient, text, options)`.
- The main send flow is updated to call the adapter and fall back to the
  simulator if the adapter is not available or fails.

How to use
1. Put the external bot implementation under `integraciones/`.
   Example structure: `integraciones/gramaddict.py` and `integraciones/requirements.txt`.
2. Ensure required dependencies are installed in your virtual environment:

```powershell
pip install -r integraciones/requirements.txt
```

3. Run the test harness to exercise the send path (PowerShell):

```powershell
python .\tools\test_integration_send.py
```

Windows notes
- Use PowerShell and the project's virtual environment. Paths in Windows
  should use backslashes when typing commands manually but Python path
  handling uses forward/backward slashes transparently.

Rollbacks
- The adapter is defensive. If import fails, the adapter returns a clear
  (False, "External bot not available") response and the menu falls back to
  the simulator. No data migrations are performed.

Contact
- If your external bot exposes different function names than the scaffold
  (e.g. `send_dm` instead of `send_direct`), update `integraciones/adapter.py`
  to map the calls accordingly.
