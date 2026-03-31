# Integration guide: external bot adapter

This document explains the optional integration that replaces the simulated
message-sending path with a thin adapter that delegates to an external bot
implementation placed under `adapters/integrations` (for example, GramAddict).

What changed
- New file: `adapters/integrations/adapter.py` exposes `send_message(account, recipient, text, options)`.
- The main send flow calls the adapter and falls back cleanly if the external bot is not available or fails.

How to use
1. Put the external bot implementation under `adapters/integrations/`.
   Example structure: `adapters/integrations/gramaddict.py` and `adapters/integrations/requirements.txt`.
2. Ensure the required dependencies are installed in your virtual environment:

```powershell
pip install -r adapters/integrations/requirements.txt
```

3. Run the smoke harness to exercise the send path:

```powershell
python .\scripts\integration_send_smoke.py
```

Windows notes
- Use PowerShell and the project's virtual environment.
- Python path handling accepts either slash style, but commands in this repo are documented with Windows-style paths.

Rollback behavior
- The adapter is defensive. If the import fails, it returns `(False, "External bot not available")`.
- No storage migrations are performed by this integration layer.

Customization
- If your external bot exposes different function names than the scaffold
  (for example `send_dm` instead of `send_direct`), update
  `adapters/integrations/adapter.py` to map the calls accordingly.
