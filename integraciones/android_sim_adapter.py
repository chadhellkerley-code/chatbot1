# integraciones/android_sim_adapter.py
import os, subprocess, threading

class AndroidSimAdapter:
    def __init__(self, account_name: str = "myaccount"):
        self.account_name = account_name
        self.proc = None

    def start_session(self, args=None):
        args = args or []
        cmd = ["gramaddict", "run", self.account_name] + args
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        threading.Thread(target=self._read_output, daemon=True).start()
        print(f"✅ GramAddict iniciado: {' '.join(cmd)}")

    def _read_output(self):
        for line in self.proc.stdout:
            print("[GA]", line.strip())
        for line in self.proc.stderr:
            print("[GA-ERR]", line.strip())

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            print("⛔ Sesión GramAddict detenida.")
