```python
class SmartctlWorker(QThread):
    finished = Signal(str)
    error = Signal(str)

    def run(self) -> None:
        script_path = None
        try:
            # choose elevation method
            if os.geteuid() == 0:
                elev = []
            elif shutil.which("pkexec"):
                elev = ["pkexec"]
            else:
                elev = ["sudo"]

            # script: list /dev paths and run smartctl -a once for each device
            shell_script = (
                "#!/bin/sh\n"
                "set -e\n"
                "smartctl --scan | awk '/^\\/dev/{print $1}' | while read dev; do\n"
                "  printf '%s\\n' '============================================================'\n"
                "  printf '%s\\n' \"$dev\"\n"
                "  printf '%s\\n' '============================================================'\n"
                "  smartctl -a \"$dev\" 2>&1\n"
                "done\n"
            )

            import tempfile
            with tempfile.NamedTemporaryFile("w", delete=False, prefix="filesage_smartctl_", suffix=".sh") as tf:
                tf.write(shell_script)
                script_path = tf.name

            os.chmod(script_path, 0o700)

            # Run the script once under elevation so the user authenticates a single time
            proc = subprocess.run(elev + [script_path], capture_output=True, text=True)
            output = (proc.stdout or "") + (proc.stderr or "")

            if proc.returncode != 0 and not output:
                raise RuntimeError("elevated smartctl run failed")

            self.finished.emit(output)

        except Exception as exc:
            self.error.emit(str(exc))

        finally:
            if script_path:
                try:
                    os.remove(script_path)
                except Exception:
                    pass

