import subprocess
import sys
import os
import datetime

from email_utils import load_env_auto, send_email

TASKS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tasks")
PYTHON    = sys.executable
LOG_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

PIPELINES = [
    ("1_cleansing.py",        "1. Cleansing"),
    ("2_standardization.py",  "2. Standardization"),
    ("3_enrichment.py",       "3. Enrichment"),
    ("4_load.py",             "4. Load"),
    ("5_load_to_onedrive.py", "5. Load to OneDrive"),
]


def _log(msg, log_file=None):
    print(msg)
    if log_file:
        log_file.write(msg + "\n")
        log_file.flush()


def run_pipeline(script, name, log_file=None):
    _log(f"\n{'=' * 60}", log_file)
    _log(f"  RUNNING: {name}", log_file)
    _log(f"{'=' * 60}", log_file)

    result = subprocess.run([PYTHON, script], cwd=TASKS_DIR)

    if result.returncode != 0:
        _log(f"\n[ERROR] {name} failed (exit code {result.returncode})", log_file)
        return False

    _log(f"[OK] {name} completed", log_file)
    return True


def notify_success(env, start, end, elapsed, log_path):
    to_addr   = env.get("EMAIL_TO_ADDRESS")
    from_addr = env.get("EMAIL_FROM_ADDRESS")

    recipients = list({addr for addr in [to_addr, from_addr] if addr})
    if not recipients:
        print("[WARN] No recipients set, skipping success email notification")
        return

    subject = "[ct-inequality-data-pipeline] All pipelines completed successfully"
    body = (
        "Hello,\n\n"
        "All Tugas Akhir data pipelines have completed successfully.\n\n"
        f"  Started  : {start.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  Finished : {end.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  Elapsed  : {elapsed}\n\n"
        "Pipelines executed:\n"
        + "".join(f"  - {name}\n" for _, name in PIPELINES)
        + f"\nLog saved at: {log_path}\n\n"
        "This email was sent automatically by the scheduler.\n"
    )

    for recipient in recipients:
        try:
            send_email(recipient, subject, body, env)
            print(f"[OK] Success email sent to {recipient}")
        except Exception as e:
            print(f"[WARN] Failed to send success email to {recipient}: {e}")


def notify_failure(env, failed_pipeline, start, log_path):
    to = env.get("EMAIL_FROM_ADDRESS")
    if not to:
        print("[WARN] EMAIL_FROM_ADDRESS not set, skipping failure email notification")
        return

    subject = "[ct-inequality-data-pipeline] FAILED: Pipeline stopped"
    body = (
        "WARNING!\n\n"
        "The Tugas Akhir data pipeline encountered an error and was stopped.\n\n"
        f"  Failed pipeline : {failed_pipeline}\n"
        f"  Started         : {start.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  Failed at       : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"Log saved at: {log_path}\n\n"
        "Please check the log file for error details.\n\n"
        "This email was sent automatically by the scheduler.\n"
    )

    try:
        send_email(to, subject, body, env)
        print(f"[OK] Failure email sent to {to}")
    except Exception as e:
        print(f"[WARN] Failed to send failure email: {e}")


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path  = os.path.join(LOG_DIR, f"run_{timestamp}.log")

    env = load_env_auto()

    with open(log_path, "w", encoding="utf-8") as log_file:
        start = datetime.datetime.now()
        header = (
            f"{'=' * 60}\n"
            f"  RUN ALL PIPELINES\n"
            f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'=' * 60}"
        )
        _log(header, log_file)

        for script, name in PIPELINES:
            success = run_pipeline(script, name, log_file)
            if not success:
                _log(f"\nPipeline aborted at: {name}", log_file)
                _log(f"Log saved to: {log_path}", log_file)
                notify_failure(env, name, start, log_path)
                sys.exit(1)

        end     = datetime.datetime.now()
        elapsed = end - start
        footer  = (
            f"\n{'=' * 60}\n"
            f"  ALL PIPELINES COMPLETED\n"
            f"  Finished: {end.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"  Elapsed : {elapsed}\n"
            f"{'=' * 60}"
        )
        _log(footer, log_file)
        _log(f"Log saved to: {log_path}")

    notify_success(env, start, end, elapsed, log_path)


if __name__ == "__main__":
    main()
