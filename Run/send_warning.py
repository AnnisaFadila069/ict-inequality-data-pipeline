import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from Utils.email_utils import load_env_auto, send_email


def main():
    env       = load_env_auto()
    recipient = env.get("EMAIL_FROM_ADDRESS")

    if not recipient:
        print("[ERROR] EMAIL_FROM_ADDRESS not set in .env")
        sys.exit(1)

    subject = "[ct-inequality-data-pipeline] Reminder: Pipeline will run in 2 hours"
    body = (
        "Hello,\n\n"
        "This is an automated reminder.\n\n"
        "The Tugas Akhir data pipeline is scheduled to run at 10:00 AM today.\n"
        "Please make sure:\n"
        "  - The computer remains on\n"
        "  - Internet connection is available\n"
        "  - The latest data files are in the Data Raw folder\n\n"
        "This email was sent automatically by the scheduler.\n"
    )

    try:
        send_email(recipient, subject, body, env)
        print(f"[OK] Warning email sent to {recipient}")
    except Exception as e:
        print(f"[ERROR] Failed to send warning email: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
