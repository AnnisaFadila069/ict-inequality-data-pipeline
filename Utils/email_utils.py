import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def load_env_auto():
    for parent in Path(__file__).resolve().parents:
        env_path = parent / ".env"
        if env_path.exists():
            env = {}
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    env[key.strip()] = value.strip().strip('"').strip("'")
            return env
    raise FileNotFoundError(".env not found")


def send_email(to: str, subject: str, body: str, env: dict = None):
    if env is None:
        env = load_env_auto()

    host     = env.get("EMAIL_SMTP_HOST")
    port     = int(env.get("EMAIL_SMTP_PORT", 587))
    sender   = env.get("EMAIL_FROM_ADDRESS")
    password = env.get("EMAIL_SMTP_PASSWORD")

    for key, val in [("EMAIL_SMTP_HOST", host), ("EMAIL_FROM_ADDRESS", sender), ("EMAIL_SMTP_PASSWORD", password)]:
        if not val:
            raise ValueError(f"Missing {key} in .env")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = to
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(host, port) as server:
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, to, msg.as_string())
