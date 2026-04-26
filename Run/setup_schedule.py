"""
Baca TYPE, PIPELINE_RUN_TIME, dan WARNING_HOURS_BEFORE dari .env,
lalu update Task Scheduler Windows secara otomatis.

Jalankan ulang script ini setiap kali mengubah jadwal di .env.
"""

import os
import sys
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

ALL_MONTHS = "".join(f"      <{m}/>\n" for m in MONTH_NAMES[1:])


def load_env():
    env_path = ROOT / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f".env not found at {env_path}")
    env = {}
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def parse_pipeline_time(raw: str) -> datetime:
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M")
    except ValueError:
        raise ValueError(
            f"Format PIPELINE_RUN_TIME salah: '{raw}'\n"
            "Gunakan format: YYYY-MM-DD HH:MM  (contoh: 2026-04-27 10:00)"
        )


def build_trigger(schedule_type: str, dt: datetime) -> str:
    start = dt.strftime("%Y-%m-%dT%H:%M:%S")
    t = schedule_type.lower()

    if t == "daily":
        return (
            f"    <CalendarTrigger>\n"
            f"      <StartBoundary>{start}</StartBoundary>\n"
            f"      <Enabled>true</Enabled>\n"
            f"      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>\n"
            f"    </CalendarTrigger>"
        )
    elif t == "monthly":
        day = dt.day
        return (
            f"    <CalendarTrigger>\n"
            f"      <StartBoundary>{start}</StartBoundary>\n"
            f"      <Enabled>true</Enabled>\n"
            f"      <ScheduleByMonth>\n"
            f"        <DaysOfMonth><Day>{day}</Day></DaysOfMonth>\n"
            f"        <Months>\n"
            f"{ALL_MONTHS}"
            f"        </Months>\n"
            f"      </ScheduleByMonth>\n"
            f"    </CalendarTrigger>"
        )
    elif t == "yearly":
        day   = dt.day
        month = MONTH_NAMES[dt.month]
        return (
            f"    <CalendarTrigger>\n"
            f"      <StartBoundary>{start}</StartBoundary>\n"
            f"      <Enabled>true</Enabled>\n"
            f"      <ScheduleByMonth>\n"
            f"        <DaysOfMonth><Day>{day}</Day></DaysOfMonth>\n"
            f"        <Months><{month}/></Months>\n"
            f"      </ScheduleByMonth>\n"
            f"    </CalendarTrigger>"
        )
    else:
        raise ValueError(
            f"TYPE tidak valid: '{schedule_type}'\n"
            "Gunakan: daily | monthly | yearly"
        )


def build_xml(task_name: str, bat_path: str, trigger_xml: str, max_hours: int) -> str:
    return (
        f'<?xml version="1.0" encoding="UTF-16"?>\n'
        f'<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">\n'
        f'  <Triggers>\n'
        f'{trigger_xml}\n'
        f'  </Triggers>\n'
        f'  <Principals>\n'
        f'    <Principal id="Author">\n'
        f'      <LogonType>InteractiveToken</LogonType>\n'
        f'      <RunLevel>LeastPrivilege</RunLevel>\n'
        f'    </Principal>\n'
        f'  </Principals>\n'
        f'  <Settings>\n'
        f'    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n'
        f'    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>\n'
        f'    <ExecutionTimeLimit>PT{max_hours}H</ExecutionTimeLimit>\n'
        f'    <Enabled>true</Enabled>\n'
        f'  </Settings>\n'
        f'  <Actions>\n'
        f'    <Exec><Command>{bat_path}</Command></Exec>\n'
        f'  </Actions>\n'
        f'</Task>'
    )


def register_task(task_name: str, xml_content: str):
    with tempfile.NamedTemporaryFile(
        suffix=".xml", delete=False, mode="w", encoding="utf-16"
    ) as f:
        f.write(xml_content)
        tmp_path = f.name

    try:
        subprocess.run(["schtasks", "/delete", "/tn", task_name, "/f"], capture_output=True)
        result = subprocess.run(
            ["schtasks", "/create", "/tn", task_name, "/xml", tmp_path, "/f"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)
    finally:
        os.unlink(tmp_path)


def describe_schedule(schedule_type: str, pipeline_dt: datetime, warning_dt: datetime, warn_hours: int) -> str:
    t = schedule_type.lower()
    if t == "daily":
        freq = f"setiap hari pukul {pipeline_dt.strftime('%H:%M')}"
        warn = f"setiap hari pukul {warning_dt.strftime('%H:%M')} ({warn_hours} jam sebelum pipeline)"
    elif t == "monthly":
        freq = f"setiap bulan tanggal {pipeline_dt.day} pukul {pipeline_dt.strftime('%H:%M')}"
        warn = f"setiap bulan tanggal {warning_dt.day} pukul {warning_dt.strftime('%H:%M')} ({warn_hours} jam sebelum pipeline)"
    else:
        freq = f"setiap tahun pada {pipeline_dt.strftime('%d %B')} pukul {pipeline_dt.strftime('%H:%M')}"
        warn = f"setiap tahun pada {warning_dt.strftime('%d %B')} pukul {warning_dt.strftime('%H:%M')} ({warn_hours} jam sebelum pipeline)"
    return freq, warn


def main():
    env = load_env()

    raw_type     = env.get("TYPE", "daily")
    raw_run_time = env.get("PIPELINE_RUN_TIME")
    raw_warn_hrs = env.get("WARNING_HOURS_BEFORE")

    if not raw_run_time:
        print("[ERROR] PIPELINE_RUN_TIME tidak ditemukan di .env")
        sys.exit(1)
    if not raw_warn_hrs:
        print("[ERROR] WARNING_HOURS_BEFORE tidak ditemukan di .env")
        sys.exit(1)

    try:
        pipeline_dt = parse_pipeline_time(raw_run_time)
        warn_hours  = int(raw_warn_hrs)
    except (ValueError, TypeError) as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    warning_dt = pipeline_dt - timedelta(hours=warn_hours)

    run_bat  = str(ROOT / "Run" / "run_all.bat")
    warn_bat = str(ROOT / "Run" / "send_warning.bat")

    try:
        pipeline_trigger = build_trigger(raw_type, pipeline_dt)
        warning_trigger  = build_trigger(raw_type, warning_dt)
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    freq_desc, warn_desc = describe_schedule(raw_type, pipeline_dt, warning_dt, warn_hours)

    print("=" * 60)
    print("  SETUP TASK SCHEDULER")
    print("=" * 60)
    print(f"  Tipe      : {raw_type.upper()}")
    print(f"  Pipeline  : {freq_desc}")
    print(f"  Peringatan: {warn_desc}")
    print()

    try:
        xml_pipeline = build_xml("TA_RunAllPipelines",  run_bat,  pipeline_trigger, 72)
        xml_warning  = build_xml("TA_SendWarningEmail", warn_bat, warning_trigger,  1)

        register_task("TA_RunAllPipelines",  xml_pipeline)
        print(f"  [OK] TA_RunAllPipelines  -> {freq_desc}")

        register_task("TA_SendWarningEmail", xml_warning)
        print(f"  [OK] TA_SendWarningEmail -> {warn_desc}")
    except RuntimeError as e:
        print(f"  [ERROR] Gagal mendaftarkan task: {e}")
        sys.exit(1)

    print()
    print("  Jadwal berhasil diperbarui.")
    print("=" * 60)


if __name__ == "__main__":
    main()
