import time
import os
from datetime import datetime
from qfieldcloud_sdk import sdk
from qfieldcloud_sdk.sdk import JobTypes  # ← 추가

# ========== 설정 ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"
PROJECT_ID = "695a0469-812b-43aa-b5fa-fa60f0f2be61"
OUTPUT_DIR = "./output"
CHECK_INTERVAL = 30
# ==========================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)

def get_latest_delta_job():
    """가장 최근 delta_apply 잡 가져오기"""
    jobs = client.list_jobs(PROJECT_ID, job_type=JobTypes.APPLY_DELTAS)  # ← 수정
    if jobs:
        return jobs[0]
    return None

def download_files():
    print(f"[{datetime.now()}] 📥 다운로드 시작...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    client.download_project(
        project_id=PROJECT_ID,
        local_dir=OUTPUT_DIR,
        filter_glob="*",
        show_progress=True,
        force_download=True
    )
    print(f"[{datetime.now()}] ✅ 다운로드 완료! → {OUTPUT_DIR}")

# ── 최초 실행 ──
print(f"[{datetime.now()}] 👀 감시 시작! {CHECK_INTERVAL}초마다 delta_apply 잡을 확인합니다...")
download_files()

last_job = get_latest_delta_job()
last_job_id = last_job["id"] if last_job else None
print(f"[{datetime.now()}] 기준 잡 ID: {last_job_id}")

# ── 감시 루프 ──
while True:
    time.sleep(CHECK_INTERVAL)
    try:
        latest_job = get_latest_delta_job()

        if latest_job is None:
            print(f"[{datetime.now()}] 잡 없음...")
            continue

        latest_id = latest_job["id"]
        latest_status = latest_job["status"]

        if latest_id != last_job_id and latest_status == "FINISHED":
            print(f"[{datetime.now()}] 🔔 새 Sync 감지! (잡 ID: {latest_id})")
            download_files()
            last_job_id = latest_id
        else:
            print(f"[{datetime.now()}] 변경 없음... (최근 잡: {latest_id}, 상태: {latest_status})")

    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 오류: {e}")