import time
import os
import geopandas as gpd
from sqlalchemy import create_engine
from datetime import datetime
from qfieldcloud_sdk import sdk
from qfieldcloud_sdk.sdk import JobTypes

# ========== 1. 설정 (사용자 환경에 맞게 수정) ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"
PROJECT_ID = "695a0469-812b-43aa-b5fa-fa60f0f2be61"
#PROJECT_ID = "eaa07462-f2c6-4db5-9bb5-69e93e01a791"
OUTPUT_DIR = "./output"
CHECK_INTERVAL = 30

# PostgreSQL 설정
DB_URL = "postgresql://postgres:1q2w3e4r@10.10.10.215:5432/rnddb"
TARGET_SCHEMA = "disaster"
# =====================================================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)
db_engine = create_engine(DB_URL)

def process_gpkg_to_db():
    """다운로드된 폴더 내의 모든 GPKG 파일을 찾아 PostGIS로 전송"""
    print(f"[{datetime.now()}] 🐘 데이터베이스 업로드 시작...")
    
    # OUTPUT_DIR 내의 모든 .gpkg 파일 탐색
    for file in os.listdir(OUTPUT_DIR):
        if file.endswith(".gpkg"):
            gpkg_path = os.path.join(OUTPUT_DIR, file)
            print(f"  - 처리 중: {file}")
            
            try:
                # GPKG 내의 레이어 목록 가져오기
                import fiona
                layers = fiona.listlayers(gpkg_path)
                
                for layer_name in layers:
                    # GeoPandas로 레이어 읽기
                    gdf = gpd.read_file(gpkg_path, layer=layer_name)
                    
                    # DB에 저장 (테이블명은 파일명_레이어명 조합)
                    table_name = f"sync_{file.replace('.gpkg', '')}_{layer_name}".lower()
                    gdf.to_postgis(table_name, db_engine, schema=TARGET_SCHEMA, if_exists="replace", index=False)
                    print(f"    ✅ 레이어 [{layer_name}] -> 테이블 [{table_name}] 업로드 완료")
                    
            except Exception as e:
                print(f"    ⚠️ {file} 처리 중 오류: {e}")

def get_latest_delta_job():
    """가장 최근 delta_apply 잡 가져오기"""
    jobs = client.list_jobs(PROJECT_ID, job_type=JobTypes.APPLY_DELTAS)
    if jobs:
        return jobs[0]
    return None

def download_files():
    print(f"[{datetime.now()}] 📥 서버 파일 다운로드 시작...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    client.download_project(
        project_id=PROJECT_ID,
        local_dir=OUTPUT_DIR,
        filter_glob="*",
        show_progress=True,
        force_download=True
    )
    print(f"[{datetime.now()}] ✅ 다운로드 완료!")
    
    # 다운로드 직후 DB 업로드 프로세스 실행
    process_gpkg_to_db()

# ── 최초 실행 ──
print(f"[{datetime.now()}] 👀 감시 시작! {CHECK_INTERVAL}초마다 확인합니다...")
download_files()

last_job = get_latest_delta_job()
last_job_id = last_job["id"] if last_job else None

# ── 감시 루프 ──
while True:
    time.sleep(CHECK_INTERVAL)
    try:
        latest_job = get_latest_delta_job()
        if latest_job is None: continue

        latest_id = latest_job["id"]
        latest_status = latest_job["status"]

        # 새로운 완료된 잡이 발견되면 실행
        if latest_id != last_job_id and latest_status == "FINISHED":
            print(f"[{datetime.now()}] 🔔 새 데이터 동기화 감지! (Job ID: {latest_id})")
            download_files()
            last_job_id = latest_id
        else:
            # 루프 로그 간소화
            pass 

    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 오류 발생: {e}")