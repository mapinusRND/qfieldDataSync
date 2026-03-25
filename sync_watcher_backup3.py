import time
import os
import shutil
import geopandas as gpd
from sqlalchemy import create_engine, text
from datetime import datetime
from qfieldcloud_sdk import sdk
from qfieldcloud_sdk.sdk import JobTypes

# ========== 1. 설정 ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"
#PROJECT_ID = "695a0469-812b-43aa-b5fa-fa60f0f2be61"
PROJECT_ID = "eaa07462-f2c6-4db5-9bb5-69e93e01a791"

# [수정] 최상위 출력 경로
BASE_OUTPUT_DIR = "./output"

# PostgreSQL 설정
DB_URL = "postgresql://postgres:1q2w3e4r@10.10.10.215:5432/rnddb"
TARGET_SCHEMA = "disaster"
CHECK_INTERVAL = 30
# ============================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)
db_engine = create_engine(DB_URL)

# 시작 시 스키마 생성
with db_engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    conn.commit()

def get_project_dir():
    """PROJECT_ID별 전용 경로 반환 및 생성"""
    project_dir = os.path.join(BASE_OUTPUT_DIR, PROJECT_ID)
    return project_dir

def process_gpkg_to_db(project_path):
    """지정된 프로젝트 폴더 내의 GPKG를 DB로 전송"""
    print(f"[{datetime.now()}] 🐘 DB 업로드 시작 (Project: {PROJECT_ID})")
    
    if not os.path.exists(project_path):
        return

    for file in os.listdir(project_path):
        if file.endswith(".gpkg"):
            gpkg_path = os.path.join(project_path, file)
            print(f"  - 파일 분석: {file}")
            
            try:
                import fiona
                layers = fiona.listlayers(gpkg_path)
                
                with db_engine.connect() as connection:
                    for layer_name in layers:
                        gdf = gpd.read_file(gpkg_path, layer=layer_name)
                        count = len(gdf)
                        
                        # 테이블명에 프로젝트 ID를 포함하거나 구분할 수 있게 명명
                        # 여기서는 기존 규칙을 유지하되 필요시 p_{PROJECT_ID[:8]}_ 등을 붙일 수 있습니다.
                        table_name = f"sync_{file.replace('.gpkg', '')}_{layer_name}".lower()
                        
                        gdf.to_postgis(
                            table_name, 
                            connection, 
                            schema=TARGET_SCHEMA, 
                            if_exists="replace", 
                            index=False
                        )
                        connection.commit()
                        print(f"    ✅ {layer_name} ({count}개) -> {TARGET_SCHEMA}.{table_name}")
                        
            except Exception as e:
                print(f"    ⚠️ {file} 처리 오류: {e}")

def download_files():
    project_path = get_project_dir()
    print(f"[{datetime.now()}] 📥 프로젝트 전용 폴더 갱신: {project_path}")
    
    # 1. 해당 프로젝트 폴더만 삭제 후 재생성 (캐시 제거)
    if os.path.exists(project_path):
        shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)

    # 2. 서버 파일 다운로드 (해당 프로젝트 폴더로 지정)
    client.download_project(
        project_id=PROJECT_ID,
        local_dir=project_path,
        filter_glob="*",
        show_progress=True,
        force_download=True
    )
    print(f"[{datetime.now()}] ✅ 다운로드 완료")
    
    # 3. 해당 경로의 파일들로 DB 작업 수행
    process_gpkg_to_db(project_path)

def get_latest_delta_job():
    try:
        jobs = client.list_jobs(PROJECT_ID, job_type=JobTypes.APPLY_DELTAS)
        return jobs[0] if jobs else None
    except:
        return None

# ── 최초 실행 ──
print(f"[{datetime.now()}] 👀 프로젝트 모니터링 시작: {PROJECT_ID}")
download_files()

last_job = get_latest_delta_job()
last_job_id = last_job["id"] if last_job else None

# ── 감시 루프 ──
while True:
    time.sleep(CHECK_INTERVAL)
    try:
        latest_job = get_latest_delta_job()
        if not latest_job: continue
        
        latest_id = latest_job["id"]
        latest_status = latest_job["status"]

        if latest_id != last_job_id and latest_status == "FINISHED":
            print(f"[{datetime.now()}] 🔔 신규 동기화 감지 (Job: {latest_id})")
            print("    ⏳ 서버 병합 대기 중 (10초)...")
            time.sleep(10)
            
            download_files()
            last_job_id = latest_id
            
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 루프 에러: {e}")