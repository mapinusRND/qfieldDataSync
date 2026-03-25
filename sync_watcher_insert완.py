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

BASE_OUTPUT_DIR = "./output"
DB_URL = "postgresql://postgres:1q2w3e4r@10.10.10.215:5432/rnddb"
TARGET_SCHEMA = "qfield"
CHECK_INTERVAL = 30 
# ============================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)
db_engine = create_engine(DB_URL)

with db_engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    conn.commit()

def get_all_projects():
    try:
        # 서버의 모든 프로젝트 리스트 반환 (자바 이식 핵심 부분)
        return client.list_projects()
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 목록 조회 실패: {e}")
        return []

def get_project_dir(project_id):
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def process_gpkg_to_db(project_id, project_path):
    print(f"    🐘 DB 업로드 시작 (ID: {project_id[:8]}...)")
    if not os.path.exists(project_path): return

    for file in os.listdir(project_path):
        if file.endswith(".gpkg"):
            gpkg_path = os.path.join(project_path, file)
            try:
                import fiona
                layers = fiona.listlayers(gpkg_path)
                with db_engine.connect() as connection:
                    for layer_name in layers:
                        gdf = gpd.read_file(gpkg_path, layer=layer_name)
                        if gdf.empty: continue
                        
                        #short_id = project_id.split('-')[0]
                        # table_name = f"sync_{short_id}_{file.replace('.gpkg', '')}_{layer_name}".lower()
                        table_name = f"{project_id}".lower()
                        
                        gdf.to_postgis(
                            table_name, connection, 
                            schema=TARGET_SCHEMA, if_exists="replace", index=False
                        )
                        connection.commit()
                        print(f"      ✅ {layer_name} ({len(gdf)}개) -> {table_name}")
            except Exception as e:
                print(f"      ⚠️ {file} 처리 오류: {e}")

def sync_single_project(project_id):
    project_path = get_project_dir(project_id)
    print(f"[{datetime.now()}] 📥 동기화 실행: {project_id}")
    
    if os.path.exists(project_path):
        shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)

    try:
        client.download_project(
            project_id=project_id,
            local_dir=project_path,
            filter_glob="*",
            show_progress=False,
            force_download=True
        )
        process_gpkg_to_db(project_id, project_path)
    except Exception as e:
        print(f"    ⚠️ 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    try:
        jobs = client.list_jobs(project_id, job_type=JobTypes.APPLY_DELTAS)
        if jobs and jobs[0]['status'] == "FINISHED":
            return jobs[0]['id']
    except:
        pass
    return None

# ==========================================
# 메인 감시 로직 (갱신 누락 방지 보강)
# ==========================================
last_jobs_cache = {}

print(f"[{datetime.now()}] 🚀 프로젝트 통합 모니터링 서비스 시작...")

while True:
    try:
        current_projects = get_all_projects()
        print(f"[{datetime.now()}] 🔍 서버 스캔 중... (발견된 프로젝트: {len(current_projects)}개)")
        
        for p in current_projects:
            p_id = p['id']
            p_name = p['name']
            project_path = get_project_dir(p_id)
            
            # 최신 완료된 Job ID 확인
            current_job_id = get_latest_job_id(p_id)
            
            # 동기화가 필요한 조건:
            # 1. 이 프로젝트를 처음 발견했거나 (last_jobs_cache에 없음)
            # 2. 로컬에 폴더가 없거나 (물리적 삭제 후 재실행 등)
            # 3. 새로운 Job ID가 생성되었을 때
            needs_sync = False
            
            if p_id not in last_jobs_cache:
                print(f"    🆕 새 프로젝트 발견: {p_name}")
                needs_sync = True
            elif not os.path.exists(project_path):
                print(f"    📂 폴더 누락 재동기화: {p_name}")
                needs_sync = True
            elif current_job_id != last_jobs_cache[p_id]:
                print(f"    🔔 신규 업데이트 감지: {p_name} (Job: {current_job_id})")
                needs_sync = True
            
            if needs_sync:
                # 변경 감지 시 서버 병합 대기 (기존 프로젝트 업데이트 시에만 적용)
                if p_id in last_jobs_cache and current_job_id != last_jobs_cache[p_id]:
                    print("    ⏳ 서버 파일 병합 대기 (10s)...")
                    time.sleep(10)
                
                sync_single_project(p_id)
                last_jobs_cache[p_id] = current_job_id
                
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ 메인 루프 에러: {e}")
        
    time.sleep(CHECK_INTERVAL)