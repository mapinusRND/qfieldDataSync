import time
import os
import shutil
import requests
import geopandas as gpd
from sqlalchemy import create_engine, text
from datetime import datetime
from qfieldcloud_sdk import sdk
from qfieldcloud_sdk.sdk import JobTypes

# ========== 1. 설정 ==========
# QFieldCloud 설정
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# 파일경로 설정
BASE_OUTPUT_DIR = "./output"

# DB 설정
DB_URL = "postgresql://postgres:1q2w3e4r@10.10.10.215:5432/rnddb"
FLASK_URL = "http://10.10.10.212:8000"
TARGET_SCHEMA = "qfield"

# 반복 체크 주기
CHECK_INTERVAL = 30 
# ============================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)
db_engine = create_engine(DB_URL)

with db_engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    conn.commit()

def convert_to_audio_text(project_path, audio_rel_path):
    """오디오 파일을 텍스트로 변환. 실패하거나 파일이 없으면 None 반환"""
    if not audio_rel_path:
        return None
        
    # 1. 실제 로컬에 파일이 있는지 먼저 확인
    # GPKG에는 'audio/filename.m4a'라고 적혀있으므로 project_path와 합쳐야 함
    full_audio_path = os.path.join(project_path, audio_rel_path.replace('/', os.sep))
    
    if not os.path.exists(full_audio_path):
        print(f"      ⚠️ 오디오 파일 없음: {full_audio_path}")
        return None

    # 2. Flask API 호출 경로 생성
    # Flask 서버가 /api/wav2text/ 경로 뒤에 상대 경로를 바로 붙이는 구조인지 확인 필요
    clean_rel_path = audio_rel_path if audio_rel_path.startswith('/') else '/' + audio_rel_path
    api_url = f"{FLASK_URL}/api/wav2text{clean_rel_path}"
    print("api_url : ",api_url);
    print(f"      🎙️ STT 요청: {api_url}")
    
    try:
        response = requests.get(api_url, timeout=60)
        if response.status_code == 200:
            result = response.json()
            stt_text = result.get('text') or result.get('result')
            return stt_text if stt_text else None
        else:
            print(f"      ⚠️ Flask 서버 응답 오류 ({response.status_code})")
    except Exception as e:
        print(f"      ⚠️ STT 통신 실패: {e}")
    
    return None

def process_gpkg_to_db(project_id, project_path, project_name):
    print(f"    🐘 DB 작업 시작 (ID: {project_id[:8]}...)")
    
    for file in os.listdir(project_path):
        if file.endswith(".gpkg"):
            gpkg_path = os.path.join(project_path, file)
            try:
                import fiona
                layers = fiona.listlayers(gpkg_path)
                with db_engine.connect() as connection:
                    # 1. 관리 테이블 생성
                    connection.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                            seq SERIAL PRIMARY KEY,
                            id TEXT UNIQUE,
                            name TEXT,
                            audio_txt TEXT DEFAULT NULL,
                            update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """))
                    connection.commit()

                    final_stt_text = None

                    for layer_name in layers:
                        gdf = gpd.read_file(gpkg_path, layer=layer_name)
                        if gdf.empty: continue
                        
                        # 오디오 컬럼이 있고 값이 있는 경우에만 STT 시도
                        if 'audio' in gdf.columns:
                            valid_audios = gdf['audio'].dropna()
                            if not valid_audios.empty:
                                # 첫 번째 유효한 오디오 파일 변환
                                final_stt_text = convert_to_audio_text(project_path, str(valid_audios.iloc[0]))

                        table_name = f"{project_id}".lower()
                        gdf.to_postgis(table_name, connection, schema=TARGET_SCHEMA, if_exists="replace", index=False)
                        connection.commit()
                        print(f"      ✅ {layer_name} ({len(gdf)}개) -> {table_name}")

                    # 2. 관리 테이블 업데이트 (실패 시 NULL 저장)
                    insert_manage_sql = f"""
                    INSERT INTO {TARGET_SCHEMA}.qfield_data_manage (id, name, audio_txt)
                    VALUES (:pid, :pname, :atxt)
                    ON CONFLICT (id) DO UPDATE 
                    SET name = EXCLUDED.name,
                        audio_txt = EXCLUDED.audio_txt,
                        update_at = CURRENT_TIMESTAMP;
                    """
                    connection.execute(text(insert_manage_sql), {
                        "pid": project_id, 
                        "pname": project_name, 
                        "atxt": final_stt_text  # None일 경우 DB에 NULL로 들어감
                    })
                    connection.commit()
                    print(f"      📝 관리 테이블 갱신 완료 (STT: {final_stt_text})")
                    
            except Exception as e:
                print(f"      ⚠️ {file} 처리 오류: {e}")

# --- 하단 메인 로직(get_all_projects, sync_single_project 등)은 이전과 동일 ---
def get_all_projects():
    try: return client.list_projects()
    except: return []

def get_project_dir(project_id):
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def sync_single_project(project_id, project_name):
    project_path = get_project_dir(project_id)
    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    try:
        client.download_project(project_id=project_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        process_gpkg_to_db(project_id, project_path, project_name)
    except Exception as e: print(f"    ⚠️ 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    try:
        jobs = client.list_jobs(project_id, job_type=JobTypes.APPLY_DELTAS)
        if jobs and jobs[0]['status'] == "FINISHED": return jobs[0]['id']
    except: pass
    return None

last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 통합 모니터링 시작...")

while True:
    try:
        current_projects = get_all_projects()
        for p in current_projects:
            p_id, p_name = p['id'], p['name']
            project_path = get_project_dir(p_id)
            current_job_id = get_latest_job_id(p_id)
            needs_sync = False
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                needs_sync = True
            
            if needs_sync:
                if p_id in last_jobs_cache and current_job_id != last_jobs_cache[p_id]:
                    time.sleep(10)
                sync_single_project(p_id, p_name)
                last_jobs_cache[p_id] = current_job_id
    except Exception as e: print(f"⚠️ 에러: {e}")
    time.sleep(CHECK_INTERVAL)