import time
import os
import shutil
import requests
import geopandas as gpd
import pandas as pd
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

# 초기 스키마 생성
with db_engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    conn.commit()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    print(f"    🐘 DB 작업 시작 (ID: {project_id[:13]}...)")
    
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    with db_engine.connect() as connection:
        # 1. 관리 테이블 생성
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                seq SERIAL PRIMARY KEY,
                id TEXT,
                name TEXT,
                gpkg_name TEXT,
                table_name TEXT, 
                owner TEXT,
                reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            );
        """))
        connection.commit()

        global_table_index = 1

        for file in os.listdir(project_path):
            if file.endswith(".gpkg"):
                gpkg_path = os.path.join(project_path, file)
                file_stem_only = os.path.splitext(file)[0]
                representative_table_name = None
                
                try:
                    import fiona
                    layers = fiona.listlayers(gpkg_path)
                    
                    for layer_name in layers:
                        # --- [추가] 시스템 레이어 및 스타일 테이블 필터링 ---
                        # 1. 이름으로 1차 필터링
                        if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'rtree_...']:
                            continue
                        
                        gdf = gpd.read_file(gpkg_path, layer=layer_name)
                        
                        # 2. 내용으로 2차 필터링 (데이터가 아예 없거나 스타일 전용 테이블인 경우)
                        if gdf.empty or 'style' in layer_name.lower():
                            continue
                        
                        # 3. 컬럼 구성으로 3차 필터링 (XML 데이터가 들어있는 시스템 테이블 특징 추출)
                        if 'stylexml' in [c.lower() for c in gdf.columns] or 'styleqml' in [c.lower() for c in gdf.columns]:
                            continue

                        # 메타 정보 추가
                        gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                        
                        # 테이블명 생성
                        table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                        
                        if representative_table_name is None:
                            representative_table_name = table_name

                        # 데이터 테이블 적재
                        if isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None:
                            gdf.to_postgis(table_name, connection, schema=TARGET_SCHEMA, if_exists="replace", index=False)
                        else:
                            gdf.to_sql(table_name, connection, schema=TARGET_SCHEMA, if_exists="replace", index=False)
                        
                        print(f"      ✅ 적재 완료: {layer_name} -> {table_name}")
                        global_table_index += 1

                    # GPKG당 Row 1개만 관리 테이블에 업데이트
                    if representative_table_name:
                        insert_manage_sql = f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage (id, name, gpkg_name, table_name, owner, reg_date, update_at)
                        VALUES (:pid, :pname, :gname, :tname, :owner, :now, :now)
                        ON CONFLICT (id, gpkg_name) DO UPDATE 
                        SET name = EXCLUDED.name,
                            table_name = EXCLUDED.table_name,
                            owner = EXCLUDED.owner,
                            update_at = EXCLUDED.update_at;
                        """
                        connection.execute(text(insert_manage_sql), {
                            "pid": project_id, 
                            "pname": project_name,
                            "gname": file_stem_only,
                            "tname": representative_table_name,
                            "owner": owner,
                            "now": now
                        })
                        connection.commit()
                        print(f"      📝 관리 테이블 갱신: {file_stem_only}")

                except Exception as e:
                    try: connection.rollback()
                    except: pass
                    print(f"      ⚠️ {file} 처리 오류: {e}")

# --- 메인 로직 ---
def get_all_projects():
    try: return client.list_projects()
    except: return []

def get_project_dir(project_id):
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def sync_single_project(project_data):
    p_id = project_data['id']
    p_name = project_data['name']
    p_owner = project_data.get('owner', 'unknown')
    project_path = get_project_dir(p_id)
    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    try:
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
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
            p_id = p['id']
            project_path = get_project_dir(p_id)
            current_job_id = get_latest_job_id(p_id)
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                if p_id in last_jobs_cache: time.sleep(5)
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id
    except Exception as e: print(f"⚠️ 에러: {e}")
    time.sleep(CHECK_INTERVAL)