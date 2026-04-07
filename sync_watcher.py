import time
import os
import shutil
import geopandas as gpd
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from datetime import datetime
from qfieldcloud_sdk import sdk
from shapely.wkb import dumps as wkb_dumps

# disaster2convert 모듈 (Speech-To-Text 외부 모듈)
try:
    import disaster2convert as dc 
except ImportError:
    dc = None
    print("⚠️ disaster2convert 모듈을 찾을 수 없습니다. STT 기능이 제외됩니다.")

# ========== 1. 설정 (Configuration) ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# QFieldCloud 운영 DB (212 서버 - 프로젝트 목록 및 실제 소유자 추출용)
QFC_DB_HOST = "10.10.10.212"
QFC_DB_PORT = 5433
QFC_DB_NAME = "qfieldcloud_db"
QFC_DB_USER = "root"
QFC_DB_PASS = "1q2w3e4r"

# 데이터 저장 대상 DB (215 서버 - 최종 데이터 수집용)
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BASE_OUTPUT_DIR = "./qfield"         # 다운로드된 파일 저장 경로
TARGET_SCHEMA = "qfield"             # 데이터를 저장할 DB 스키마
VIEW_NAME = "v_qfield_total_data"    # 전체 통합 뷰 이름
CHECK_INTERVAL = 30                  # 모니터링 주기 (초)
# ============================================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)

def get_pg_conn():
    """데이터 저장용 DB(215) 연결"""
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_qfc_db_conn():
    """운영 DB(212) 연결"""
    return psycopg2.connect(host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS)

# 시작 시 대상 스키마 생성 확인 (215 서버)
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

def update_unified_view():
    """통합 뷰 생성 및 갱신"""
    print(f"    📊 통합 뷰({VIEW_NAME}) 갱신 중...")
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage")
        rows = cur.fetchall()
        if not rows: return

        view_parts = []
        for r in rows:
            t_name = r['table_name']
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
            if cur.fetchone()[0]:
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s AND column_name != 'seq' ORDER BY ordinal_position", (t_name,))
                columns = [f'd."{col[0]}"' for col in cur.fetchall()]
                column_string = ", ".join(columns)
                part = f"SELECT '{r['id']}'::text as manage_id, '{r['name']}'::text as project_name, '{r['gpkg_name']}'::text as source_gpkg, '{t_name}'::text as source_table, {column_string} FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                view_parts.append(part)

        if view_parts:
            cur.execute(f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{VIEW_NAME} AS " + " UNION ALL ".join(view_parts))
            conn.commit()
            print(f"      ✅ 통합 뷰 생성 완료.")
    except Exception as e: print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally: cur.close(); conn.close()

def save_gdf_direct(gdf, table_name, schema, project_path, owner_name):
    """GeoDataFrame을 DB 테이블로 실제 저장 (owner_name 반영)"""
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None
        
        final_cols = []
        for c in gdf.columns:
            if c == geom_col: continue
            final_cols.append(c)
            if c == 'record': final_cols.append('audio_txt')

        # 테이블 컬럼 타입 정의
        col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
        for col in final_cols:
            if col == 'audio_txt': col_defs.append(f'"{col}" TEXT')
            else:
                dtype = str(gdf[col].dtype)
                if 'int' in dtype: col_defs.append(f'"{col}" BIGINT')
                elif 'float' in dtype: col_defs.append(f'"{col}" DOUBLE PRECISION')
                elif 'datetime' in dtype: col_defs.append(f'"{col}" TIMESTAMP')
                else: col_defs.append(f'"{col}" TEXT')
        
        if is_geo: col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')

        # 테이블 재생성
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        for _, row in gdf.iterrows():
            cols, placeholders, values = ['platform_type'], ['%s'], [1]
            for col in final_cols:
                cols.append(f'"{col}"')
                placeholders.append('%s')
                if col == 'audio_txt':
                    record_file = row.get('record')
                    stt_val = ""
                    if record_file and isinstance(record_file, str):
                        audio_path = os.path.join(project_path, record_file)
                        if os.path.exists(audio_path) and dc:
                            try: stt_val = dc.read_audio(audio_path)
                            except: pass
                    values.append(stt_val)
                else:
                    val = row[col]
                    values.append(None if pd.isna(val) else val)

            if is_geo:
                cols.append(f'"{geom_col}"')
                geom = row[geom_col]
                if geom:
                    values.append(wkb_dumps(geom, hex=True, srid=3857))
                    placeholders.append('%s::geometry')
                else:
                    values.append(None); placeholders.append('%s')

            cur.execute(f'INSERT INTO {schema}."{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})', values)
        
        conn.commit()
        print(f"        ✅ 테이블 저장 완료: {schema}.{table_name} (소유자: {owner_name})")
    except Exception as e:
        conn.rollback()
        print(f"        ❌ DB 저장 에러: {e}")
    finally: cur.close(); conn.close()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """프로젝트 내 GPKG 파일들을 DB로 변환"""
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # 관리 테이블 존재 확인
    with db_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                seq SERIAL PRIMARY KEY, id TEXT, name TEXT, gpkg_name TEXT,
                table_name TEXT, owner TEXT, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            );
        """))

    global_table_index, any_updated = 1, False
    for file in os.listdir(project_path):
        if not file.endswith(".gpkg"): continue
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        
        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                if layer_name.lower() in ['layer_styles', 'geopackage_contents']: continue
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue
                
                # 좌표계 변환 및 메타데이터(실제 소유자 명) 할당
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                
                # 실제 소유자 명이 포함된 테이블명 생성
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner)
                
                # 관리 테이블 정보 기록/갱신
                with db_engine.begin() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage (id, name, gpkg_name, table_name, owner, reg_date, update_at)
                        VALUES (:pid, :pname, :gname, :tname, :owner, :now, :now)
                        ON CONFLICT (id, gpkg_name) DO UPDATE
                        SET name = EXCLUDED.name, table_name = EXCLUDED.table_name, owner = EXCLUDED.owner, update_at = EXCLUDED.update_at;
                    """), {"pid": project_id, "pname": project_name, "gname": file_stem, "tname": table_name, "owner": owner, "now": now})
                
                any_updated, global_table_index = True, global_table_index + 1
        except Exception as e: print(f"      ⚠️ {file} 처리 오류: {e}")

    if any_updated: update_unified_view()

def get_all_projects_from_db():
    """212 DB에서 프로젝트와 실제 소유자 명을 조인하여 가져옴"""
    projects = []
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # core_project와 core_user 조인 (owner_id 기준)
        query = """
            SELECT p.id, p.name, u.username as owner_name
            FROM public.core_project p
            JOIN public.core_user u ON p.owner_id = u.id
        """
        cur.execute(query)
        for r in cur.fetchall():
            projects.append({
                'id': str(r['id']), 
                'name': r['name'], 
                'owner': r['owner_name']
            })
    except Exception as e: print(f"⚠️ QFC 운영 DB 조회 에러: {e}")
    finally: 
        if conn: conn.close()
    return projects

def sync_single_project(project_data):
    """프로젝트 다운로드 및 권한 체크"""
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

    # admin 협업자 강제등록 시도 (API 접근 허용 유도)
    try:
        client.create_collaborator(project_id=p_id, username=USERNAME, role="admin")
    except: pass

    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    
    try:
        print(f"    🚀 다운로드: {p_name} (소유자: {p_owner})")
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e: print(f"    ⚠️ {p_name} 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    """최신 작업 ID 확인"""
    try:
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return "NO_JOB"
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except: return "AUTH_ERROR"

# ========== 메인 실행 루프 ==========
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 프로젝트 소유자 기반 실시간 동기화 엔진 시작...")

while True:
    try:
        current_projects = get_all_projects_from_db()
        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            current_job_id = get_latest_job_id(p_id)
            
            # 동기화 조건: 캐시에 없거나, 로컬 폴더가 없거나, Job ID가 바뀌었을 때
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (Owner: {p['owner']})")
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id
                
    except Exception as e: print(f"⚠️ 메인 루프 에러: {e}")
    time.sleep(CHECK_INTERVAL)