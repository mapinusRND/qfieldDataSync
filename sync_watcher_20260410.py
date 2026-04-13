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

# QFieldCloud 운영 DB (212 서버)
QFC_DB_HOST = "10.10.10.212"
QFC_DB_PORT = 5433
QFC_DB_NAME = "qfieldcloud_db"
QFC_DB_USER = "root"
QFC_DB_PASS = "1q2w3e4r"

# 데이터 저장 대상 DB (215 서버)
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BASE_OUTPUT_DIR = "./qfield"
TARGET_SCHEMA = "qfield"
VIEW_NAME = "v_qfield_total_data"
CHECK_INTERVAL = 30
# ============================================

# SDK 클라이언트 초기화 및 로그인 함수 (세션 만료 대비)
def login_client():
    try:
        new_client = sdk.Client(url=URL)
        new_client.login(username=USERNAME, password=PASSWORD)
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None

client = login_client()
# SQLAlchemy 엔진 (연결 풀 최적화)
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=600)

def get_pg_conn():
    """215 저장용 DB 연결"""
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_qfc_db_conn():
    """212 운영 DB 연결"""
    return psycopg2.connect(host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS)

# 시작 시 대상 스키마 생성 확인 (215 서버)
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

def grant_admin_permission_via_db(project_id):
    """212 DB에 접속하여 admin 권한 강제 주입"""
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM public.core_user WHERE username = %s", (USERNAME,))
        admin_res = cur.fetchone()
        if admin_res:
            admin_id = admin_res[0]
            query = """
                INSERT INTO public.core_projectcollaborator 
                (project_id, collaborator_id, role, created_at, updated_at, created_by_id, updated_by_id, is_incognito)
                VALUES (%s, %s, 'admin', NOW(), NOW(), %s, %s, false)
                ON CONFLICT (project_id, collaborator_id) DO NOTHING
            """
            cur.execute(query, (project_id, admin_id, admin_id, admin_id))
            conn.commit()
    except Exception as e:
        print(f"    ⚠️ 권한 부여 중 에러: {e}")
    finally:
        if conn: conn.close()

def save_gdf_direct(gdf, table_name, schema, project_path, owner_name):
    """GDF를 DB 테이블로 변환 및 저장 (215 서버)"""
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None
        
        final_cols = []
        for c in gdf.columns:
            if c == geom_col: continue
            final_cols.append(c)
            if c == 'record': final_cols.append('audio_txt')

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
                    if record_file and isinstance(record_file, str) and record_file.strip():
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
        print(f"        ✅ [DB 저장 성공] {table_name}")
    except Exception as e:
        if conn: conn.rollback()
        print(f"        ❌ [DB 저장 실패] {e}")
    finally:
        if conn: cur.close(); conn.close()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """다운로드 후 GPKG 분석 및 DB 전송 (215 서버)"""
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # 1. 관리 테이블 확인 (SQLAlchemy 엔진 사용)
    with db_engine.connect() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (seq SERIAL PRIMARY KEY, id TEXT, name TEXT, gpkg_name TEXT, table_name TEXT, owner TEXT, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name));"))
        conn.commit()

    any_updated, global_table_index = False, 1
    
    # 2. GPKG 파일 순회
    if not os.path.exists(project_path): return
    files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]
    
    for file in files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        print(f"    📄 [파일 분석] {file}")

        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']: continue
                print(f"        🏷️ [레이어 읽기] {layer_name}")
                
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue
                
                print(f"        📐 [좌표계 변환] {layer_name}")
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner)
                
                # 매니지 테이블 업데이트
                with db_engine.connect() as conn:
                    conn.execute(text(f"INSERT INTO {TARGET_SCHEMA}.qfield_data_manage (id, name, gpkg_name, table_name, owner, reg_date, update_at) VALUES (:pid, :pname, :gname, :tname, :owner, :now, :now) ON CONFLICT (id, gpkg_name) DO UPDATE SET name = EXCLUDED.name, table_name = EXCLUDED.table_name, owner = EXCLUDED.owner, update_at = EXCLUDED.update_at;"), 
                                 {"pid": project_id, "pname": project_name, "gname": file_stem, "tname": table_name, "owner": owner, "now": now})
                    conn.commit()
                
                any_updated, global_table_index = True, global_table_index + 1
        except Exception as e:
            print(f"        ⚠️ {file} 레이어 처리 중 에러: {e}")

    if any_updated:
        update_unified_view()

def update_unified_view():
    """통합 뷰 갱신 (215 서버)"""
    print(f"    📊 [통합 뷰 갱신 시작] {VIEW_NAME}")
    conn = None
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
            print(f"      ✅ 통합 뷰 갱신 완료")
    except Exception as e: 
        if conn: conn.rollback()
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally: 
        if conn: cur.close(); conn.close()

def sync_single_project(project_data):
    """개별 프로젝트 동기화 워크플로우"""
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

    # 1. 권한 부여 (212)
    grant_admin_permission_via_db(p_id)
    time.sleep(1) 

    # 2. 로컬 경로 정리
    if os.path.exists(project_path): 
        try: shutil.rmtree(project_path)
        except: pass
    os.makedirs(project_path, exist_ok=True)
    
    # 3. 다운로드 및 처리
    try:
        print(f"    🚀 [다운로드 시도] {p_name} (소유자: {p_owner})")
        if not client: client = login_client()
        
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        print(f"    ✅ [다운로드 완료] {p_name}")
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e: 
        if "401" in str(e) or "Unauthorized" in str(e):
            print("    🔄 세션 만료됨. 재로그인 후 재시도합니다.")
            client = login_client()
        print(f"    ⚠️ {p_name} 처리 실패: {e}")

def get_latest_job_id(project_id):
    """최신 작업 ID 조회"""
    global client
    try:
        if not client: client = login_client()
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return "NO_JOB"
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except: return "JOB_CHECK_ERROR"

def get_all_projects_from_db():
    """운영 DB(212)에서 프로젝트 목록 조회"""
    projects = []
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT p.id, p.name, u.username as owner_name FROM public.core_project p JOIN public.core_user u ON p.owner_id = u.id"
        cur.execute(query)
        for r in cur.fetchall():
            projects.append({'id': str(r['id']), 'name': r['name'], 'owner': r['owner_name']})
    except Exception as e: print(f"⚠️ 운영 DB 조회 에러: {e}")
    finally: 
        if conn: conn.close()
    return projects

# ========== 메인 실행 루프 ==========
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...")

while True:
    try:
        current_projects = get_all_projects_from_db()
        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            current_job_id = get_latest_job_id(p_id)
            
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (소유자: {p['owner']})")
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id
                
    except Exception as e: 
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(5)
        client = login_client()
        
    time.sleep(CHECK_INTERVAL)