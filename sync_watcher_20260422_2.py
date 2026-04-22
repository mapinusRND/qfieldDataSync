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

# ========== 외부 모듈 로드: Speech-To-Text (STT) ==========
try:
    import disaster2convert as dc
except ImportError:
    dc = None
    print("⚠️ disaster2convert 모듈을 찾을 수 없습니다. STT 기능이 제외됩니다.")

# ========== 1. 설정 (Configuration) ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

QFC_DB_HOST = "10.10.10.212"
QFC_DB_PORT = 5433
QFC_DB_NAME = "qfieldcloud_db"
QFC_DB_USER = "root"
QFC_DB_PASS = "1q2w3e4r"

DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ENV = os.getenv('FLASK_ENV', 'local')
if ENV == 'local':
    BASE_OUTPUT_DIR = "D:/work/qfield"
else:
    BASE_OUTPUT_DIR = "/app/webfiles/qfield"

TARGET_SCHEMA = "qfield"
CHECK_INTERVAL = 30 

QFIELD_INFO_SCHEMA = "disaster"
QFIELD_INFO_TABLE = "qfield_info"

if not os.path.exists(BASE_OUTPUT_DIR):
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    print(f"📂 [경로 생성] {BASE_OUTPUT_DIR}")

# ---------- SDK 및 DB 연결 관련 함수 ----------

def login_client():
    try:
        new_client = sdk.Client(url=URL)
        new_client.login(username=USERNAME, password=PASSWORD)
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None

client = login_client()
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=600)

def get_pg_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_qfc_db_conn():
    return psycopg2.connect(host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS)

with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

# ---------- qfield_info (기준 정보) 관리 함수 ----------

def get_qfield_info_column_lists():
    result = {}
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = f"SELECT qfield_type, column_list FROM {QFIELD_INFO_SCHEMA}.{QFIELD_INFO_TABLE}"
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            qfield_type = row['qfield_type']
            raw_list = row['column_list']

            if isinstance(raw_list, list):
                col_list = raw_list
            elif isinstance(raw_list, str):
                cleaned = raw_list.strip()
                if cleaned.startswith('{') and cleaned.endswith('}'):
                    inner = cleaned[1:-1]
                    col_list = [c.strip().strip('"') for c in inner.split(',') if c.strip()]
                else:
                    col_list = [c.strip() for c in cleaned.split(',') if c.strip()]
            else:
                col_list = []

            result[qfield_type] = col_list
        print(f"    ✅ [qfield_info] 총 {len(result)}개 타입 로드 완료")
    except Exception as e:
        print(f"    ❌ [qfield_info 조회 실패] {e}")
    finally:
        if conn: conn.close()
    return result

def find_matching_qfield_type(gpkg_columns, qfield_info_map):
    gpkg_col_set = set(c.lower() for c in gpkg_columns)
    for qfield_type, col_list in qfield_info_map.items():
        required_cols = set(c.lower() for c in col_list)
        if not required_cols: continue
        if required_cols.issubset(gpkg_col_set):
            return qfield_type, col_list
    return None, None

# ---------- 권한 및 데이터 저장 함수 ----------

def grant_admin_permission_via_db(project_id):
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

def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None

        if allowed_columns is not None:
            allowed_lower = [c.lower() for c in allowed_columns]
            filtered_cols = [c for c in gdf.columns if c != geom_col and c.lower() in allowed_lower]
            # update_at 제외
            meta_cols = ['owner', 'reg_date']
            for mc in meta_cols:
                if mc in gdf.columns and mc not in filtered_cols:
                    filtered_cols.append(mc)
            source_cols = filtered_cols
        else:
            source_cols = [c for c in gdf.columns if c != geom_col]

        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if 'record' in c.lower():
                final_cols.append(c + '_txt')

        col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
        for col in final_cols:
            if col.endswith('_txt') and 'record' in col.lower():
                col_defs.append(f'"{col}" TEXT')
            else:
                dtype = str(gdf[col].dtype)
                if 'int' in dtype: col_defs.append(f'"{col}" BIGINT')
                elif 'float' in dtype: col_defs.append(f'"{col}" DOUBLE PRECISION')
                elif 'datetime' in dtype: col_defs.append(f'"{col}" TIMESTAMP')
                else: col_defs.append(f'"{col}" TEXT')

        if is_geo:
            col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')

        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        for _, row in gdf.iterrows():
            cols, placeholders, values = ['platform_type'], ['%s'], [1]
            for col in final_cols:
                cols.append(f'"{col}"')
                placeholders.append('%s')

                if col.endswith('_txt') and 'record' in col.lower():
                    origin_record_col = col[:-4]
                    record_file = row.get(origin_record_col)
                    stt_val = ""
                    if record_file and isinstance(record_file, str) and record_file.strip():
                        audio_path = os.path.join(project_path, record_file)
                        if not os.path.exists(audio_path):
                            filename = os.path.basename(record_file)
                            for root, dirs, files in os.walk(project_path):
                                if filename in files:
                                    audio_path = os.path.join(root, filename)
                                    break
                        if os.path.exists(audio_path) and dc:
                            try:
                                stt_val = dc.read_audio(audio_path)
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
                    values.append(None)
                    placeholders.append('%s')

            cur.execute(
                f'INSERT INTO {schema}."{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})',
                values
            )

        conn.commit()
        print(f"        ✅ [DB 저장 성공] {table_name}")
    except Exception as e:
        if conn: conn.rollback()
        print(f"        ❌ [DB 저장 실패] {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# ---------- GPKG 분석 및 워크플로우 제어 함수 ----------

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map: return False

    with db_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                seq SERIAL PRIMARY KEY,
                id TEXT,
                name TEXT,
                gpkg_name TEXT,
                table_name TEXT,
                owner TEXT,
                qfield_type TEXT,
                reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            )
        """))

    any_updated, global_table_index = False, 1
    if not os.path.exists(project_path): return False

    files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]

    for file in files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        
        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']:
                    continue

                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue

                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                geom_col = gdf.geometry.name if is_geo else None
                gpkg_columns = [c for c in gdf.columns if c != geom_col]

                matched_type, matched_col_list = find_matching_qfield_type(gpkg_columns, qfield_info_map)
                if matched_type is None: continue

                print(f"        ✅ [매칭 성공] type='{matched_type}'")

                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                # update_at 할당 제거
                gdf = gdf.assign(owner=owner, reg_date=now)

                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                with db_engine.begin() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage
                            (id, name, gpkg_name, table_name, owner, qfield_type, reg_date)
                        VALUES
                            (:pid, :pname, :gname, :tname, :owner, :qtype, :now)
                        ON CONFLICT (id, gpkg_name) DO NOTHING
                    """), {
                        "pid": project_id, "pname": project_name, "gname": file_stem,
                        "tname": table_name, "owner": owner, "qtype": matched_type, "now": now
                    })

                any_updated, global_table_index = True, global_table_index + 1
        except Exception as e:
            print(f"        ⚠️ {file} 처리 중 에러: {e}")

    if any_updated:
        update_unified_view()
    return any_updated

def update_unified_view():
    print(f"    📊 [개별 뷰 갱신 시작]")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"SELECT DISTINCT qfield_type FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type IS NOT NULL")
        types = [r['qfield_type'] for r in cur.fetchall()]

        if not types: return

        for q_type in types:
            cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type = %s", (q_type,))
            rows = cur.fetchall()
            
            view_parts = []
            for r in rows:
                t_name = r['table_name']
                cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
                if cur.fetchone()[0]:
                    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s AND column_name != 'seq' ORDER BY ordinal_position", (t_name,))
                    columns = [f'd."{col[0]}"' for col in cur.fetchall()]
                    column_string = ", ".join(columns)
                    
                    part = (
                        f"SELECT '{r['id']}'::text as manage_id, '{r['name']}'::text as project_name, "
                        f"'{r['gpkg_name']}'::text as source_gpkg, '{t_name}'::text as source_table, "
                        f"'{q_type}'::text as qfield_type, {column_string} "
                        f"FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                    )
                    view_parts.append(part)

            if view_parts:
                specific_view_name = f"{q_type}_v_qfield_data"
                create_view_sql = f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{specific_view_name} AS " + " UNION ALL ".join(view_parts)
                cur.execute(create_view_sql)
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        if conn: conn.close()

def sync_single_project(project_data):
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
    grant_admin_permission_via_db(p_id)
    time.sleep(1)

    if os.path.exists(project_path):
        try: shutil.rmtree(project_path)
        except: pass
    os.makedirs(project_path, exist_ok=True)

    try:
        if not client: client = login_client()
        client.download_project(
            project_id=p_id,
            local_dir=project_path,
            filter_glob="*",
            show_progress=False,
            force_download=True
        )
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e:
        if "401" in str(e) or "Unauthorized" in str(e):
            client = login_client()
        print(f"    ⚠️ {p_name} 처리 실패: {e}")

def get_latest_job_id(project_id):
    global client
    try:
        if not client: client = login_client()
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return "NO_JOB"
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except:
        return "JOB_CHECK_ERROR"

def get_all_projects_from_db():
    projects = []
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT p.id, p.name, u.username as owner_name FROM public.core_project p JOIN public.core_user u ON p.owner_id = u.id"
        cur.execute(query)
        for r in cur.fetchall():
            projects.append({'id': str(r['id']), 'name': r['name'], 'owner': r['owner_name']})
    except Exception as e:
        print(f"⚠️ 운영 DB 조회 에러: {e}")
    finally:
        if conn: conn.close()
    return projects

# ========== 메인 실행 루프 (Infinite Loop) ==========
last_jobs_cache = {} 

# [수정 추가] 루프 시작 전, 관리 테이블이 없다면 미리 생성합니다.
with db_engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
            seq SERIAL PRIMARY KEY,
            id TEXT,
            name TEXT,
            gpkg_name TEXT,
            table_name TEXT,
            owner TEXT,
            qfield_type TEXT,
            reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
        )
    """))

print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...", flush=True)

while True:
    try:
        current_projects = get_all_projects_from_db()
        current_project_ids = [p['id'] for p in current_projects]
        
        ghost_found = False
        if current_project_ids:
            if len(current_project_ids) == 1:
                id_params_str = f"('{current_project_ids[0]}')"
            else:
                id_params_str = str(tuple(current_project_ids))

            with db_engine.begin() as conn:
                ghost_tables_res = conn.execute(text(f"""
                    SELECT table_name, id, name FROM {TARGET_SCHEMA}.qfield_data_manage 
                    WHERE id NOT IN {id_params_str}
                """))
                
                ghost_list = ghost_tables_res.fetchall()
                if ghost_list:
                    for row in ghost_list:
                        t_name, p_id, p_name = row[0], row[1], row[2]
                        conn.execute(text(f'DROP TABLE IF EXISTS {TARGET_SCHEMA}."{t_name}" CASCADE'))
                        conn.execute(text(f"DELETE FROM {TARGET_SCHEMA}.qfield_data_manage WHERE id = :pid"), {"pid": p_id})
                    ghost_found = True

        if ghost_found:
            update_unified_view()

        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            current_job_id = get_latest_job_id(p_id)

            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (소유자: {p['owner']})", flush=True)
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id

    except Exception as e:
        print(f"⚠️ 루프 에러 발생: {e}", flush=True)
        time.sleep(5)
        client = login_client()

    time.sleep(CHECK_INTERVAL)