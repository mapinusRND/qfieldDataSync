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
from qfieldcloud_sdk.sdk import JobTypes
from shapely.wkb import dumps as wkb_dumps

# ========== 1. 설정 ==========
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

BASE_OUTPUT_DIR = "./output"
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TARGET_SCHEMA = "qfield"
VIEW_NAME = "v_qfield_total_data" 
CHECK_INTERVAL = 30
# ============================

client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)

db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)

def get_pg_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000"
    )

with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

def update_unified_view():
    """관리 테이블 정보와 데이터 테이블을 결합한 통합 뷰 생성 (seq 컬럼 제외)"""
    print(f"    📊 통합 뷰({VIEW_NAME}) 갱신 중...")
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 1. 관리 테이블 정보 조회
        cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage")
        rows = cur.fetchall()
        if not rows: return

        view_parts = []
        for r in rows:
            t_name = r['table_name']
            
            # 2. 해당 테이블이 존재하는지 확인
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
            if cur.fetchone()[0]:
                
                # 3. [핵심] 'seq' 컬럼을 제외한 나머지 컬럼 리스트 추출
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{TARGET_SCHEMA}' 
                      AND table_name = %s 
                      AND column_name != 'seq'
                    ORDER BY ordinal_position
                """, (t_name,))
                
                columns = [f'd."{col[0]}"' for col in cur.fetchall()]
                column_string = ", ".join(columns)

                # 4. 쿼리 조각 생성
                part = f"""
                SELECT 
                    '{r['id']}'::text as manage_id,
                    '{r['name']}'::text as project_name,
                    '{r['gpkg_name']}'::text as source_gpkg,
                    '{t_name}'::text as source_table,
                    {column_string}
                FROM {TARGET_SCHEMA}."{t_name}" d
                """
                view_parts.append(part)

        if view_parts:
            # 5. 전체 뷰 생성
            create_view_sql = f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{VIEW_NAME} AS " + " UNION ALL ".join(view_parts)
            cur.execute(create_view_sql)
            conn.commit()
            print(f"      ✅ 통합 뷰 생성 완료 (seq 제외): {TARGET_SCHEMA}.{VIEW_NAME}")
            
    except Exception as e:
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        cur.close(); conn.close()

def save_gdf_direct(gdf, table_name, schema):
    """psycopg2로 직접 데이터 저장 (seq는 PK로만 유지, platform_type:1 추가)"""
    conn = get_pg_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None and not gdf.geometry.isnull().all())
        geom_col = gdf.geometry.name if is_geo else None
        data_cols = [c for c in gdf.columns if c != geom_col] if is_geo else list(gdf.columns)

        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", (schema, table_name))
        exists = cur.fetchone()[0]

        if exists:
            cur.execute(f'DELETE FROM {schema}."{table_name}"')
        else:
            # 물리 테이블에는 seq가 PK로 존재 (웹 관리 등을 위해 유지)
            col_defs = [
                'seq SERIAL PRIMARY KEY',
                'platform_type SMALLINT DEFAULT 1'
            ]
            for col in data_cols:
                dtype = str(gdf[col].dtype)
                if 'int' in dtype: col_defs.append(f'"{col}" BIGINT')
                elif 'float' in dtype: col_defs.append(f'"{col}" DOUBLE PRECISION')
                elif 'datetime' in dtype or 'date' in dtype: col_defs.append(f'"{col}" TIMESTAMP')
                else: col_defs.append(f'"{col}" TEXT')
            
            if is_geo:
                col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')
            
            cur.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({", ".join(col_defs)})')

        for _, row in gdf.iterrows():
            values = [1] 
            placeholders = ['%s']
            cols = ['platform_type']

            for col in data_cols:
                val = row[col]
                if pd.isna(val) if not hasattr(val, '__iter__') or isinstance(val, str) else False: val = None
                values.append(val); placeholders.append('%s'); cols.append(f'"{col}"')
            
            if is_geo:
                geom = row[geom_col]
                if geom is not None and not pd.isna(str(geom)):
                    values.append(wkb_dumps(geom, hex=True, include_srid=True))
                else: values.append(None)
                placeholders.append('%s::geometry'); cols.append(f'"{geom_col}"')
            
            cur.execute(f'INSERT INTO {schema}."{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})', values)
        
        conn.commit()
    except Exception as e:
        conn.rollback(); raise e
    finally:
        cur.close(); conn.close()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    print(f"    🐘 DB 작업 시작 (ID: {project_id[:13]}...)")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    with db_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                seq SERIAL PRIMARY KEY,
                id TEXT, name TEXT, gpkg_name TEXT,
                table_name TEXT, owner TEXT,
                reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            );
        """))

    global_table_index = 1
    any_updated = False

    for file in os.listdir(project_path):
        if not file.endswith(".gpkg"): continue
        gpkg_path = os.path.join(project_path, file)
        file_stem_only = os.path.splitext(file)[0]
        representative_table_name = None

        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'rtree_...']: continue
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty or 'style' in layer_name.lower(): continue
                
                # 좌표계 3857 변환
                if gdf.crs is not None:
                    gdf = gdf.to_crs(epsg=3857)
                else:
                    gdf.set_crs(epsg=5186, inplace=True)
                    gdf = gdf.to_crs(epsg=3857)

                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                if representative_table_name is None: representative_table_name = table_name

                for attempt in range(3):
                    try:
                        save_gdf_direct(gdf, table_name, TARGET_SCHEMA)
                        any_updated = True
                        break
                    except Exception as e:
                        if attempt < 2: time.sleep(3)
                        else: raise e
                global_table_index += 1

            if representative_table_name:
                with db_engine.begin() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage (id, name, gpkg_name, table_name, owner, reg_date, update_at)
                        VALUES (:pid, :pname, :gname, :tname, :owner, :now, :now)
                        ON CONFLICT (id, gpkg_name) DO UPDATE
                        SET name = EXCLUDED.name, table_name = EXCLUDED.table_name, owner = EXCLUDED.owner, update_at = EXCLUDED.update_at;
                    """), {"pid": project_id, "pname": project_name, "gname": file_stem_only, "tname": representative_table_name, "owner": owner, "now": now})

        except Exception as e:
            print(f"      ⚠️ {file} 처리 오류: {e}")

    if any_updated:
        update_unified_view()

# --- 하단 메인 로직 ---
def get_all_projects():
    try: return client.list_projects()
    except: return []

def get_project_dir(project_id):
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def sync_single_project(project_data):
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data.get('owner', 'unknown')
    project_path = get_project_dir(p_id)
    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    try:
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e: print(f"    ⚠️ 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    try:
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return None
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except: return None

last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 통합 모니터링 시작...")

while True:
    try:
        current_projects = get_all_projects()
        for p in current_projects:
            p_id, project_path = p['id'], get_project_dir(p['id'])
            current_job_id = get_latest_job_id(p_id)
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (Job: {current_job_id})")
                if p_id in last_jobs_cache: time.sleep(5)
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id
    except Exception as e: print(f"⚠️ 에러: {e}")
    time.sleep(CHECK_INTERVAL)