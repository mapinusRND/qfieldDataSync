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

# qfield_info 테이블이 있는 DB (212 서버의 disaster 스키마)
QFIELD_INFO_SCHEMA = "disaster"
QFIELD_INFO_TABLE = "qfield_info"
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

# ========== [신규] qfield_info 컬럼 목록 조회 ==========
def get_qfield_info_column_lists():
    """
    215 서버 disaster.qfield_info 테이블에서
    qfield_type별 column_list를 딕셔너리로 반환.
    """
    result = {}
    conn = None
    try:
        conn = get_pg_conn()  # ✅ 212 → 215 서버로 변경
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = f"""
            SELECT qfield_type, column_list
            FROM {QFIELD_INFO_SCHEMA}.{QFIELD_INFO_TABLE}
        """
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
            print(f"    📋 [qfield_info 로드] type='{qfield_type}' → columns={col_list}")

        print(f"    ✅ [qfield_info] 총 {len(result)}개 타입 로드 완료")
    except Exception as e:
        print(f"    ❌ [qfield_info 조회 실패] {e}")
    finally:
        if conn:
            conn.close()
    return result


def find_matching_qfield_type(gpkg_columns, qfield_info_map):
    """
    GPKG 레이어 컬럼 목록과 qfield_info의 column_list를 비교하여
    일치하는 qfield_type을 반환. 없으면 None 반환.
    
    비교 기준: qfield_info의 column_list가 gpkg_columns의 부분집합이거나 완전 일치.
    (gpkg에 추가 컬럼이 있어도 허용, qfield_info 기준 컬럼이 모두 존재해야 함)
    """
    gpkg_col_set = set(c.lower() for c in gpkg_columns)
    
    for qfield_type, col_list in qfield_info_map.items():
        required_cols = set(c.lower() for c in col_list)
        if not required_cols:
            continue
        
        # qfield_info의 모든 컬럼이 gpkg에 존재하는지 확인
        if required_cols.issubset(gpkg_col_set):
            return qfield_type, col_list
    
    return None, None
# =======================================================

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

def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    """
    GDF를 DB 테이블로 변환 및 저장 (215 서버)
    allowed_columns: qfield_info의 column_list. None이면 전체 저장.
    """
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None

        # allowed_columns가 있으면 해당 컬럼만 필터링 (geometry, owner, reg_date, update_at 제외 후 추가)
        if allowed_columns is not None:
            allowed_lower = [c.lower() for c in allowed_columns]
            # GDF 컬럼 중 allowed_columns에 있는 것만 선택 (대소문자 무시)
            filtered_cols = [
                c for c in gdf.columns
                if c != geom_col and c.lower() in allowed_lower
            ]
            # 항상 포함할 메타 컬럼 추가 (중복 제거)
            meta_cols = ['owner', 'reg_date', 'update_at']
            for mc in meta_cols:
                if mc in gdf.columns and mc not in filtered_cols:
                    filtered_cols.append(mc)
            source_cols = filtered_cols
        else:
            source_cols = [c for c in gdf.columns if c != geom_col]

        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if c == 'record':
                final_cols.append('audio_txt')

        col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
        for col in final_cols:
            if col == 'audio_txt':
                col_defs.append(f'"{col}" TEXT')
            else:
                dtype = str(gdf[col].dtype)
                if 'int' in dtype:
                    col_defs.append(f'"{col}" BIGINT')
                elif 'float' in dtype:
                    col_defs.append(f'"{col}" DOUBLE PRECISION')
                elif 'datetime' in dtype:
                    col_defs.append(f'"{col}" TIMESTAMP')
                else:
                    col_defs.append(f'"{col}" TEXT')

        if is_geo:
            col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')

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
                            try:
                                stt_val = dc.read_audio(audio_path)
                            except:
                                pass
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
        if conn:
            conn.rollback()
        print(f"        ❌ [DB 저장 실패] {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """다운로드 후 GPKG 분석 및 DB 전송 (215 서버)"""
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # [신규] qfield_info에서 column_list 로드 (212 서버)
    print(f"    🗂️ [qfield_info 조회 중] {QFIELD_INFO_SCHEMA}.{QFIELD_INFO_TABLE}")
    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map:
        print(f"    ⚠️ qfield_info 데이터 없음. 모든 레이어를 스킵합니다.")
        return

    # 1. 관리 테이블 확인 (SQLAlchemy 엔진 사용)
    with db_engine.connect() as conn:
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
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            )
        """))
        conn.commit()

    any_updated, global_table_index = False, 1

    # 2. GPKG 파일 순회
    if not os.path.exists(project_path):
        return
    files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]

    for file in files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        print(f"    📄 [파일 분석] {file}")

        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']:
                    continue
                print(f"        🏷️ [레이어 읽기] {layer_name}")

                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty:
                    continue

                # ===== [신규] 컬럼 비교 로직 =====
                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                geom_col = gdf.geometry.name if is_geo else None
                gpkg_columns = [c for c in gdf.columns if c != geom_col]

                print(f"        📌 [GPKG 컬럼 목록] {gpkg_columns}")

                matched_type, matched_col_list = find_matching_qfield_type(gpkg_columns, qfield_info_map)

                if matched_type is None:
                    print(f"        ⏭️ [스킵] '{layer_name}' - qfield_info와 일치하는 타입 없음")
                    print(f"           → qfield_info 타입 목록: {list(qfield_info_map.keys())}")
                    continue

                print(f"        ✅ [매칭 성공] type='{matched_type}' | 사용 컬럼={matched_col_list}")
                # ==================================

                print(f"        📐 [좌표계 변환] {layer_name}")
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)

                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                # 매니지 테이블 업데이트 (qfield_type 컬럼 추가)
                with db_engine.connect() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage 
                            (id, name, gpkg_name, table_name, owner, qfield_type, reg_date, update_at)
                        VALUES 
                            (:pid, :pname, :gname, :tname, :owner, :qtype, :now, :now)
                        ON CONFLICT (id, gpkg_name) DO UPDATE SET
                            name = EXCLUDED.name,
                            table_name = EXCLUDED.table_name,
                            owner = EXCLUDED.owner,
                            qfield_type = EXCLUDED.qfield_type,
                            update_at = EXCLUDED.update_at
                    """), {
                        "pid": project_id,
                        "pname": project_name,
                        "gname": file_stem,
                        "tname": table_name,
                        "owner": owner,
                        "qtype": matched_type,
                        "now": now
                    })
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
        cur.execute(f"SELECT id, name, gpkg_name, table_name, qfield_type FROM {TARGET_SCHEMA}.qfield_data_manage")  # ✅ qfield_type 추가
        rows = cur.fetchall()
        if not rows:
            return

        view_parts = []
        for r in rows:
            t_name = r['table_name']
            qfield_type = r['qfield_type'] or ''  # ✅ qfield_type 값 추출
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)",
                (t_name,)
            )
            if cur.fetchone()[0]:
                cur.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s AND column_name != 'seq' ORDER BY ordinal_position",
                    (t_name,)
                )
                columns = [f'd."{col[0]}"' for col in cur.fetchall()]
                column_string = ", ".join(columns)
                part = (
                    f"SELECT '{r['id']}'::text as manage_id, "
                    f"'{r['name']}'::text as project_name, "
                    f"'{r['gpkg_name']}'::text as source_gpkg, "
                    f"'{t_name}'::text as source_table, "
                    f"'{qfield_type}'::text as qfield_type, "  # ✅ qfield_type 컬럼 추가
                    f"{column_string} "
                    f"FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                )
                view_parts.append(part)

        if view_parts:
            cur.execute(
                f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{VIEW_NAME} AS " + " UNION ALL ".join(view_parts)
            )
            conn.commit()
            print(f"      ✅ 통합 뷰 갱신 완료")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

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
        try:
            shutil.rmtree(project_path)
        except:
            pass
    os.makedirs(project_path, exist_ok=True)

    # 3. 다운로드 및 처리
    try:
        print(f"    🚀 [다운로드 시도] {p_name} (소유자: {p_owner})")
        if not client:
            client = login_client()

        client.download_project(
            project_id=p_id,
            local_dir=project_path,
            filter_glob="*",
            show_progress=False,
            force_download=True
        )
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
        if not client:
            client = login_client()
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs:
            return "NO_JOB"
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except:
        return "JOB_CHECK_ERROR"

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
    except Exception as e:
        print(f"⚠️ 운영 DB 조회 에러: {e}")
    finally:
        if conn:
            conn.close()
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