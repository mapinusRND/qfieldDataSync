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
from contextlib import contextmanager

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
BASE_OUTPUT_DIR = "D:/work/qfield" if ENV == 'local' else "/app/webfiles/qfield"

TARGET_SCHEMA = "qfield"
CHECK_INTERVAL = 30

QFIELD_INFO_SCHEMA = "disaster"
QFIELD_INFO_TABLE = "qfield_info"

# 타임아웃 설정 (초 단위)
STATEMENT_TIMEOUT_MS = 60_000   # DDL/DML 개별 쿼리 최대 60초
LOCK_TIMEOUT_MS      = 10_000   # 락 대기 최대 10초
CONNECT_TIMEOUT_SEC  = 10       # TCP 연결 최대 10초
BATCH_SIZE           = 200      # INSERT 배치 크기

# 디렉토리 초기화
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
print(f"📂 [경로 확인] {BASE_OUTPUT_DIR}")

# ---------- DB 연결 관리 ----------

# SQLAlchemy 엔진: 커넥션 풀 + 타임아웃 옵션
_connect_args = {
    "connect_timeout": CONNECT_TIMEOUT_SEC,
    "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c lock_timeout={LOCK_TIMEOUT_MS}",
}
db_engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,           # 과도한 풀 크기 줄임 (원본 10)
    max_overflow=10,        # (원본 20)
    connect_args=_connect_args,
)


@contextmanager
def get_pg_conn_safe(autocommit=False):
    """
    psycopg2 커넥션을 안전하게 관리.
    - connect_timeout: TCP 연결 hang 방지
    - statement_timeout / lock_timeout: 쿼리/락 대기 hang 방지
    - autocommit 옵션: DDL 전용 커넥션에 사용
    """
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=CONNECT_TIMEOUT_SEC,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c lock_timeout={LOCK_TIMEOUT_MS}",
    )
    conn.autocommit = autocommit
    try:
        yield conn
    except Exception:
        if not autocommit:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_qfc_db_conn():
    """운영 메타데이터 조회용(212) 커넥션"""
    return psycopg2.connect(
        host=QFC_DB_HOST, port=QFC_DB_PORT,
        dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS,
        connect_timeout=CONNECT_TIMEOUT_SEC,
    )


# 초기 스키마 생성
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

# ---------- SDK 및 공통 함수 ----------

def login_client():
    try:
        new_client = sdk.Client(url=URL)
        new_client.login(username=USERNAME, password=PASSWORD)
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None


client = login_client()


def get_qfield_info_column_lists():
    result = {}
    try:
        with get_pg_conn_safe() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = f"SELECT qfield_type, column_list FROM {QFIELD_INFO_SCHEMA}.{QFIELD_INFO_TABLE}"
                cur.execute(query)
                for row in cur.fetchall():
                    qfield_type, raw_list = row['qfield_type'], row['column_list']
                    if isinstance(raw_list, list):
                        col_list = raw_list
                    elif isinstance(raw_list, str):
                        cleaned = raw_list.strip()
                        if cleaned.startswith('{') and cleaned.endswith('}'):
                            col_list = [c.strip().strip('"') for c in cleaned[1:-1].split(',') if c.strip()]
                        else:
                            col_list = [c.strip() for c in cleaned.split(',') if c.strip()]
                    else:
                        col_list = []
                    result[qfield_type] = col_list
        print(f"    ✅ [qfield_info] {len(result)}개 타입 로드 완료")
    except Exception as e:
        print(f"    ❌ [qfield_info 조회 실패] {e}")
    return result


def find_matching_qfield_type(gpkg_columns, qfield_info_map):
    gpkg_col_set = set(c.lower() for c in gpkg_columns)
    for qfield_type, col_list in qfield_info_map.items():
        required_cols = set(c.lower() for c in col_list)
        if required_cols and required_cols.issubset(gpkg_col_set):
            return qfield_type, col_list
    return None, None


# ---------- 데이터 적재 및 권한 함수 ----------

def grant_admin_permission_via_db(project_id):
    try:
        conn = get_qfc_db_conn()
        with conn:
            with conn.cursor() as cur:
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
        conn.close()
    except Exception as e:
        print(f"    ⚠️ 권한 부여 중 에러: {e}")


def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    """
    GeoDataFrame을 DB에 저장.
    핵심 변경:
    - DROP/CREATE는 autocommit=True 커넥션으로 별도 실행 → 락 경합 최소화
    - INSERT는 executemany 배치로 분할 → 단일 장시간 트랜잭션 방지
    """
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")

    try:
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None

        # 컬럼 필터링
        if allowed_columns is not None:
            allowed_lower = [c.lower() for c in allowed_columns]
            filtered_cols = [c for c in gdf.columns if c != geom_col and c.lower() in allowed_lower]
            for mc in ['owner', 'reg_date']:
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

        # ── STEP 0: 스키마 내 idle 블로킹 커넥션 선제 종료 ──
        # 뷰를 통해 참조 테이블에 락이 걸린 외부 커넥션(DBeaver 등)을 먼저 제거
        _terminate_schema_idle_blockers(schema, label=f"before save {table_name}")

        # ── STEP 1: DROP / CREATE (autocommit으로 즉시 반영, 락 대기 최소화) ──
        col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
        for col in final_cols:
            if col.endswith('_txt'):
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

        with get_pg_conn_safe(autocommit=True) as ddl_conn:
            with ddl_conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
                cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        # ── STEP 2: 데이터 준비 ──
        rows_to_insert = []
        insert_cols = ['platform_type'] + [f'"{c}"' for c in final_cols]
        if is_geo:
            insert_cols.append(f'"{geom_col}"')

        for _, row in gdf.iterrows():
            values = [1]
            for col in final_cols:
                if col.endswith('_txt'):
                    origin_record_col = col[:-4]
                    record_file = row.get(origin_record_col)
                    stt_val = ""
                    if record_file and isinstance(record_file, str) and record_file.strip():
                        audio_path = os.path.join(project_path, record_file)
                        if not os.path.exists(audio_path) and dc:
                            filename = os.path.basename(record_file)
                            for root, _, files in os.walk(project_path):
                                if filename in files:
                                    audio_path = os.path.join(root, filename)
                                    break
                        if os.path.exists(audio_path) and dc:
                            try:
                                stt_val = dc.read_audio(audio_path)
                            except Exception:
                                pass
                    values.append(stt_val)
                else:
                    val = row[col]
                    values.append(None if pd.isna(val) else val)

            if is_geo:
                geom = row[geom_col]
                if geom:
                    values.append(wkb_dumps(geom, hex=True, srid=3857))
                else:
                    values.append(None)

            rows_to_insert.append(values)

        # ── STEP 3: 배치 INSERT (BATCH_SIZE 단위로 트랜잭션 분할) ──
        placeholders = ', '.join(
            ['%s::geometry' if (is_geo and i == len(insert_cols) - 1) else '%s'
             for i in range(len(insert_cols))]
        )
        insert_sql = f'INSERT INTO {schema}."{table_name}" ({", ".join(insert_cols)}) VALUES ({placeholders})'

        total = len(rows_to_insert)
        for batch_start in range(0, total, BATCH_SIZE):
            batch = rows_to_insert[batch_start:batch_start + BATCH_SIZE]
            with get_pg_conn_safe() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, insert_sql, batch, page_size=BATCH_SIZE)
                conn.commit()
            print(f"        ↳ 배치 INSERT {min(batch_start + BATCH_SIZE, total)}/{total}")

        print(f"        ✅ [DB 저장 성공] {table_name}")

    except Exception as e:
        print(f"        ❌ [DB 저장 실패] {table_name}: {e}")


# ---------- 워크플로우 제어 함수 ----------

def _build_view_sql_parts(q_type, table_rows):
    """
    뷰 SQL 조각을 미리 조립 (information_schema 조회만 수행, 실제 테이블 락 없음).
    별도 커넥션으로 메타데이터만 읽어서 반환.
    """
    view_parts = []
    try:
        with get_pg_conn_safe() as conn:
            with conn.cursor() as cur:
                for r in table_rows:
                    t_name = r['table_name']
                    # information_schema 조회는 락을 유발하지 않음
                    cur.execute(
                        "SELECT EXISTS ("
                        "  SELECT 1 FROM information_schema.tables"
                        f" WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s"
                        ")",
                        (t_name,),
                    )
                    if not cur.fetchone()[0]:
                        continue
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns"
                        f" WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s"
                        "  AND column_name != 'seq'"
                        " ORDER BY ordinal_position",
                        (t_name,),
                    )
                    cols = [f'd."{col[0]}"' for col in cur.fetchall()]
                    if not cols:
                        continue
                    view_parts.append(
                        f"SELECT '{r['id']}'::text as manage_id, "
                        f"'{r['name']}'::text as project_name, "
                        f"'{r['gpkg_name']}'::text as source_gpkg, "
                        f"'{t_name}'::text as source_table, "
                        f"'{q_type}'::text as qfield_type, "
                        f"{', '.join(cols)} "
                        f"FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                    )
    except Exception as e:
        print(f"      ⚠️ 뷰 SQL 조립 오류 ({q_type}): {e}")
    return view_parts


def _terminate_schema_idle_blockers(schema=TARGET_SCHEMA, label=""):
    """
    특정 스키마의 테이블/뷰에 락을 잡고
    'idle in transaction' 또는 'idle' 상태인 외부 커넥션을 강제 종료.

    - DBeaver 등 클라이언트가 뷰를 통해 참조 테이블까지 락을 보유하는 경우 대응
    - idle in transaction : 트랜잭션 열고 방치
    - idle               : 쿼리 완료 후 커넥션 풀에서 락을 미반환하는 경우
    - active 상태(실행 중인 쿼리)는 건드리지 않음
    - pg_terminate_backend() 는 superuser 권한 필요
    """
    terminate_sql = """
        SELECT sa.pid, sa.state, sa.application_name,
               pg_terminate_backend(sa.pid) AS terminated
        FROM pg_locks lk
        JOIN pg_stat_activity sa ON lk.pid = sa.pid
        JOIN pg_class pc ON lk.relation = pc.oid
        JOIN pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE pn.nspname = %s
          AND sa.state IN ('idle in transaction', 'idle')
          AND sa.pid <> pg_backend_pid()
        GROUP BY sa.pid, sa.state, sa.application_name
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASS,
            connect_timeout=CONNECT_TIMEOUT_SEC,
        )
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(terminate_sql, (schema,))
            rows = cur.fetchall()
            terminated = [r for r in rows if r['terminated']]
            if terminated:
                for r in terminated:
                    print(f"      🔌 커넥션 강제 종료 [{r['state']}] pid={r['pid']} app={r['application_name']} ({label})")
            else:
                print(f"      ℹ️ 종료할 블로킹 커넥션 없음 ({label or schema})")
        conn.close()
        if terminated:
            time.sleep(1)
        return len(terminated)
    except Exception as e:
        print(f"      ⚠️ 블로킹 커넥션 종료 실패 (권한 부족?): {e}")
        return 0


def _drop_create_view(view_name, view_sql, max_retries=3, retry_delay=3):
    """
    DROP VIEW IF EXISTS → CREATE VIEW 순서로 실행.
    락 충돌(lock_timeout) 발생 시:
      → idle in transaction 상태의 블로킹 커넥션을 강제 종료 후 재시도
      → 최대 max_retries 회까지 반복
    """
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                dbname=DB_NAME, user=DB_USER, password=DB_PASS,
                connect_timeout=CONNECT_TIMEOUT_SEC,
                options=f"-c lock_timeout=5000 -c statement_timeout={STATEMENT_TIMEOUT_MS}",
            )
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE")
                    cur.execute(view_sql)
                print(f"      ✅ 뷰 생성: {view_name}")
                return True
            finally:
                conn.close()

        except psycopg2.errors.LockNotAvailable:
            print(f"      🔁 뷰 락 충돌 감지 ({attempt}/{max_retries}): {view_name}")
            _terminate_schema_idle_blockers(label=view_name)  # idle 커넥션 강제 종료
            time.sleep(retry_delay)

        except Exception as e:
            print(f"      ⚠️ 뷰 생성 오류 ({view_name}, 시도 {attempt}): {e}")
            time.sleep(retry_delay)

    print(f"      ❌ 뷰 생성 최종 실패 (재시도 {max_retries}회 소진): {view_name}")
    return False


def update_unified_view():
    """
    뷰 갱신 전략:
    1. information_schema로 컬럼 목록 조회 (락 없음) → SQL 미리 조립
    2. DROP VIEW IF EXISTS → CREATE VIEW 순서로 DDL 실행
    3. lock_timeout=5초 + 최대 3회 재시도 → 외부 클라이언트 락 충돌 자동 복구
    4. 타입별 독립 커넥션 → 하나 실패해도 나머지 뷰는 정상 생성
    """
    print(f"    📊 [개별 뷰 갱신 시작]")
    try:
        # ── STEP 1: 관리 테이블에서 타입/테이블 목록 읽기 ──
        type_tables = {}
        with get_pg_conn_safe() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    f"SELECT DISTINCT qfield_type FROM {TARGET_SCHEMA}.qfield_data_manage"
                    f" WHERE qfield_type IS NOT NULL"
                )
                types = [r['qfield_type'] for r in cur.fetchall()]
                for q_type in types:
                    cur.execute(
                        f"SELECT id, name, gpkg_name, table_name"
                        f" FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type = %s",
                        (q_type,),
                    )
                    type_tables[q_type] = cur.fetchall()

        # ── STEP 2: 타입별로 뷰 SQL 조립 후 DDL 실행 ──
        # 뷰 DDL 전 스키마 내 idle 커넥션 일괄 종료 (DBeaver 등 외부 클라이언트 대응)
        _terminate_schema_idle_blockers(label="before view DDL")

        for q_type, table_rows in type_tables.items():
            view_name = f"{TARGET_SCHEMA}.{q_type}_v_qfield_data"

            # 메타데이터만 조회해 SQL 조각 생성 (테이블 락 없음)
            view_parts = _build_view_sql_parts(q_type, table_rows)
            if not view_parts:
                print(f"      ℹ️ 뷰 생성 스킵 (참조 테이블 없음): {view_name}")
                continue

            view_sql = (
                f"CREATE VIEW {view_name} AS "
                + " UNION ALL ".join(view_parts)
            )
            # DROP → CREATE + 재시도
            _drop_create_view(view_name, view_sql)

    except Exception as e:
        print(f"      ⚠️ 뷰 갱신 전체 오류: {e}")


def process_gpkg_to_db(project_id, project_path, project_name, owner):
    print(f"    🔍 [분석 시작] {project_name}")
    short_id, now = project_id[:13], datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map:
        return False

    any_updated, global_table_index = False, 1
    if not os.path.exists(project_path):
        return False

    gpkg_files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]
    for file in gpkg_files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
        except Exception as e:
            print(f"        ⚠️ {file} fiona 열기 실패: {e}")
            continue

        for layer_name in layers:
            if layer_name.lower() in ['layer_styles', 'gpkg_contents', 'geopackage_contents']:
                continue
            try:
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty:
                    continue

                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                gpkg_columns = [c for c in gdf.columns if c != (gdf.geometry.name if is_geo else None)]
                matched_type, matched_cols = find_matching_qfield_type(gpkg_columns, qfield_info_map)

                if matched_type:
                    print(f"        ✅ [매칭] layer='{layer_name}' type='{matched_type}'")
                    if is_geo:
                        gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                    gdf = gdf.assign(owner=owner, reg_date=now)
                    table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                    save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_cols)

                    try:
                        with db_engine.begin() as conn:
                            conn.execute(
                                text(
                                    f"INSERT INTO {TARGET_SCHEMA}.qfield_data_manage "
                                    f"(id, name, gpkg_name, table_name, owner, qfield_type, reg_date) "
                                    f"VALUES (:pid, :pname, :gname, :tname, :owner, :qtype, :now) "
                                    f"ON CONFLICT (id, gpkg_name) DO NOTHING"
                                ),
                                {"pid": project_id, "pname": project_name, "gname": file_stem,
                                 "tname": table_name, "owner": owner, "qtype": matched_type, "now": now},
                            )
                    except Exception as e:
                        print(f"        ⚠️ qfield_data_manage 삽입 실패: {e}")

                    any_updated = True
                    global_table_index += 1

            except Exception as e:
                print(f"        ⚠️ {file}/{layer_name} 에러: {e}")

    if any_updated:
        update_unified_view()
    return any_updated


def sync_single_project(project_data):
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
    grant_admin_permission_via_db(p_id)

    if os.path.exists(project_path):
        shutil.rmtree(project_path, ignore_errors=True)
    os.makedirs(project_path, exist_ok=True)

    try:
        if not client:
            client = login_client()
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", force_download=True)
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e:
        if "401" in str(e):
            client = login_client()
        print(f"    ⚠️ {p_name} 실패: {e}")


def get_latest_job_id(project_id):
    global client
    try:
        if not client:
            client = login_client()
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs:
            return "NO_JOB"
        return sorted(delta_jobs, key=lambda j: j.get('created_at', ''), reverse=True)[0]['id']
    except Exception:
        return "JOB_ERROR"


def get_all_projects_from_db():
    projects = []
    try:
        conn = get_qfc_db_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT p.id, p.name, u.username as owner_name "
                "FROM public.core_project p JOIN public.core_user u ON p.owner_id = u.id"
            )
            for r in cur.fetchall():
                projects.append({'id': str(r['id']), 'name': r['name'], 'owner': r['owner_name']})
        conn.close()
    except Exception as e:
        print(f"⚠️ 운영 DB 조회 에러: {e}")
    return projects


# ========== 관리 테이블 초기화 ==========
with db_engine.begin() as conn:
    conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage ("
        f"seq SERIAL PRIMARY KEY, id TEXT, name TEXT, gpkg_name TEXT, "
        f"table_name TEXT, owner TEXT, qfield_type TEXT, "
        f"reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        f"CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name))"
    ))

# ========== 메인 실행 루프 ==========
last_jobs_cache = {}

print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...", flush=True)

while True:
    try:
        current_projects = get_all_projects_from_db()
        current_ids = [p['id'] for p in current_projects]

        # 1. 유령 프로젝트 정리 (삭제된 프로젝트의 테이블/행 제거)
        if current_ids:
            id_placeholders = ', '.join([f"'{i}'" for i in current_ids])
            id_list_sql = f"({id_placeholders})"
            try:
                with db_engine.begin() as conn:
                    ghosts = conn.execute(
                        text(f"SELECT table_name, id FROM {TARGET_SCHEMA}.qfield_data_manage WHERE id NOT IN {id_list_sql}")
                    ).fetchall()
                    if ghosts:
                        for g in ghosts:
                            conn.execute(text(f'DROP TABLE IF EXISTS {TARGET_SCHEMA}."{g[0]}" CASCADE'))
                            conn.execute(text(f"DELETE FROM {TARGET_SCHEMA}.qfield_data_manage WHERE id = :pid"), {"pid": g[1]})
                        # last_jobs_cache에서도 제거
                        ghost_ids = set(g[1] for g in ghosts)
                        for gid in ghost_ids:
                            last_jobs_cache.pop(gid, None)
                        update_unified_view()
            except Exception as e:
                print(f"    ⚠️ 유령 프로젝트 정리 오류: {e}")

        # 2. 프로젝트별 동기화
        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            try:
                current_job_id = get_latest_job_id(p_id)
                needs_sync = (
                    p_id not in last_jobs_cache
                    or not os.path.exists(project_path)
                    or current_job_id != last_jobs_cache[p_id]
                )
                if needs_sync:
                    print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']}")
                    sync_single_project(p)
                    last_jobs_cache[p_id] = current_job_id
            except Exception as e:
                print(f"    ⚠️ 프로젝트 처리 오류 ({p['name']}): {e}")

    except Exception as e:
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(10)
        client = login_client()

    finally:
        # 루프 끝에 엔진 풀 정리 (좀비 커넥션 방지)
        try:
            db_engine.dispose()
        except Exception:
            pass

    print(f"[{datetime.now()}] 💤 대기 중 ({CHECK_INTERVAL}초)...", flush=True)
    time.sleep(CHECK_INTERVAL)