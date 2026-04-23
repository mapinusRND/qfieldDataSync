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
# 'disaster2convert' 모듈은 현장에서 녹음된 음성 파일을 텍스트로 변환하는 데 사용됩니다.
try:
    import disaster2convert as dc
except ImportError:
    dc = None
    print("⚠️ disaster2convert 모듈을 찾을 수 없습니다. STT 기능이 제외됩니다.")

# ========== 1. 설정 (Configuration) ==========
# QFieldCloud 서버 접속 정보 (API Endpoint 및 관리자 계정)
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# [운영 DB] 212 서버: QFieldCloud 프레임워크 자체의 메타데이터(프로젝트 목록, 유저, 권한)가 들어있는 DB
QFC_DB_HOST = "10.10.10.212"
QFC_DB_PORT = 5433
QFC_DB_NAME = "qfieldcloud_db"
QFC_DB_USER = "root"
QFC_DB_PASS = "1q2w3e4r"

# [저장 및 분석 DB] 215 서버: 가공된 GIS 데이터와 STT 결과물이 최종적으로 적재될 대상 DB (PostGIS 환경)
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ========== 기타 경로 및 환경 설정 ==========
# 실행 환경(로컬 vs 도커)에 따라 파일 저장 경로를 동적으로 설정합니다.
# 로컬 개발 시에는 D 드라이브를, 도커 배포 시에는 /app 내부 경로를 사용합니다.
ENV = os.getenv('FLASK_ENV', 'local')
if ENV == 'local':
    BASE_OUTPUT_DIR = "D:/work/qfield"
else:
    BASE_OUTPUT_DIR = "/app/webfiles/qfield"

TARGET_SCHEMA = "qfield"         # 215 DB 내에서 분석 완료된 데이터를 관리할 전용 스키마 명칭
CHECK_INTERVAL = 30              # 새로운 데이터(Job)가 있는지 체크하는 루프 주기 (초 단위)

# [기준 정보 테이블] 215 서버의 disaster.qfield_info 테이블:
# 수집된 데이터가 '화재', '침수' 등 어떤 타입인지 컬럼 구성을 통해 판별하기 위한 딕셔너리형 기준 정보
QFIELD_INFO_SCHEMA = "disaster"
QFIELD_INFO_TABLE = "qfield_info"

# 타임아웃 설정: DB 연결/쿼리/락 대기 시 무한정 hang 방지
STATEMENT_TIMEOUT_MS = 60_000   # 개별 쿼리 최대 60초
LOCK_TIMEOUT_MS      = 10_000   # 락 대기 최대 10초
CONNECT_TIMEOUT_SEC  = 10       # TCP 연결 최대 10초
BATCH_SIZE           = 200      # INSERT 배치 크기

# 디렉토리 초기화: 데이터를 다운로드할 기본 경로가 없으면 생성합니다.
if not os.path.exists(BASE_OUTPUT_DIR):
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    print(f"📂 [경로 생성] {BASE_OUTPUT_DIR}")

# ---------- SDK 및 DB 연결 관련 함수 ----------

def login_client():
    """
    QFieldCloud API에 로그인하여 인증된 세션(Client)을 생성합니다.
    SDK를 통해 프로젝트 리스트 조회, 파일 다운로드, 작업(Job) 상태 확인 등을 수행합니다.
    실패 시 None을 반환하며, 이후 메인 루프에서 재시도하게 됩니다.
    """
    try:
        new_client = sdk.Client(url=URL)
        new_client.login(username=USERNAME, password=PASSWORD)
        print(f"    ✅ QFieldCloud 로그인 성공")
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None

# 전역 SDK 클라이언트 초기화
client = login_client()

# SQLAlchemy 엔진: 데이터베이스 연결 풀링을 통해 대량의 INSERT/UPDATE 작업 시 안정성을 확보합니다.
# pool_pre_ping은 끊긴 연결을 자동으로 감지하여 재연결하는 역할을 합니다.
# connect_args로 타임아웃을 설정하여 hang 방지합니다.
_connect_args = {
    "connect_timeout": CONNECT_TIMEOUT_SEC,
    "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c lock_timeout={LOCK_TIMEOUT_MS}",
}
db_engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,
    max_overflow=10,
    connect_args=_connect_args,
)


@contextmanager
def get_pg_conn_safe(autocommit=False):
    """
    215 서버용 psycopg2 커넥션을 안전하게 관리합니다.
    - connect_timeout: TCP 연결 hang 방지
    - statement_timeout / lock_timeout: 쿼리/락 대기 hang 방지
    - autocommit 옵션: DDL(DROP/CREATE) 전용 커넥션에 사용
    - 예외 발생 시 자동 rollback 및 커넥션 close 보장
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


def get_pg_conn():
    """최종 데이터 적재 및 조회용(215 서버) psycopg2 커넥션 생성 (Raw SQL 처리용)"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=CONNECT_TIMEOUT_SEC,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c lock_timeout={LOCK_TIMEOUT_MS}",
    )

def get_qfc_db_conn():
    """운영 메타데이터 조회용(212 서버) psycopg2 커넥션 생성 (사용자 및 프로젝트 정보 확인용)"""
    return psycopg2.connect(
        host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS,
        connect_timeout=CONNECT_TIMEOUT_SEC,
    )

# 프로그램 시작 시 215 서버에 데이터 저장용 스키마(qfield)가 없다면 미리 생성합니다.
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

# ---------- qfield_info (기준 정보) 관리 함수 ----------
def get_qfield_info_column_lists():
    """
    disaster.qfield_info 테이블에서 '재난타입별 필수 컬럼 리스트'를 읽어옵니다.
    반환값 예시: {'rain': ['depth', 'time'], 'fire': ['cause', 'size']}
    이 정보는 다운로드된 GPKG 파일이 어떤 종류의 재난 데이터인지 분류하는 기준이 됩니다.
    """
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

            # DB의 컬럼 리스트 데이터 형식(PostgreSQL 배열 {} 또는 문자열 ,)에 따른 파싱 처리
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
    현재 분석 중인 GPKG 레이어의 컬럼 목록과 DB 기준 정보(qfield_info)를 비교합니다.
    GPKG 레이어가 특정 재난 타입의 '필수 컬럼'들을 모두 포함하고 있다면 해당 타입으로 간주합니다.
    이를 통해 데이터의 정체성(예: 강우 조사 데이터인지 화재 조사 데이터인지)을 식별합니다.
    """
    gpkg_col_set = set(c.lower() for c in gpkg_columns)

    for qfield_type, col_list in qfield_info_map.items():
        required_cols = set(c.lower() for c in col_list)
        if not required_cols:
            continue
        # 기준 정보에 정의된 컬럼들이 실제 파일 내 컬럼의 부분집합(subset)인지 확인
        if required_cols.issubset(gpkg_col_set):
            return qfield_type, col_list

    return None, None


# ---------- 권한 및 데이터 저장 함수 ----------

def grant_admin_permission_via_db(project_id):
    """
    QFieldCloud 서비스 내에서 특정 유저가 프로젝트 접근 권한이 없을 경우 다운로드가 실패합니다.
    이를 방지하기 위해 212 운영 DB의 협업자 테이블에 'admin' 계정의 관리자 권한을 강제로 삽입(UPSERT)합니다.
    API를 통한 권한 설정이 실패할 경우를 대비한 하이패스 로직입니다.
    """
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor()
        # admin 유저의 내부 고유 ID(UUID/Integer) 조회
        cur.execute("SELECT id FROM public.core_user WHERE username = %s", (USERNAME,))
        admin_res = cur.fetchone()
        if admin_res:
            admin_id = admin_res[0]
            # 협업자 테이블에 admin 권한 데이터 삽입 (이미 존재 시 무시)
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
        if conn:
            conn.close()


def _terminate_schema_idle_blockers(schema=TARGET_SCHEMA, label=""):
    """
    특정 스키마의 테이블/뷰에 락을 잡고 'idle in transaction' 또는 'idle' 상태인
    외부 커넥션(DBeaver 등)을 강제 종료합니다.
    - idle in transaction: 트랜잭션을 열고 방치한 상태
    - idle: 쿼리 완료 후 커넥션 풀에서 락을 미반환하는 경우
    - active 상태(실행 중인 쿼리)는 건드리지 않음
    - pg_terminate_backend()는 superuser 권한 필요
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
            time.sleep(2)  # 종료 신호 OS 반영 대기
            # 강제 종료된 커넥션이 db_engine 풀에 있을 수 있으므로 풀 전체 재생성
            try:
                db_engine.dispose()
            except Exception:
                pass
        return len(terminated)
    except Exception as e:
        print(f"      ⚠️ 블로킹 커넥션 종료 실패 (권한 부족?): {e}")
        return 0


def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    """
    정제된 GeoDataFrame을 215 DB에 실제 물리 테이블로 생성하고 데이터를 배치로 INSERT 합니다.
    주요 기능:
    1. 테이블 생성: 매번 테이블을 DROP 후 새로 생성하여 스키마 변경에 대응(Overwrite).
    2. STT 연동: 컬럼명에 'record'가 포함된 경우 해당 경로의 음성파일을 찾아 텍스트로 변환 후 '{명칭}_txt' 컬럼에 저장.
    3. 좌표계: PostGIS 지오메트리를 WKB 포맷으로 변환하고 EPSG:3857(구글/웹메르카토르)로 고정하여 적재.
    4. 메타데이터: 소유자(owner), 등록일(reg_date), 수정일(update_at)을 강제 포함.
    5. 배치 INSERT: BATCH_SIZE 단위로 트랜잭션을 분할하여 단일 장시간 트랜잭션으로 인한 락 경합 방지.
    """
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None

        # 1. 컬럼 필터링 (불필요한 시스템 컬럼 제외 및 필수 컬럼 선정)
        if allowed_columns is not None:
            allowed_lower = [c.lower() for c in allowed_columns]
            filtered_cols = [c for c in gdf.columns if c != geom_col and c.lower() in allowed_lower]
            meta_cols = ['owner', 'reg_date', 'update_at']
            for mc in meta_cols:
                if mc in gdf.columns and mc not in filtered_cols:
                    filtered_cols.append(mc)
            source_cols = filtered_cols
        else:
            source_cols = [c for c in gdf.columns if c != geom_col]

        # 2. 음성 결과물 저장을 위한 가상 컬럼(_txt) 구조 정의
        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if 'record' in c.lower():
                final_cols.append(c + '_txt')

        # 3. 외부 idle 커넥션 선제 종료 후 DDL 실행 (DBeaver 등의 락 충돌 방지)
        _terminate_schema_idle_blockers(schema, label=f"before save {table_name}")

        # 4. DB 컬럼 타입 정의 및 테이블 생성 (autocommit으로 즉시 반영, 락 경합 최소화)
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

        # 기존 테이블을 삭제하고 새로운 구조로 재생성 (스키마 유연성 확보)
        with get_pg_conn_safe(autocommit=True) as ddl_conn:
            with ddl_conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
                cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        # 5. 데이터 행 준비
        rows_to_insert = []
        insert_cols = ['platform_type'] + [f'"{c}"' for c in final_cols]
        if is_geo:
            insert_cols.append(f'"{geom_col}"')

        for _, row in gdf.iterrows():
            values = [1]
            for col in final_cols:
                # STT(음성 텍스트화) 처리 로직
                if col.endswith('_txt') and 'record' in col.lower():
                    origin_record_col = col[:-4]
                    record_file = row.get(origin_record_col)
                    stt_val = ""
                    if record_file and isinstance(record_file, str) and record_file.strip():
                        audio_path = os.path.join(project_path, record_file)
                        # GPKG 내 저장된 경로가 유효하지 않을 경우 하위 폴더 전체 재탐색
                        if not os.path.exists(audio_path):
                            filename = os.path.basename(record_file)
                            for root, dirs, files in os.walk(project_path):
                                if filename in files:
                                    audio_path = os.path.join(root, filename)
                                    break
                        # 음성 파일이 실제 존재하고 모듈이 로드된 경우 텍스트 추출 수행
                        if os.path.exists(audio_path) and dc:
                            try:
                                stt_val = dc.read_audio(audio_path)
                                if stt_val: print(f"        🎤 [STT 성공] ({col}) 결과: '{stt_val}'")
                            except Exception as e:
                                print(f"        🎤 [STT 실패] ({col}) 에러: {e}")
                    values.append(stt_val)
                else:
                    # 일반 속성 데이터 적재 (NaN 값은 None으로 변환하여 DB NULL 처리)
                    val = row[col]
                    values.append(None if pd.isna(val) else val)

            # 지리 정보(Geometry)를 WKB(Hex) 포맷으로 변환하여 PostGIS에 삽입
            if is_geo:
                geom = row[geom_col]
                if geom:
                    values.append(wkb_dumps(geom, hex=True, srid=3857))
                else:
                    values.append(None)
            rows_to_insert.append(values)

        # 6. 배치 INSERT (BATCH_SIZE 단위로 트랜잭션 분할하여 락 경합 방지)
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
        print(f"        ❌ [DB 저장 실패] {e}")


# ---------- GPKG 분석 및 워크플로우 제어 함수 ----------

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """
    다운로드된 프로젝트 폴더 내의 모든 .gpkg 파일을 스캔하여 분석 및 적재를 수행합니다.
    1. 관리 테이블(qfield_data_manage) 생성 및 프로젝트 변환 이력 관리.
    2. GPKG 내의 각 레이어를 개별 데이터프레임으로 변환.
    3. 좌표계를 3857로 통일하고 고유 테이블 명칭을 생성하여 save_gdf_direct 호출.
    4. 분석 결과가 업데이트되면 최종 통합 뷰(VIEW)를 갱신하도록 트리거합니다.
    """
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13] # 테이블 명칭 길이를 제한하기 위한 ID 슬라이싱
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_') # DB 명칭 규칙 준수용 치환

    # 데이터 분류를 위한 기준 정보 로드
    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map:
        return False

    # 어떤 프로젝트가 어떤 물리 테이블과 매칭되는지 기록하는 통합 관리 테이블
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
    if not os.path.exists(project_path): return False

    # 프로젝트 폴더 내의 모든 GeoPackage 파일 리스트 확보
    files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]

    for file in files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        
        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                # QGIS 내부 관리용 시스템 레이어는 데이터 분석에서 제외
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']:
                    continue

                # 레이어 읽기 및 빈 데이터 체크
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue

                # 지리 정보 유무 확인 및 컬럼 목록 추출
                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                geom_col = gdf.geometry.name if is_geo else None
                gpkg_columns = [c for c in gdf.columns if c != geom_col]

                # 해당 레이어가 우리가 관리하는 '재난 타입'에 해당하는지 판별
                matched_type, matched_col_list = find_matching_qfield_type(gpkg_columns, qfield_info_map)

                if matched_type is None:
                    print(f"        ⏭️ [스킵] '{layer_name}' - 매칭 타입 없음")
                    continue

                print(f"        ✅ [매칭 성공] type='{matched_type}'")

                # 좌표계 통일: 원본 CRS가 있으면 3857로 변환, 없으면 기본값(5186) 부여 후 변환
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)

                # 개별 테이블명 생성 (소유자_ID_순번) 및 DB 전송
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                # 관리 테이블에 변환 이력 기록 (이미 있는 파일이면 최신 정보로 UPDATE)
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
                        "pid": project_id, "pname": project_name, "gname": file_stem,
                        "tname": table_name, "owner": owner, "qtype": matched_type, "now": now
                    })
                    conn.commit()

                any_updated, global_table_index = True, global_table_index + 1

        except Exception as e:
            print(f"        ⚠️ {file} 처리 중 에러: {e}")

    # 하나라도 데이터가 갱신되었다면 UNION ALL 기반의 대시보드용 뷰를 다시 생성합니다.
    if any_updated:
        update_unified_view()

    return any_updated


def _cleanup_deleted_project(project_id, project_name=""):
    """
    212 서버에서 삭제된 프로젝트를 215 DB와 로컬 파일시스템에서 정리합니다.
    1. qfield_data_manage에서 해당 프로젝트의 행 조회
    2. 참조된 물리 테이블을 DROP (락 충돌 시 재시도)
    3. qfield_data_manage에서 해당 행 삭제
    4. 로컬 다운로드 디렉토리 삭제
    """
    label = project_name or project_id[:8]
    print(f"      🧹 [삭제 프로젝트 정리 시작] {label}")
    try:
        # ① 해당 프로젝트가 참조하는 물리 테이블 목록 조회
        with get_pg_conn_safe() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT table_name FROM {TARGET_SCHEMA}.qfield_data_manage WHERE id = %s",
                    (project_id,)
                )
                table_names = [row[0] for row in cur.fetchall()]

        # ② 물리 테이블 DROP (autocommit + 재시도)
        for t_name in table_names:
            for attempt in range(1, 4):
                try:
                    conn_ddl = psycopg2.connect(
                        host=DB_HOST, port=DB_PORT,
                        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
                        connect_timeout=CONNECT_TIMEOUT_SEC,
                        options=f"-c lock_timeout=5000 -c statement_timeout={STATEMENT_TIMEOUT_MS}",
                    )
                    conn_ddl.autocommit = True
                    with conn_ddl.cursor() as cur:
                        cur.execute(f'DROP TABLE IF EXISTS {TARGET_SCHEMA}."{t_name}" CASCADE')
                    conn_ddl.close()
                    print(f"      🗑️ 테이블 삭제: {t_name}")
                    break
                except psycopg2.errors.LockNotAvailable:
                    print(f"      🔁 테이블 DROP 락 충돌 ({attempt}/3): {t_name}")
                    _terminate_schema_idle_blockers(TARGET_SCHEMA, label=f"drop {t_name}")
                    time.sleep(3)
                except Exception as te:
                    print(f"      ⚠️ 테이블 DROP 오류 ({t_name}): {te}")
                    break

        # ③ qfield_data_manage에서 해당 프로젝트 행 삭제
        with get_pg_conn_safe() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {TARGET_SCHEMA}.qfield_data_manage WHERE id = %s",
                    (project_id,)
                )
            conn.commit()
        print(f"      ✅ qfield_data_manage 행 삭제 완료: {label}")

        # ④ 로컬 다운로드 디렉토리 삭제
        project_path = os.path.join(BASE_OUTPUT_DIR, project_id)
        if os.path.exists(project_path):
            shutil.rmtree(project_path, ignore_errors=True)
            print(f"      🗂️ 로컬 파일 삭제: {project_path}")

    except Exception as e:
        print(f"      ❌ 삭제 프로젝트 정리 실패 ({label}): {e}")


def _get_active_project_ids():
    """
    212 운영 DB에서 현재 실제로 존재하는 프로젝트 ID 목록을 조회합니다.
    뷰 생성 시 use_yn 컬럼 값을 결정하는 데 사용됩니다.
    - 현재도 서버에 존재하는 프로젝트 → use_yn = 'y'
    - 서버에서 삭제된 프로젝트   → use_yn = 'n'
    UUID 타입 차이(하이픈 유무, 대소문자)로 인한 비교 오류를 방지하기 위해
    소문자 + 하이픈 포함 형식으로 정규화하여 저장합니다.
    """
    active_ids = set()
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor()
        # UUID를 TEXT로 캐스팅하여 하이픈 포함 소문자 형식으로 통일
        cur.execute("SELECT id::text FROM public.core_project")
        for row in cur.fetchall():
            normalized = str(row[0]).strip().lower()
            active_ids.add(normalized)
        print(f"      📋 활성 프로젝트 ID {len(active_ids)}개 조회 완료")
        # 디버그: 처음 3개 샘플 출력 (형식 확인용)
        for sample in list(active_ids)[:3]:
            print(f"         샘플 ID: '{sample}'")
    except Exception as e:
        print(f"      ⚠️ 활성 프로젝트 ID 조회 실패: {e}")
    finally:
        if conn:
            conn.close()
    return active_ids


def _drop_create_view(view_name, view_sql, max_retries=3, retry_delay=3):
    """
    DROP VIEW IF EXISTS → CREATE VIEW 순서로 DDL을 실행합니다.
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
                print(f"      ✅ 뷰 생성 완료: {view_name}")
                return True
            finally:
                conn.close()
        except psycopg2.errors.LockNotAvailable:
            print(f"      🔁 뷰 락 충돌 감지 ({attempt}/{max_retries}): {view_name}")
            _terminate_schema_idle_blockers(label=view_name)
            time.sleep(retry_delay)
        except Exception as e:
            print(f"      ⚠️ 뷰 생성 오류 ({view_name}, 시도 {attempt}): {e}")
            time.sleep(retry_delay)
    print(f"      ❌ 뷰 생성 최종 실패 (재시도 {max_retries}회 소진): {view_name}")
    return False


def update_unified_view():
    """
    분산되어 적재된 여러 사용자의 개별 테이블들을 '재난 타입별'로 하나로 묶어줍니다.
    dashboard나 GIS 클라이언트에서 조회하기 편하도록 'rain_v_qfield_data' 같은 이름의 VIEW를 생성합니다.
    동일한 컬럼 구조를 가진 테이블들을 'UNION ALL'로 결합하는 SQL을 동적으로 생성하여 실행합니다.

    [use_yn 컬럼]
    - 뷰의 owner 컬럼 앞에 use_yn(CHAR(1)) 컬럼을 배치합니다.
    - 현재 212 서버에 존재하는 프로젝트(manage_id) → 'y'
    - 212 서버에서 삭제된 프로젝트(manage_id)      → 'n'
    - 이를 통해 클라이언트에서 유효한 데이터와 삭제된 데이터를 구분할 수 있습니다.

    [UNION ALL 컬럼 정합성]
    - UNION ALL은 모든 SELECT의 컬럼 수/타입이 동일해야 합니다.
    - 같은 qfield_type에 속하는 테이블들의 '공통 컬럼 교집합'만 사용하여 안전하게 결합합니다.
    - 공통 컬럼은 첫 번째 테이블의 ordinal_position 순서를 기준으로 정렬합니다.
    """
    print(f"    📊 [개별 뷰 갱신 시작]")
    try:
        # ── STEP 1: 메타데이터 조회 (일반 커넥션, 락 없음) ──
        # 212 서버에서 현재 활성 상태인 프로젝트 ID 목록 조회 (use_yn 값 결정에 사용)
        active_project_ids = _get_active_project_ids()

        type_rows_map = {}   # {q_type: [row, ...]}
        table_cols_map = {}  # {table_name: [col_name, ...]}

        with get_pg_conn_safe() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 현재 DB에 관리 중인 재난 타입(예: rain, fire) 종류 조회
                cur.execute(
                    f"SELECT DISTINCT qfield_type FROM {TARGET_SCHEMA}.qfield_data_manage"
                    f" WHERE qfield_type IS NOT NULL"
                )
                types = [r['qfield_type'] for r in cur.fetchall()]

                if not types:
                    print(f"      ℹ️ 관리 중인 타입 없음, 뷰 갱신 스킵")
                    return

                for q_type in types:
                    # 해당 타입에 속하는 모든 물리 테이블 리스트 조회
                    cur.execute(
                        f"SELECT id, name, gpkg_name, table_name"
                        f" FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type = %s",
                        (q_type,)
                    )
                    type_rows_map[q_type] = cur.fetchall()

                # 각 테이블의 컬럼 목록 조회 (information_schema는 락을 유발하지 않음)
                all_table_names = [r['table_name'] for rows in type_rows_map.values() for r in rows]
                for t_name in all_table_names:
                    cur.execute(
                        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables"
                        f" WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)",
                        (t_name,)
                    )
                    if not cur.fetchone()[0]:
                        # 물리 테이블이 없는 경우 빈 리스트로 표시
                        table_cols_map[t_name] = []
                        continue
                    cur.execute(
                        f"SELECT column_name FROM information_schema.columns"
                        f" WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s"
                        f"   AND column_name != 'seq'"
                        f" ORDER BY ordinal_position",
                        (t_name,)
                    )
                    table_cols_map[t_name] = [row[0] for row in cur.fetchall()]

        # ── STEP 2: 타입별 공통 컬럼 교집합 계산 후 뷰 SQL 조립 ──
        # _terminate 후 db_engine.dispose()가 호출될 수 있으므로
        # DDL 전에 미리 모든 메타데이터를 수집한 뒤 커넥션을 닫고 진행합니다.
        _terminate_schema_idle_blockers(label="before view DDL")

        for q_type, rows in type_rows_map.items():
            # 물리 테이블이 실제로 존재하는 행만 필터링
            valid_rows = [r for r in rows if table_cols_map.get(r['table_name'])]
            if not valid_rows:
                print(f"      ℹ️ 뷰 생성 스킵 (유효 테이블 없음): {q_type}_v_qfield_data")
                continue

            # 모든 유효 테이블의 컬럼 교집합 계산
            # → UNION ALL 시 컬럼 수 불일치로 인한 SQL 오류 방지
            col_sets = [set(table_cols_map[r['table_name']]) for r in valid_rows]
            common_cols_set = col_sets[0]
            for cs in col_sets[1:]:
                common_cols_set = common_cols_set & cs

            # 첫 번째 테이블의 ordinal_position 순서로 공통 컬럼 정렬
            first_table_cols = table_cols_map[valid_rows[0]['table_name']]
            # seq는 이미 제외됨, platform_type 등 공통 컬럼 순서 유지
            ordered_common_cols = [c for c in first_table_cols if c in common_cols_set]

            if not ordered_common_cols:
                print(f"      ⚠️ 뷰 생성 스킵 (공통 컬럼 없음): {q_type}_v_qfield_data")
                continue

            # owner 컬럼 앞에 use_yn을 삽입할 위치 결정
            # UNION ALL 전체에 동일한 컬럼 구조를 적용해야 하므로
            # 고정 컬럼(manage_id, project_name 등) + use_yn + 공통 데이터 컬럼 순으로 구성
            owner_idx = next(
                (i for i, c in enumerate(ordered_common_cols) if c.lower() == 'owner'),
                None  # owner 컬럼이 없는 경우
            )

            view_parts = []
            for r in valid_rows:
                t_name   = r['table_name']
                manage_id = str(r['id']).strip().lower()

                # use_yn: 212 서버에 프로젝트가 존재하면 'y', 삭제됐으면 'n'
                use_yn_val = 'y' if manage_id in active_project_ids else 'n'
                # 디버그: use_yn 판단 근거 출력
                print(f"      🔍 manage_id='{manage_id}' → use_yn='{use_yn_val}' (active 목록에 {'있음' if use_yn_val == 'y' else '없음'})")

                # 공통 컬럼 SELECT 표현식 목록 구성
                col_exprs = [f'd."{c}"' for c in ordered_common_cols]

                # owner 컬럼 앞에 use_yn 삽입
                # owner가 없으면 공통 컬럼 목록 맨 앞에 배치
                if owner_idx is not None:
                    col_exprs.insert(owner_idx, f"'{use_yn_val}'::CHAR(1) AS use_yn")
                else:
                    col_exprs.insert(0, f"'{use_yn_val}'::CHAR(1) AS use_yn")

                column_string = ", ".join(col_exprs)

                part = (
                    f"SELECT '{manage_id}'::text AS manage_id,"
                    f" '{r['name']}'::text AS project_name,"
                    f" '{r['gpkg_name']}'::text AS source_gpkg,"
                    f" '{t_name}'::text AS source_table,"
                    f" '{q_type}'::text AS qfield_type,"
                    f" {column_string}"
                    f" FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                )
                view_parts.append(part)

            # 수집된 SELECT 문들을 UNION ALL로 엮어서 하나의 VIEW로 통합 생성
            specific_view_name = f"{TARGET_SCHEMA}.{q_type}_v_qfield_data"
            create_view_sql = f"CREATE VIEW {specific_view_name} AS " + " UNION ALL ".join(view_parts)
            _drop_create_view(specific_view_name, create_view_sql)

    except Exception as e:
        print(f"      ⚠️ 뷰 갱신 전체 오류: {e}")


def sync_single_project(project_data):
    """
    하나의 프로젝트에 대해 전체 동기화 프로세스를 수행합니다.
    1. 권한 부여는 메인 루프에서 job 조회 전에 이미 완료됨.
    2. 로컬의 이전 파일들 삭제 (중복 충돌 방지).
    3. SDK를 이용한 최신 프로젝트 파일 벌크 다운로드.
       - 토큰 만료(401) 감지 시 재로그인 후 1회 재시도.
    4. 분석 로직(GPKG 분석 및 적재) 호출.
    5. 매칭 데이터가 전혀 없는 프로젝트는 공간 절약을 위해 파일 삭제.
    """
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
    # grant_admin_permission_via_db는 메인 루프에서 job 조회 전에 이미 실행됨

    # 로컬 디렉토리 완전 초기화
    if os.path.exists(project_path):
        try: shutil.rmtree(project_path)
        except: pass
    os.makedirs(project_path, exist_ok=True)

    try:
        print(f"    🚀 [다운로드 시도] {p_name}")
        if not client:
            client = login_client()
        if not client:
            print(f"    ❌ client 없음, {p_name} 동기화 스킵")
            return

        try:
            client.download_project(
                project_id=p_id,
                local_dir=project_path,
                filter_glob="*",
                show_progress=False,
                force_download=True
            )
        except Exception as dl_err:
            err_str = str(dl_err)
            # 토큰 만료(401) 감지 시 재로그인 후 1회 재시도
            if "401" in err_str or "expired" in err_str.lower() or "Unauthorized" in err_str:
                print(f"    🔑 다운로드 중 토큰 만료 → 재로그인 후 재시도: {p_name}")
                client = login_client()
                if client:
                    client.download_project(
                        project_id=p_id,
                        local_dir=project_path,
                        filter_glob="*",
                        show_progress=False,
                        force_download=True
                    )
                else:
                    print(f"    ❌ 재로그인 실패, {p_name} 동기화 스킵")
                    return
            else:
                raise

        print(f"    ✅ [다운로드 완료] {p_name}")

        # 분석 및 적재 시작
        matched = process_gpkg_to_db(p_id, project_path, p_name, p_owner)

        # 매칭 레이어가 없는 프로젝트는 공간 절약을 위해 로컬 파일 삭제
        if not matched:
            print(f"    🗑️ [파일 삭제] 매칭 레이어 없음")
            try: shutil.rmtree(project_path)
            except: pass

    except Exception as e:
        err_str = str(e)
        # 인증 오류(401) 발생 시 로그인을 재시도하도록 클라이언트 초기화
        if "401" in err_str or "expired" in err_str.lower() or "Unauthorized" in err_str:
            print(f"    🔑 토큰 만료 감지 → 재로그인 시도")
            client = login_client()
        print(f"    ⚠️ {p_name} 처리 실패: {e}")


def get_latest_job_id(project_id):
    """
    사용자가 모바일 기기에서 QField 데이터를 서버로 'Push' 했을 때 생성되는
    'delta_apply'(변경사항 적용) 작업의 최신 ID를 조회합니다.
    성공적으로 끝난(finished) 작업의 ID가 이전과 달라졌다면, 새로운 데이터가 업로드된 것으로 판단합니다.

    [반환값]
    - job ID 문자열: 정상 조회 성공
    - "NO_JOB"    : 완료된 delta_apply job이 없음 (첫 업로드 전)
    - None        : 조회 자체가 실패한 경우 → 캐시에 저장하지 않고 다음 루프에서 재시도
                    (기존 "JOB_CHECK_ERROR" 반환은 캐시에 저장되어 이후 변경 감지 불가 문제 유발)
    """
    global client
    try:
        if not client:
            client = login_client()
        if not client:
            print(f"    ⚠️ [job 조회 실패] client 없음: {project_id[:8]}...")
            return None
        jobs = client.list_jobs(project_id)
        # delta_apply 타입 중 성공한 작업들만 필터링
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs:
            return "NO_JOB"
        # 생성 시간 순으로 정렬하여 가장 최근 것 선택
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except Exception as e:
        err_str = str(e)
        print(f"    ⚠️ [job 조회 실패] project={project_id[:8]}... 원인: {type(e).__name__}: {err_str[:120]}")
        # 토큰 만료(401 / Token has expired) → 즉시 재로그인
        if "401" in err_str or "expired" in err_str.lower() or "Unauthorized" in err_str:
            print(f"    🔑 토큰 만료 감지 → 재로그인 시도...")
            try:
                client = login_client()
            except Exception as re_err:
                print(f"    ❌ 재로그인 실패: {re_err}")
        else:
            # 그 외 일시적 오류도 재로그인 시도
            try:
                client = login_client()
            except Exception:
                pass
        return None  # None 반환 → 캐시 저장 방지, 다음 루프에서 재시도


def get_all_projects_from_db():
    """
    운영 중인 전체 프로젝트 목록을 212 DB에서 실시간으로 조회합니다.
    API 대신 DB를 직접 조회함으로써 서버 부하를 줄이고 유저 이름 등을 조인하여 가져옵니다.
    """
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


# ========== 관리 테이블 초기화 ==========
with db_engine.begin() as conn:
    conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage ("
        f"seq SERIAL PRIMARY KEY, id TEXT, name TEXT, gpkg_name TEXT, "
        f"table_name TEXT, owner TEXT, qfield_type TEXT, "
        f"reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        f"update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        f"CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name))"
    ))

# ========== 메인 실행 루프 (Infinite Loop) ==========
# 30초 간격으로 무한 반복하며 QFieldCloud의 모든 프로젝트를 감시합니다.
last_jobs_cache = {} # 메모리 상에서 프로젝트별로 처리 완료된 최신 Job ID를 기억함
print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...", flush=True)

while True:
    try:
        # client 상태 확인 (리눅스 서버에서 장시간 실행 시 세션 만료 대응)
        if not client:
            print(f"[{datetime.now()}] ⚠️ client 없음, 재로그인 시도...")
            client = login_client()

        # 1. 212 DB를 통해 현재 운영 중인 모든 프로젝트 목록 확보
        current_projects = get_all_projects_from_db()
        current_ids = set(p['id'] for p in current_projects)
        print(f"    📋 프로젝트 {len(current_projects)}개 확인됨", flush=True)

        # ── 삭제된 프로젝트 정리 ──
        # 212 DB에서 사라진 프로젝트(유령 프로젝트)를 215 DB에서도 제거합니다.
        # qfield_data_manage에 남아있지만 current_ids에 없는 항목이 대상입니다.
        try:
            with get_pg_conn_safe() as ghost_conn:
                with ghost_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as ghost_cur:
                    # 215 DB 관리 테이블에서 현재 존재하지 않는 프로젝트 ID 목록 조회
                    ghost_cur.execute(
                        f"SELECT DISTINCT id, name FROM {TARGET_SCHEMA}.qfield_data_manage"
                    )
                    managed_rows = ghost_cur.fetchall()

                # current_ids에 없는 항목 = 삭제된 프로젝트
                ghost_ids = {
                    str(r['id']): r['name']
                    for r in managed_rows
                    if str(r['id']).strip().lower() not in {cid.strip().lower() for cid in current_ids}
                }

            if ghost_ids:
                for ghost_id, ghost_name in ghost_ids.items():
                    print(f"    🗑️ [삭제된 프로젝트 감지] {ghost_name} ({ghost_id})")
                    _cleanup_deleted_project(ghost_id, ghost_name)
                # 유령 프로젝트 정리 후 뷰도 갱신
                update_unified_view()
                # 캐시에서도 제거
                for ghost_id in ghost_ids:
                    last_jobs_cache.pop(ghost_id, None)

        except Exception as ghost_err:
            print(f"    ⚠️ 삭제 프로젝트 정리 오류: {ghost_err}")

        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

            try:
                # job 조회 전에 먼저 권한을 부여합니다.
                # 권한 없이 job 조회 시 admin이 collaborator로 등록되지 않은 프로젝트는 404를 반환합니다.
                grant_admin_permission_via_db(p_id)

                # 2. 각 프로젝트별로 최신 성공 작업 ID 조회
                current_job_id = get_latest_job_id(p_id)

                # job 조회 자체가 실패(None)하면 캐시 갱신 없이 스킵
                # → 다음 루프에서 재시도하므로 무한 스킵 방지
                if current_job_id is None:
                    print(f"    ⏭️ [스킵] job 조회 실패, 다음 루프 재시도: {p['name']}")
                    continue

                cached_job_id = last_jobs_cache.get(p_id)
                path_exists   = os.path.exists(project_path)
                first_run     = (p_id not in last_jobs_cache)
                job_changed   = (current_job_id != cached_job_id)

                # 디버그 로그: 동기화 판단 근거 출력 (리눅스 서버 문제 진단용)
                short_job    = current_job_id[:8] if len(current_job_id) > 8 else current_job_id
                short_cached = str(cached_job_id)[:8] if cached_job_id else 'None'
                print(
                    f"    🔎 [{p['name']}] job={short_job} cached={short_cached}"
                    f" path={path_exists} changed={job_changed} first={first_run}",
                    flush=True
                )

                # 3. 변경 감지 조건:
                # - 캐시에 정보가 없거나 (처음 실행)
                # - 로컬에 파일이 없거나 (실수로 삭제된 경우)
                # - 서버의 Job ID가 이전과 다를 때 (사용자가 기기에서 업로드 완료 시)
                needs_sync = first_run or not path_exists or job_changed

                if needs_sync:
                    reason = '첫실행' if first_run else ('경로없음' if not path_exists else 'job변경')
                    print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (소유자: {p['owner']}, 사유: {reason})")

                    # 실질적인 동기화 및 DB 적재 수행
                    sync_single_project(p)

                    # 처리 완료된 Job ID를 캐시에 저장하여 중복 실행 방지
                    last_jobs_cache[p_id] = current_job_id

            except Exception as e:
                print(f"    ⚠️ 프로젝트 처리 오류 ({p['name']}): {e}")

    except Exception as e:
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(5)
        client = login_client() # 치명적 에러 발생 시 세션 재로그인 시도

    finally:
        # 루프 끝에 엔진 풀 정리 (좀비 커넥션 방지)
        try:
            db_engine.dispose()
        except Exception:
            pass

    # 과도한 API 호출 방지를 위해 지정된 주기(30초)만큼 대기
    print(f"[{datetime.now()}] 💤 대기 중 ({CHECK_INTERVAL}초)...", flush=True)
    time.sleep(CHECK_INTERVAL)