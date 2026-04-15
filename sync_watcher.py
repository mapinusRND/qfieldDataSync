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
# 음성 파일을 텍스트로 변환하기 위한 커스텀 모듈 'disaster2convert' 임포트 시도
try:
    import disaster2convert as dc
except ImportError:
    dc = None
    print("⚠️ disaster2convert 모듈을 찾을 수 없습니다. STT 기능이 제외됩니다.")

# ========== 1. 설정 (Configuration) ==========
# QFieldCloud API 및 계정 정보
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# [운영 DB] 212 서버: QFieldCloud의 실제 운영 데이터(프로젝트, 사용자 등)가 저장된 DB
QFC_DB_HOST = "10.10.10.212"
QFC_DB_PORT = 5433
QFC_DB_NAME = "qfieldcloud_db"
QFC_DB_USER = "root"
QFC_DB_PASS = "1q2w3e4r"

# [저장 및 분석 DB] 215 서버: QFieldCloud에서 다운로드한 GeoPackage 데이터를 가공하여 적재할 대상 DB
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 기타 경로 및 간격 설정
# ========== 기타 경로 및 환경 설정 ==========
# 환경 변수에 따라 로컬(D드라이브) 또는 도커 내부(/app/webfiles) 경로 자동 선택
ENV = os.getenv('FLASK_ENV', 'local')
if ENV == 'local':
    BASE_OUTPUT_DIR = "D:/work/qfield"
else:
    BASE_OUTPUT_DIR = "/app/webfiles/qfield"

TARGET_SCHEMA = "qfield"        # 215 DB 내에서 데이터를 관리할 스키마 명칭
CHECK_INTERVAL = 30             # 동기화 루프 주기 (초)

# [기준 정보 테이블] 215 서버의 disaster 스키마 내에 위치한 qfield_info 테이블 정보
# 각 재난 타입별로 어떤 컬럼을 추출할지 정의되어 있음
QFIELD_INFO_SCHEMA = "disaster"
QFIELD_INFO_TABLE = "qfield_info"
# ============================================

# 디렉토리가 없으면 생성
if not os.path.exists(BASE_OUTPUT_DIR):
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    print(f"📂 [경로 생성] {BASE_OUTPUT_DIR}")

# ---------- SDK 및 DB 연결 관련 함수 ----------

def login_client():
    """QFieldCloud API 클라이언트를 초기화하고 로그인을 수행하여 세션을 생성합니다."""
    try:
        new_client = sdk.Client(url=URL)
        new_client.login(username=USERNAME, password=PASSWORD)
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None

# 전역 SDK 클라이언트 초기화
client = login_client()

# SQLAlchemy 엔진: 데이터베이스 연결 풀링을 지원하여 효율적인 연결 관리 수행
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=600)

def get_pg_conn():
    """215 저장용 DB(rnddb)에 대한 직접적인 psycopg2 연결을 반환합니다."""
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_qfc_db_conn():
    """212 운영 DB(qfieldcloud_db)에 대한 직접적인 psycopg2 연결을 반환합니다."""
    return psycopg2.connect(host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS)

# 프로그램 시작 시 215 서버에 대상 스키마(qfield)가 없으면 생성
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))


# ---------- qfield_info (기준 정보) 관리 함수 ----------

def get_qfield_info_column_lists():
    """
    215 서버의 qfield_info 테이블에서 타입별(rain, fire 등) 추출 대상 컬럼 목록을 조회합니다.
    조회된 결과는 {'타입명': ['컬럼1', '컬럼2', ...]} 형태의 딕셔너리로 반환됩니다.
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

            # DB 데이터 형태(배열 혹은 콤마 구분 문자열)에 따른 파싱 처리
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
    GPKG 파일 내 레이어의 컬럼 목록과 qfield_info에서 로드한 기준 컬럼을 비교합니다.
    GPKG 컬럼이 기준 컬럼을 모두 포함하고 있는 경우(subset), 해당 타입을 반환합니다.
    """
    gpkg_col_set = set(c.lower() for c in gpkg_columns)

    for qfield_type, col_list in qfield_info_map.items():
        required_cols = set(c.lower() for c in col_list)
        if not required_cols:
            continue
        # 기준 컬럼들이 GPKG 레이어 컬럼의 부분집합인지 확인
        if required_cols.issubset(gpkg_col_set):
            return qfield_type, col_list

    return None, None


# ---------- 권한 및 데이터 저장 함수 ----------

def grant_admin_permission_via_db(project_id):
    """
    212 운영 DB의 협업자 테이블에 강제로 admin 권한을 삽입합니다.
    API를 통한 프로젝트 접근 권한 오류를 방지하기 위해 사용됩니다.
    """
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
        if conn:
            conn.close()


def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    """
    GeoDataFrame 객체를 분석하여 215 DB에 실제 테이블로 생성하고 데이터를 적재합니다.
    - allowed_columns: qfield_info에 정의된 특정 컬럼만 필터링하여 저장합니다.
    - STT 처리: 컬럼명에 'record'가 포함된 경우 해당 음성 파일을 찾아 텍스트화한 후 '_txt' 컬럼에 저장합니다.
    - 좌표계: EPSG:3857로 변환하여 저장합니다.
    """
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
        geom_col = gdf.geometry.name if is_geo else None

        # 1. 대상 컬럼 필터링 (메타데이터 owner, reg_date 등 포함)
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

        # 2. 음성 텍스트화(_txt) 컬럼 동적 추가 정의
        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if 'record' in c.lower():
                final_cols.append(c + '_txt')

        # 3. PostgreSQL 테이블 생성 스키마(CREATE TABLE) 정의
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

        # 기존 테이블 삭제 후 재생성 (Overwrite 방식)
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        # 4. 행(Row)별 데이터 삽입 루프
        for _, row in gdf.iterrows():
            cols, placeholders, values = ['platform_type'], ['%s'], [1]
            for col in final_cols:
                cols.append(f'"{col}"')
                placeholders.append('%s')

                # 'record' 관련 컬럼인 경우 STT(음성->텍스트) 변환 로직 실행
                if col.endswith('_txt') and 'record' in col.lower():
                    origin_record_col = col[:-4]
                    record_file = row.get(origin_record_col)
                    stt_val = ""
                    if record_file and isinstance(record_file, str) and record_file.strip():
                        audio_path = os.path.join(project_path, record_file)
                        # 로컬 경로에 파일이 없는 경우 하위 디렉토리(DCIM 등) 검색
                        if not os.path.exists(audio_path):
                            filename = os.path.basename(record_file)
                            for root, dirs, files in os.walk(project_path):
                                if filename in files:
                                    audio_path = os.path.join(root, filename)
                                    break
                        # STT 모듈을 사용하여 음성 파일 분석
                        if os.path.exists(audio_path) and dc:
                            try:
                                stt_val = dc.read_audio(audio_path)
                                if stt_val: print(f"        🎤 [STT 성공] ({col}) 결과: '{stt_val}'")
                                else: print(f"        🎤 [STT 결과 없음] ({col})")
                            except Exception as e:
                                print(f"        🎤 [STT 실패] ({col}) 에러: {e}")
                    values.append(stt_val)
                else:
                    val = row[col]
                    values.append(None if pd.isna(val) else val)

            # 지오메트리 데이터(WKB 포맷) 처리
            if is_geo:
                cols.append(f'"{geom_col}"')
                geom = row[geom_col]
                if geom:
                    values.append(wkb_dumps(geom, hex=True, srid=3857))
                    placeholders.append('%s::geometry')
                else:
                    values.append(None)
                    placeholders.append('%s')

            # 최종 데이터 INSERT
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
    """
    다운로드된 로컬 디렉토리를 순회하며 모든 .gpkg 파일을 읽어 DB로 전송합니다.
    qfield_data_manage 테이블에 프로젝트-테이블 매핑 정보를 업데이트합니다.
    """
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # 기준 정보 로드
    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map:
        return False

    # 관리용 메타 테이블(qfield_data_manage) 생성
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

    files = [f for f in os.listdir(project_path) if f.endswith(".gpkg")]

    for file in files:
        gpkg_path = os.path.join(project_path, file)
        file_stem = os.path.splitext(file)[0]
        
        try:
            import fiona
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                # 스타일 및 시스템 레이어는 무시
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']:
                    continue

                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue

                # GPKG의 컬럼 구조가 qfield_info의 어떤 재난 타입과 일치하는지 판별
                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                geom_col = gdf.geometry.name if is_geo else None
                gpkg_columns = [c for c in gdf.columns if c != geom_col]

                matched_type, matched_col_list = find_matching_qfield_type(gpkg_columns, qfield_info_map)

                if matched_type is None:
                    print(f"        ⏭️ [스킵] '{layer_name}' - 매칭 타입 없음")
                    continue

                print(f"        ✅ [매칭 성공] type='{matched_type}'")

                # 데이터 좌표계 변환 및 메타 정보 할당
                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)

                # 고유한 테이블 명 생성 및 데이터 적재
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                # 매니지 테이블에 이력 기록 (ON CONFLICT를 통한 UPSERT 처리)
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

    # 하나라도 업데이트된 경우 통합/개별 뷰 갱신 함수 호출
    if any_updated:
        update_unified_view()

    return any_updated


def update_unified_view():
    """
    qfield_data_manage 테이블을 조회하여 qfield_type별로
    관련된 모든 테이블을 UNION ALL로 묶어 별도의 VIEW를 생성합니다.
    예: rain_v_qfield_data, fire_v_qfield_data
    """
    print(f"    📊 [개별 뷰 갱신 시작]")
    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 관리되는 고유 재난 타입 목록 조회
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

            # 타입별 뷰 생성 SQL 실행
            if view_parts:
                specific_view_name = f"{q_type}_v_qfield_data"
                create_view_sql = f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{specific_view_name} AS " + " UNION ALL ".join(view_parts)
                cur.execute(create_view_sql)
                print(f"      ✅ 뷰 생성 완료: {TARGET_SCHEMA}.{specific_view_name}")

        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


def sync_single_project(project_data):
    """
    특정 프로젝트 하나에 대해 [권한부여 -> 로컬경로정리 -> 다운로드 -> DB처리]의 전체 동기화 과정을 수행합니다.
    """
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

    # 1. 212 DB에 admin 권한 직접 주입
    grant_admin_permission_via_db(p_id)
    time.sleep(1)

    # 2. 로컬 디렉토리 초기화 (중복 방지)
    if os.path.exists(project_path):
        try: shutil.rmtree(project_path)
        except: pass
    os.makedirs(project_path, exist_ok=True)

    # 3. QFieldCloud로부터 프로젝트 최신 파일 다운로드
    try:
        print(f"    🚀 [다운로드 시도] {p_name}")
        if not client: client = login_client()

        client.download_project(
            project_id=p_id,
            local_dir=project_path,
            filter_glob="*",
            show_progress=False,
            force_download=True
        )
        print(f"    ✅ [다운로드 완료] {p_name}")

        # 4. 다운로드된 GPKG 파일을 분석하여 DB 전송
        matched = process_gpkg_to_db(p_id, project_path, p_name, p_owner)

        # 재난 타입에 매칭되는 레이어가 전혀 없는 경우 공간 절약을 위해 로컬 디렉토리 삭제
        if not matched:
            print(f"    🗑️ [파일 삭제] 매칭 레이어 없음")
            try: shutil.rmtree(project_path)
            except: pass

    except Exception as e:
        if "401" in str(e) or "Unauthorized" in str(e):
            client = login_client()
        print(f"    ⚠️ {p_name} 처리 실패: {e}")


def get_latest_job_id(project_id):
    """
    QFieldCloud 프로젝트의 Job 목록을 조회하여, 
    가장 최근에 성공적으로 완료된 'delta_apply'(데이터 동기화) 작업의 ID를 반환합니다.
    이 ID를 비교하여 새로운 데이터 변경 여부를 감지합니다.
    """
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
    """212 운영 DB에서 전체 프로젝트 목록과 소유자 정보를 조회합니다."""
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
# 각 프로젝트의 마지막 처리된 Job ID를 캐싱하여 변경사항을 감시합니다.
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...")

while True:
    try:
        # 1. 운영 DB에서 최신 프로젝트 목록 로드
        current_projects = get_all_projects_from_db()
        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            # 2. 프로젝트별 최신 성공 Job ID 확인
            current_job_id = get_latest_job_id(p_id)

            # 3. 캐시된 Job ID와 다르거나 로컬 파일이 없는 경우 동기화 실행
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (소유자: {p['owner']})")
                sync_single_project(p)
                # 처리 완료 후 캐시 업데이트
                last_jobs_cache[p_id] = current_job_id

    except Exception as e:
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(5)
        client = login_client()

    # 설정된 주기(30초)마다 반복 실행
    time.sleep(CHECK_INTERVAL)