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
        return new_client
    except Exception as e:
        print(f"❌ QFieldCloud 로그인 실패: {e}")
        return None

# 전역 SDK 클라이언트 초기화
client = login_client()

# SQLAlchemy 엔진: 데이터베이스 연결 풀링을 통해 대량의 INSERT/UPDATE 작업 시 안정성을 확보합니다.
# pool_pre_ping은 끊긴 연결을 자동으로 감지하여 재연결하는 역할을 합니다.
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=600)

def get_pg_conn():
    """최종 데이터 적재 및 조회용(215 서버) psycopg2 커넥션 생성 (Raw SQL 처리용)"""
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def get_qfc_db_conn():
    """운영 메타데이터 조회용(212 서버) psycopg2 커넥션 생성 (사용자 및 프로젝트 정보 확인용)"""
    return psycopg2.connect(host=QFC_DB_HOST, port=QFC_DB_PORT, dbname=QFC_DB_NAME, user=QFC_DB_USER, password=QFC_DB_PASS)

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


def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    """
    정제된 GeoDataFrame을 215 DB에 실제 물리 테이블로 생성하고 데이터를 한 줄씩 INSERT 합니다.
    주요 기능:
    1. 테이블 생성: 매번 테이블을 DROP 후 새로 생성하여 스키마 변경에 대응(Overwrite).
    2. STT 연동: 컬럼명에 'record'가 포함된 경우 해당 경로의 음성파일을 찾아 텍스트로 변환 후 '{명칭}_txt' 컬럼에 저장.
    3. 좌표계: PostGIS 지오메트리를 WKB 포맷으로 변환하고 EPSG:3857(구글/웹메르카토르)로 고정하여 적재.
    4. 메타데이터: 소유자(owner), 등록일(reg_date), 수정일(update_at)을 강제 포함.
    """
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")
    conn = None
    try:
        conn = get_pg_conn()
        # [추가] 자동 커밋 모드 활성화 - 락 대기를 방지하고 즉시 반영합니다.
        conn.autocommit = True
        cur = conn.cursor()
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

        # 3. DB 컬럼 타입 정의 및 테이블 생성
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
        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        # [중요] 루프 시작 전 다시 커밋 모드 조정 (대량 인서트를 위해)
        conn.autocommit = False

        # 4. 데이터 삽입 루프 (행 단위 처리)
        for _, row in gdf.iterrows():
            cols, placeholders, values = ['platform_type'], ['%s'], [1]
            for col in final_cols:
                cols.append(f'"{col}"')
                placeholders.append('%s')

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
    if not qfield_info_map:
        return False

    # [수정] 관리 테이블 생성 시 즉시 커밋 및 연결 종료
    with db_engine.begin() as conn:  # .begin()은 블록 종료 시 자동 커밋을 보장합니다.
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

                if matched_type is None:
                    continue

                print(f"        ✅ [매칭 성공] type='{matched_type}'")

                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)

                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                
                # [중요] 여기서 save_gdf_direct를 호출하기 전에 
                # SQLAlchemy 엔진의 연결이 모두 반환(Commit)되었는지 확인해야 합니다.
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                # [수정] 이력 기록 시에도 .begin()을 사용하여 작업 후 즉시 트랜잭션 종료
                with db_engine.begin() as conn:
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

                any_updated, global_table_index = True, global_table_index + 1

        except Exception as e:
            print(f"        ⚠️ {file} 처리 중 에러: {e}")

    if any_updated:
        update_unified_view()

    return any_updated


def update_unified_view():
    """
    분산되어 적재된 여러 사용자의 개별 테이블들을 '재난 타입별'로 하나로 묶어줍니다.
    dashboard나 GIS 클라이언트에서 조회하기 편하도록 'rain_v_qfield_data' 같은 이름의 VIEW를 생성합니다.
    동일한 컬럼 구조를 가진 테이블들을 'UNION ALL'로 결합하는 SQL을 동적으로 생성하여 실행합니다.
    """
    print(f"    📊 [개별 뷰 갱신 시작]")
    conn = None
    try:
        conn = get_pg_conn()
        # [중요] 뷰 생성은 DDL이므로 자동 커밋 모드로 설정하여 락 대기를 방지합니다.
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 현재 DB에 관리 중인 재난 타입(예: rain, fire) 종류 조회
        cur.execute(f"SELECT DISTINCT qfield_type FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type IS NOT NULL")
        types = [r['qfield_type'] for r in cur.fetchall()]

        if not types: return

        for q_type in types:
            # 해당 타입에 속하는 모든 물리 테이블 리스트 조회
            cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage WHERE qfield_type = %s", (q_type,))
            rows = cur.fetchall()
            
            view_parts = []
            for r in rows:
                t_name = r['table_name']
                # 실제 DB에 해당 테이블이 물리적으로 존재하는지 최종 확인
                cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
                if cur.fetchone()[0]:
                    # UNION 연산을 위해 컬럼 순서를 일정하게 맞추어 SELECT 문구 생성
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

            # 수집된 SELECT 문들을 UNION ALL로 엮어서 하나의 VIEW로 통합 생성
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
    하나의 프로젝트에 대해 전체 동기화 프로세스를 수행합니다.
    1. 212 DB 권한 강제 주입 (다운로드 권한 확보).
    2. 로컬의 이전 파일들 삭제 (중복 충돌 방지).
    3. SDK를 이용한 최신 프로젝트 파일 벌크 다운로드.
    4. 분석 로직(GPKG 분석 및 적재) 호출.
    5. 매칭 데이터가 전혀 없는 프로젝트는 공간 절약을 위해 파일 삭제.
    """
    global client
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data['owner']
    project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

    # 1. 212 DB 직접 접근을 통한 admin 권한 활성화
    grant_admin_permission_via_db(p_id)
    time.sleep(1)

    # 2. 로컬 디렉토리 완전 초기화
    if os.path.exists(project_path):
        try: shutil.rmtree(project_path)
        except: pass
    os.makedirs(project_path, exist_ok=True)

    # 3. QFieldCloud 서버로부터 데이터 다운로드
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

        # 4. 분석 및 적재 시작
        matched = process_gpkg_to_db(p_id, project_path, p_name, p_owner)

        # 5. 불필요한 파일 정리
        if not matched:
            print(f"    🗑️ [파일 삭제] 매칭 레이어 없음")
            try: shutil.rmtree(project_path)
            except: pass

    except Exception as e:
        # 인증 오류(401) 발생 시 로그인을 재시도하도록 클라이언트 초기화
        if "401" in str(e) or "Unauthorized" in str(e):
            client = login_client()
        print(f"    ⚠️ {p_name} 처리 실패: {e}")


def get_latest_job_id(project_id):
    """
    사용자가 모바일 기기에서 QField 데이터를 서버로 'Push' 했을 때 생성되는
    'delta_apply'(변경사항 적용) 작업의 최신 ID를 조회합니다.
    성공적으로 끝난(finished) 작업의 ID가 이전과 달라졌다면, 새로운 데이터가 업로드된 것으로 판단합니다.
    """
    global client
    try:
        if not client: client = login_client()
        jobs = client.list_jobs(project_id)
        
        # 1. 현재 진행 중인(진행될) 작업이 있는지 확인
        active_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') in ['pending', 'running']]
        if active_jobs:
            # 작업이 아직 진행 중이면 'WAIT'를 반환하여 루프가 잠시 대기하게 함
            return "JOB_IN_PROGRESS"

        # 2. 완료된 최신 작업 확인
        finished_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not finished_jobs: return "NO_JOB"
        
        finished_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return finished_jobs[0]['id']
    except Exception as e:
        return f"ERROR_{str(e)}"


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


# ========== 메인 실행 루프 (Infinite Loop) ==========
# 30초 간격으로 무한 반복하며 QFieldCloud의 모든 프로젝트를 감시합니다.
last_jobs_cache = {} # 메모리 상에서 프로젝트별로 처리 완료된 최신 Job ID를 기억함
print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...")

while True:
    try:
        current_projects = get_all_projects_from_db()
        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)
            current_job_id = get_latest_job_id(p_id)

            # [수정] 작업 중일 때는 다음 루프에서 확인하도록 건너뜀
            if current_job_id == "JOB_IN_PROGRESS":
                print(f"    ⏳ {p['name']}: 서버에서 변경사항 적용 중... 대기")
                continue

            # 변경 감지 조건
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                # 에러 상태가 아닐 때만 실행
                if "ERROR" not in current_job_id:
                    print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (Job: {current_job_id})")
                    sync_single_project(p)
                    last_jobs_cache[p_id] = current_job_id

    except Exception as e:
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(5)
        client = login_client() # 치명적 에러 발생 시 세션 재로그인 시도

    # 과도한 API 호출 방지를 위해 지정된 주기(30초)만큼 대기
    time.sleep(CHECK_INTERVAL)