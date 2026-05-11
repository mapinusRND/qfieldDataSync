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
from psycopg2.extras import execute_batch

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
AUDIO_FILE_CACHE = {}
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
def build_audio_cache(project_path):
    cache = {}
    for root, _, files in os.walk(project_path):
        for f in files:
            cache[f] = os.path.join(root, f)
    return cache

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


# ============================
# 🔥 성능 개선된 핵심 함수
# ============================
def save_gdf_direct(gdf, table_name, schema, project_path, owner_name, allowed_columns=None):
    print(f"        💾 [DB 저장 시작] 테이블: {table_name}")

    conn = None
    try:
        conn = get_pg_conn()
        cur = conn.cursor()

        is_geo = isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None
        geom_col = gdf.geometry.name if is_geo else None

        # === 컬럼 필터 ===
        if allowed_columns:
            allowed_lower = set(c.lower() for c in allowed_columns)
            source_cols = [c for c in gdf.columns if c != geom_col and c.lower() in allowed_lower]
        else:
            source_cols = [c for c in gdf.columns if c != geom_col]

        # === STT 컬럼 추가 ===
        final_cols = []
        for c in source_cols:
            final_cols.append(c)
            if 'record' in c.lower():
                final_cols.append(c + '_txt')

        # === 테이블 생성 ===
        col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
        for col in final_cols:
            col_defs.append(f'"{col}" TEXT')
        col_defs.append("use_yn CHAR(1) DEFAULT 'y'")
        if is_geo:
            col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')

        cur.execute(f'DROP TABLE IF EXISTS {schema}."{table_name}" CASCADE')
        cur.execute(f'CREATE TABLE {schema}."{table_name}" ({", ".join(col_defs)})')

        # ✅ 추가: geometry 컬럼에 GiST 인덱스 생성
        if is_geo:
            index_name = f"idx_{table_name}_{geom_col}"
            cur.execute(f"""
                CREATE INDEX "{index_name}"
                ON {schema}."{table_name}"
                USING GIST ("{geom_col}")
            """)
            print(f"        🗂️ [인덱스 생성] {index_name}")

        # === 🔥 AUDIO 캐시 생성 (중요) ===
        audio_cache = build_audio_cache(project_path)

        insert_cols = ['platform_type'] + final_cols + ['use_yn']
        if is_geo:
            insert_cols.append(geom_col)

        placeholders = ['%s'] * len(insert_cols)
        if is_geo:
            placeholders[-1] = '%s::geometry'

        sql = f'''
            INSERT INTO {schema}."{table_name}"
            ({", ".join([f'"{c}"' for c in insert_cols])})
            VALUES ({", ".join(placeholders)})
        '''

        # === 🔥 batch 데이터 생성 ===
        batch_data = []

        for row in gdf.itertuples(index=False):
            row_dict = row._asdict()
            values = [1]
            for col in final_cols:
                if col.endswith('_txt'):
                    origin = col[:-4]
                    file = row_dict.get(origin)

                    stt_val = ""
                    if isinstance(file, str) and file.strip():
                        filename = os.path.basename(file)
                        path = audio_cache.get(filename)

                        if path and dc:
                            try:
                                stt_val = dc.read_audio(path)
                            except:
                                pass
                    values.append(stt_val)
                else:
                    val = row_dict.get(col)
                    values.append(None if pd.isna(val) else val)

            values.append('y')
            if is_geo:
                geom = row_dict.get(geom_col)
                values.append(wkb_dumps(geom, hex=True, srid=3857) if geom else None)
            batch_data.append(values)

        # === 🔥 핵심: batch insert ===
        execute_batch(cur, sql, batch_data, page_size=1000)

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


# ---------- GPKG 분석 및 워크플로우 제어 함수 ----------

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """
    다운로드된 프로젝트 폴더 내의 모든 .gpkg 파일을 스캔하여 분석 및 적재를 수행합니다.
    """
    print(f"    🔍 [분석 시작] {project_name}")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    qfield_info_map = get_qfield_info_column_lists()
    if not qfield_info_map:
        return False

    # ✅ qfield_data_manage에 use_yn 컬럼 추가 (소프트 삭제 상태 관리용)
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
                use_yn CHAR(1) DEFAULT 'y',
                reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            )
        """))
        # ✅ 기존 테이블에 use_yn 컬럼이 없을 경우 대비 (ALTER로 안전하게 추가)
        conn.execute(text(f"""
            ALTER TABLE {TARGET_SCHEMA}.qfield_data_manage
            ADD COLUMN IF NOT EXISTS use_yn CHAR(1) DEFAULT 'y'
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
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'gpkg_contents']:
                    continue

                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue

                is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None)
                geom_col = gdf.geometry.name if is_geo else None
                gpkg_columns = [c for c in gdf.columns if c != geom_col]

                matched_type, matched_col_list = find_matching_qfield_type(gpkg_columns, qfield_info_map)

                if matched_type is None:
                    print(f"        ⏭️ [스킵] '{layer_name}' - 매칭 타입 없음")
                    continue

                print(f"        ✅ [매칭 성공] type='{matched_type}'")

                gdf = gdf.to_crs(epsg=3857) if gdf.crs else gdf.set_crs(epsg=5186).to_crs(epsg=3857)
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)

                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path, owner, allowed_columns=matched_col_list)

                with db_engine.connect() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {TARGET_SCHEMA}.qfield_data_manage
                            (id, name, gpkg_name, table_name, owner, qfield_type, use_yn, reg_date, update_at)
                        VALUES
                            (:pid, :pname, :gname, :tname, :owner, :qtype, 'y', :now, :now)
                        ON CONFLICT (id, gpkg_name) DO UPDATE SET
                            name = EXCLUDED.name,
                            table_name = EXCLUDED.table_name,
                            owner = EXCLUDED.owner,
                            qfield_type = EXCLUDED.qfield_type,
                            use_yn = 'y',                          -- ✅ 재등록 시 use_yn 복구
                            update_at = EXCLUDED.update_at
                    """), {
                        "pid": project_id, "pname": project_name, "gname": file_stem,
                        "tname": table_name, "owner": owner, "qtype": matched_type, "now": now
                    })
                    conn.commit()

                any_updated, global_table_index = True, global_table_index + 1

        except Exception as e:
            print(f"        ⚠️ {file} 처리 중 에러: {e}")

    if any_updated:
        update_unified_view()

    return any_updated


def update_unified_view():
    print(f"    📊 [개별 뷰 갱신 시작 (순번 생성 로직 적용)]")
    conn = None
    try:
        conn = get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute(f"""
            SELECT DISTINCT qfield_type 
            FROM {TARGET_SCHEMA}.qfield_data_manage 
            WHERE qfield_type IS NOT NULL
        """)
        types = [r['qfield_type'] for r in cur.fetchall()]

        if not types:
            return

        for q_type in types:
            cur.execute(f"""
                SELECT id, name, gpkg_name, table_name, reg_date
                FROM {TARGET_SCHEMA}.qfield_data_manage 
                WHERE qfield_type = %s
                ORDER BY reg_date ASC
            """, (q_type,))
            rows = cur.fetchall()

            view_parts = []
            all_data_cols = []

            for r in rows:
                t_name = r['table_name']
                reg_date_val = str(r['reg_date']) if r['reg_date'] else '1970-01-01'

                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (TARGET_SCHEMA, t_name))
                if not cur.fetchone()[0]:
                    continue

                # ✅ geometry 컬럼명 확인
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                      AND udt_name = 'geometry'
                    LIMIT 1
                """, (TARGET_SCHEMA, t_name))
                geom_row = cur.fetchone()
                geom_col_name = geom_row[0] if geom_row else None

                # ✅ seq, geometry 제외한 일반 컬럼 조회
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                      AND column_name != 'seq'
                      AND udt_name != 'geometry'
                    ORDER BY ordinal_position
                """, (TARGET_SCHEMA, t_name))
                col_rows = cur.fetchall()
                col_names = [col[0] for col in col_rows]
                all_data_cols = col_names

                non_geom_cols = ", ".join([f'd."{c}"' for c in col_names])

                # ✅ geometry 앞에 reg_date(테이블 등록시간) 삽입
                geom_part = f', d."{geom_col_name}"' if geom_col_name else ''

                part = (
                    f"SELECT "
                    f"d.seq AS origin_seq, "
                    f"'{reg_date_val}'::timestamp AS table_reg_date, "  # 정렬 기준 겸 뷰 노출용
                    f"'{r['id']}'::text AS manage_id, "
                    f"'{r['name']}'::text AS project_name, "
                    f"'{r['gpkg_name']}'::text AS source_gpkg, "
                    f"'{t_name}'::text AS source_table, "
                    f"'{q_type}'::text AS qfield_type, "
                    f"{non_geom_cols}, "                                # 일반 데이터 컬럼
                    f"'{reg_date_val}'::timestamp AS added_at"          # ✅ geometry 앞 등록시간 컬럼
                    f"{geom_part} "                                     # ✅ geometry 맨 마지막
                    f"FROM {TARGET_SCHEMA}.\"{t_name}\" d"
                )
                view_parts.append(part)

            if view_parts:
                specific_view_name = f"{q_type}_v_qfield_data"
                union_sql = " UNION ALL ".join(view_parts)

                outer_cols = ", ".join([f'sub."{c}"' for c in all_data_cols])
                geom_outer = f', sub."{geom_col_name}"' if geom_col_name else ''

                create_view_sql = (
                    f"CREATE VIEW {TARGET_SCHEMA}.{specific_view_name} AS "
                    f"SELECT "
                    f"  ROW_NUMBER() OVER ("
                    f"    ORDER BY sub.table_reg_date ASC, sub.origin_seq ASC"
                    f"  ) AS seq, "
                    f"  sub.manage_id, sub.project_name, sub.source_gpkg, "
                    f"  sub.source_table, sub.qfield_type, "
                    f"  {outer_cols}, "                                 # 일반 데이터 컬럼
                    f"  sub.added_at"                                   # ✅ geometry 앞 등록시간
                    f"  {geom_outer} "                                  # ✅ geometry 맨 마지막
                    f"FROM ({union_sql}) sub"
                )

                try:
                    cur.execute(f"DROP VIEW IF EXISTS {TARGET_SCHEMA}.{specific_view_name} CASCADE")
                    cur.execute(create_view_sql)
                    print(f"      ✅ 뷰 생성 완료: {TARGET_SCHEMA}.{specific_view_name}")
                except Exception as view_err:
                    print(f"      ❌ 뷰 생성 실패: {specific_view_name} → {view_err}")

    except Exception as e:
        print(f"      ⚠️ 뷰 갱신 오류: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def update_total_view():
    """
    disaster.qfield_info 테이블을 동적으로 읽어 통합 뷰를 생성합니다.
    하드코딩 없이 타입별 컬럼 목록을 자동으로 파악하여 UNION ALL 구성합니다.
    """
    print(f"    📊 [통합 뷰 생성 시작] qfield.v_total_qfield_data")

    # ================================
    # try 밖으로 꺼낸 헬퍼 함수
    # ================================
    def col_or_null(actual_col, alias, existing_cols):
        """실제 컬럼이 뷰에 존재하면 참조, 없으면 NULL로 대체"""
        if actual_col and actual_col in existing_cols:
            return f'"{actual_col}" AS "{alias}"'
        return f'NULL AS "{alias}"'

    def parse_column_list(raw_list):
        """PostgreSQL 배열 형식 또는 문자열을 파이썬 리스트로 파싱"""
        if isinstance(raw_list, list):
            return raw_list
        if isinstance(raw_list, str):
            cleaned = raw_list.strip()
            if cleaned.startswith('{') and cleaned.endswith('}'):
                inner = cleaned[1:-1]
                return [c.strip().strip('"') for c in inner.split(',') if c.strip()]
            return [c.strip() for c in cleaned.split(',') if c.strip()]
        return []

    conn = None
    try:
        conn = get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # ================================
        # 1. disaster.qfield_info에서 타입별 컬럼 목록 로드
        # ================================
        cur.execute(f"""
            SELECT qfield_type, column_list
            FROM {QFIELD_INFO_SCHEMA}.{QFIELD_INFO_TABLE}
            ORDER BY id
        """)
        qfield_info_rows = cur.fetchall()

        if not qfield_info_rows:
            print(f"    ⚠️ [통합 뷰 스킵] qfield_info 데이터 없음")
            return

        # ================================
        # 2. 타입별 역할명(접두사 제거) 분류
        # ================================
        all_role_names = {}
        type_col_map = {}

        for row in qfield_info_rows:
            q_type = row['qfield_type']
            col_list = parse_column_list(row['column_list'])
            prefix = f"{q_type}_"
            role_map = {}

            for col in col_list:
                role = col[len(prefix):] if col.lower().startswith(prefix) else col
                role_map[role] = col
                all_role_names[role] = True

            # record_txt: STT 결과 컬럼 (qfield_info에 없지만 뷰에 존재)
            if 'record' in role_map:
                role_map['record_txt'] = f"{q_type}_record_txt"

            type_col_map[q_type] = role_map
            print(f"    📋 [{q_type}] 역할 매핑: {role_map}")

        all_role_names['record_txt'] = True

        # 컬럼 출력 순서 고정
        ordered_roles = [
            'facilities_name',
            'height', 'length', 'breadth', 'diameter',
            'photo_1', 'photo_2', 'photo_3', 'photo_4', 'photo_5',
            'record', 'record_txt',
            'd_date',
        ]
        # 위 목록에 없는 나머지 역할명 뒤에 추가 (예: snow의 slope 등)
        for role in all_role_names:
            if role not in ordered_roles:
                ordered_roles.append(role)

        print(f"    📋 [최종 역할 순서] {ordered_roles}")

        # ================================
        # 3. 타입별 뷰 확인 후 UNION ALL 파트 생성
        # ================================
        union_parts = []
        included_types = []

        for row in qfield_info_rows:
            q_type = row['qfield_type']
            view_name = f"{q_type}_v_qfield_data"

            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.views
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (TARGET_SCHEMA, view_name))

            if not cur.fetchone()[0]:
                print(f"    ⏭️ [스킵] {view_name} 뷰 없음 (아직 데이터 미수집)")
                continue

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (TARGET_SCHEMA, view_name))
            existing_cols = set(r['column_name'] for r in cur.fetchall())

            role_map = type_col_map.get(q_type, {})

            role_selects = []
            for role in ordered_roles:
                actual_col = role_map.get(role)
                role_selects.append(col_or_null(actual_col, role, existing_cols))

            role_select_sql = ",\n    ".join(role_selects)

            geom_select = (
                '"geometry"'
                if 'geometry' in existing_cols
                else 'NULL::geometry AS "geometry"'
            )

            part = f"""-- {q_type}
SELECT
    (qfield_type || '_' || CAST(seq AS VARCHAR)) AS total_seq,
    seq                                          AS original_seq,
    manage_id, project_name, source_gpkg, source_table, qfield_type,
    {col_or_null('platform_type', 'platform_type', existing_cols)},
    {role_select_sql},
    {col_or_null('use_yn', 'use_yn', existing_cols)},
    {col_or_null('added_at', 'added_at', existing_cols)},
    {geom_select}
FROM {TARGET_SCHEMA}.{view_name}"""

            union_parts.append(part)
            included_types.append(q_type)
            print(f"    ✅ [파트 추가] {view_name}")

        if not union_parts:
            print(f"    ⚠️ [통합 뷰 스킵] 유효한 타입별 뷰 없음")
            return

        # ================================
        # 4. CREATE OR REPLACE VIEW 실행
        # ================================
        total_view_sql = (
            f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.v_total_qfield_data AS\n"
            + "\nUNION ALL\n".join(union_parts)
        )

        cur.execute(total_view_sql)

        # f-string 중첩 이슈 해결: 타입 목록을 변수로 분리
        types_str = ', '.join(included_types)
        print(f"    ✅ [통합 뷰 생성 완료] {TARGET_SCHEMA}.v_total_qfield_data "
              f"({len(union_parts)}개 타입: {types_str})")

    except Exception as e:
        print(f"    ❌ [통합 뷰 생성 실패] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
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
    212 DB의 core_job 테이블에서 delta_apply 완료 시각을 직접 조회
    """
    conn = None
    try:
        conn = get_qfc_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # ✅ delta_apply job finished_at 직접 조회
        cur.execute("""
            SELECT id, finished_at
            FROM public.core_job
            WHERE project_id = %s::uuid
              AND type = 'delta_apply'
              AND status = 'finished'
            ORDER BY finished_at DESC
            LIMIT 1
        """, (project_id,))

        job_row = cur.fetchone()

        if job_row and job_row['finished_at']:
            return f"delta_{str(job_row['id'])}_{str(job_row['finished_at'])}"

        # ✅ delta_apply job 없으면 data_last_updated_at 폴백
        cur.execute("""
            SELECT id, data_last_updated_at
            FROM public.core_project
            WHERE id = %s::uuid
        """, (project_id,))

        proj_row = cur.fetchone()
        if not proj_row:
            return "PROJECT_NOT_FOUND"
        if not proj_row['data_last_updated_at']:
            return "NO_JOB"

        return f"proj_{str(proj_row['data_last_updated_at'])}"

    except Exception as e:
        err = str(e)
        # uuid 캐스팅 오류 시 캐스팅 없이 재시도
        if "invalid input syntax" in err or "uuid" in err:
            try:
                conn2 = get_qfc_db_conn()
                cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur2.execute("""
                    SELECT id, finished_at FROM public.core_job
                    WHERE project_id::text = %s
                      AND type = 'delta_apply' AND status = 'finished'
                    ORDER BY finished_at DESC LIMIT 1
                """, (project_id,))
                row = cur2.fetchone()
                conn2.close()
                if row and row['finished_at']:
                    return f"delta_{str(row['id'])}_{str(row['finished_at'])}"
                return "NO_JOB"
            except:
                pass
        print(f"    ⚠️ [Job DB 조회 오류] {project_id}: {e}")
        return "JOB_CHECK_ERROR"
    finally:
        if conn:
            conn.close()
        if conn:
            conn.close()


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

def cleanup_deleted_projects(current_project_ids):
    """
    QFieldCloud에서 삭제된 프로젝트(유령 데이터)를 DB에서 정리한다.
    - use_yn = 'y' 인 활성 프로젝트만 체크 (이미 'n' 처리된 건 반복 감지 방지)
    - 물리 테이블 use_yn = 'n' 소프트 삭제
    - qfield_data_manage use_yn = 'n' 업데이트
    - VIEW 재생성
    """
    if not current_project_ids:
        return

    conn = None
    ghost_found = False

    try:
        conn = get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor()

        id_params_str = str(tuple(current_project_ids)) if len(current_project_ids) > 1 else f"('{current_project_ids[0]}')"

        # ✅ use_yn = 'y' 인 활성 프로젝트 중에서만 삭제된 것을 감지
        cur.execute(f"""
            SELECT table_name, id, name 
            FROM {TARGET_SCHEMA}.qfield_data_manage
            WHERE id NOT IN {id_params_str}
              AND use_yn = 'y'
        """)
        ghost_list = cur.fetchall()

        if ghost_list:
            print(f"[{datetime.now()}] 🧹 삭제된 프로젝트 감지: {len(ghost_list)}개")

            for table_name, pid, pname in ghost_list:
                try:
                    # 물리 테이블 소프트 삭제
                    cur.execute(
                        f'UPDATE {TARGET_SCHEMA}."{table_name}" SET use_yn = %s',
                        ('n',)
                    )
                    # ✅ qfield_data_manage도 use_yn = 'n' 으로 업데이트 (반복 감지 방지 핵심)
                    cur.execute(
                        f"UPDATE {TARGET_SCHEMA}.qfield_data_manage SET use_yn = 'n' WHERE id = %s",
                        (pid,)
                    )
                    print(f"    🗑️ 소프트 삭제 완료 (use_yn=n): {pname} ({pid})")
                    ghost_found = True

                except Exception as e:
                    print(f"    ⚠️ 삭제 처리 실패: {table_name} → {e}")

    except Exception as e:
        print(f"⚠️ 유령 정리 오류: {e}")

    finally:
        if conn:
            conn.close()

    if ghost_found:
        update_unified_view()

# ========== 메인 실행 루프 (Infinite Loop) ==========
# 30초 간격으로 무한 반복하며 QFieldCloud의 모든 프로젝트를 감시합니다.
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 실시간 동기화 엔진 가동 중...")

while True:
    try:
        current_projects = get_all_projects_from_db()
        current_project_ids = [p['id'] for p in current_projects]
        cleanup_deleted_projects(current_project_ids)

        cycle_updated = False  # ✅ 추가 1: 사이클 변경 여부 초기화

        for p in current_projects:
            p_id = p['id']
            project_path = os.path.join(BASE_OUTPUT_DIR, p_id)

            current_job_id = get_latest_job_id(p_id)

            if current_job_id == "PROJECT_NOT_FOUND":
                print(f"    ⚠️ [스킵] {p['name']} - DB에 없는 프로젝트")
                continue

            if current_job_id == "JOB_CHECK_ERROR":
                continue

            needs_sync = (
                p_id not in last_jobs_cache
                or not os.path.exists(project_path)
                or current_job_id != last_jobs_cache[p_id]
            )

            if needs_sync:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (소유자: {p['owner']})")
                sync_single_project(p)
                last_jobs_cache[p_id] = current_job_id
                cycle_updated = True  # ✅ 추가 2: 변경 발생 표시

        # ✅ 추가 3: 전체 프로젝트 순회 완료 후 통합 뷰 갱신
        if cycle_updated:
            update_total_view()

    except Exception as e:
        print(f"⚠️ 루프 에러: {e}")
        time.sleep(5)
        client = login_client()

    print(f"[{datetime.now()}] 💤 대기중... ({CHECK_INTERVAL}초)")
    time.sleep(CHECK_INTERVAL)