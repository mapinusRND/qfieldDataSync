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
import disaster2convert as dc   # 음성 파일을 텍스트로 변환하는 모듈


# ========== 1. 설정 (Configuration) ==========
# QFieldCloud 서버 접속 정보
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# 로컬 저장 및 DB 접속 정보
BASE_OUTPUT_DIR = "./qfield"
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
# SQLAlchemy용 DB 연결 문자열
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TARGET_SCHEMA = "qfield"         # 데이터를 저장할 PostgreSQL 스키마
VIEW_NAME = "v_qfield_total_data" # 여러 테이블을 하나로 합쳐 보여줄 가상 뷰 이름
CHECK_INTERVAL = 30              # 클라우드 변경 감지 주기 (초)
# ============================

# SDK 클라이언트 초기화 및 로그인
client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)

# SQLAlchemy 엔진 생성 (커넥션 풀링 설정 포함)
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)

def get_pg_conn():
    """psycopg2를 이용한 직접적인 DB 연결 객체 반환 (세부 제어용)"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000" # 타임아웃 설정
    )

# 프로그램 시작 시 대상 스키마가 없으면 생성
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

def update_unified_view():
    """
    [통합 뷰 생성 로직]
    각 프로젝트별로 흩어져 있는 물리 테이블들을 하나의 'UNION ALL' 쿼리로 묶어 
    v_qfield_total_data 뷰를 생성합니다. (자동 생성된 seq 컬럼은 제외)
    """
    print(f"    📊 통합 뷰({VIEW_NAME}) 갱신 중...")
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 1. 관리 테이블(qfield_data_manage)에서 현재 동기화된 모든 테이블 목록 조회
        cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage")
        rows = cur.fetchall()
        if not rows: return

        view_parts = []
        for r in rows:
            t_name = r['table_name']
            
            # 2. 실제로 DB에 해당 테이블이 생성되어 있는지 체크
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
            if cur.fetchone()[0]:
                
                # 3. [핵심] 테이블의 컬럼 정보를 읽어와 'seq' 컬럼만 제외하고 쿼리 생성
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

                # 4. 각 테이블에 어떤 프로젝트/GPKG에서 왔는지 정보를 추가하여 SELECT문 작성
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
            # 5. 수집된 모든 SELECT문을 합쳐서 최종 VIEW 생성 또는 교체
            create_view_sql = f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{VIEW_NAME} AS " + " UNION ALL ".join(view_parts)
            cur.execute(create_view_sql)
            conn.commit()
            print(f"      ✅ 통합 뷰 생성 완료 (seq 제외): {TARGET_SCHEMA}.{VIEW_NAME}")
            
    except Exception as e:
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        cur.close(); conn.close()

def save_gdf_direct(gdf, table_name, schema):
    """
    [데이터 물리 저장 로직]
    GeoPandas 객체(GDF)를 PostgreSQL 테이블로 직접 삽입합니다.
    기존 데이터는 삭제(Delete) 후 재생성하는 'Full Refresh' 방식입니다.
    """
    conn = get_pg_conn()
    conn.autocommit = False # 트랜잭션 수동 제어
    cur = conn.cursor()
    try:
        # 공간 데이터(Geometry) 포함 여부 확인
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None and not gdf.geometry.isnull().all())
        geom_col = gdf.geometry.name if is_geo else None
        data_cols = [c for c in gdf.columns if c != geom_col] if is_geo else list(gdf.columns)

        # 테이블 존재 확인
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", (schema, table_name))
        exists = cur.fetchone()[0]

        if exists:
            # 테이블이 있으면 기존 데이터만 삭제
            cur.execute(f'DELETE FROM {schema}."{table_name}"')
        else:
            # 테이블이 없으면 CREATE TABLE 수행 (seq PK 및 platform_type 기본값 설정)
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
                # 공간 데이터 타입 정의 (EPSG:3857 구글 메르카토르 좌표계 고정)
                col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')
            
            cur.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({", ".join(col_defs)})')

        # GDF의 각 행(row)을 순회하며 INSERT 쿼리 실행
        for _, row in gdf.iterrows():
            values = [1] # platform_type 기본값 1
            placeholders = ['%s']
            cols = ['platform_type']

            for col in data_cols:
                val = row[col]
                # 결측치(NaN/None) 처리
                if pd.isna(val) if not hasattr(val, '__iter__') or isinstance(val, str) else False: val = None
                values.append(val); placeholders.append('%s'); cols.append(f'"{col}"')
            
            if is_geo:
                geom = row[geom_col]
                if geom is not None and not pd.isna(str(geom)):
                    # 공간 정보를 WKB(Well-Known Binary) 포맷으로 변환하여 삽입
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
    """
    [GPKG 처리 메인 로직]
    다운로드된 프로젝트 폴더 내의 .gpkg 파일들을 분석하여 레이어별로 DB화합니다.
    """
    print(f"    🐘 DB 작업 시작 (ID: {project_id[:13]}...)")
    short_id = project_id[:13]
    now = datetime.now()
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # 프로젝트 메타데이터를 관리할 qfield_data_manage 테이블 생성
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

    # 프로젝트 폴더 내 모든 파일 검사
    for file in os.listdir(project_path):
        if not file.endswith(".gpkg"): continue
        gpkg_path = os.path.join(project_path, file)
        file_stem_only = os.path.splitext(file)[0]
        representative_table_name = None

        try:
            import fiona
            # GPKG 내부에 포함된 모든 레이어(테이블) 목록 추출
            layers = fiona.listlayers(gpkg_path)
            for layer_name in layers:
                # 시스템 테이블 및 스타일 정보 제외
                if layer_name.lower() in ['layer_styles', 'geopackage_contents', 'rtree_...']: continue
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty or 'style' in layer_name.lower(): continue
                
                # 좌표계 변환 (WGS84 등에서 EPSG:3857로 통일)
                if gdf.crs is not None:
                    gdf = gdf.to_crs(epsg=3857)
                else:
                    # 좌표계 정보가 없는 경우 EPSG:5186(중부원점)으로 가정 후 변환
                    gdf.set_crs(epsg=5186, inplace=True)
                    gdf = gdf.to_crs(epsg=3857)

                # 메타데이터 컬럼 추가
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                # 물리 DB 테이블명 규칙: 소유자_프로젝트ID_인덱스
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                if representative_table_name is None: representative_table_name = table_name

                # 실패 시 최대 3번 재시도하며 DB 저장
                for attempt in range(3):
                    try:
                        save_gdf_direct(gdf, table_name, TARGET_SCHEMA)
                        any_updated = True
                        break
                    except Exception as e:
                        if attempt < 2: time.sleep(3)
                        else: raise e
                global_table_index += 1

            # 처리가 완료되면 관리 테이블(qfield_data_manage)에 이력 기록 (ON CONFLICT는 업데이트)
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

    # 데이터 업데이트가 발생했다면 전체 통합 뷰 갱신
    if any_updated:
        update_unified_view()

# --- 하단 메인 로직 (Main Workflow) ---
def get_all_projects():
    """QFieldCloud 서버에서 사용 가능한 모든 프로젝트 목록 가져오기"""
    try: return client.list_projects()
    except: return []

def get_project_dir(project_id):
    """프로젝트별 로컬 저장 경로 생성"""
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def sync_single_project(project_data):
    """특정 프로젝트를 다운로드하고 DB 처리 프로세스 호출"""
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data.get('owner', 'unknown')
    project_path = get_project_dir(p_id)
    # 기존 폴더 삭제 후 새로 생성 (깨끗한 상태 유지)
    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    try:
        # 클라우드에서 프로젝트 파일 전체 다운로드
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        # 다운로드 완료 후 DB 변환 시작
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e: print(f"    ⚠️ 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    """
    [변경 감지 핵심] 
    해당 프로젝트의 작업(Job) 목록 중 'delta_apply'(변경 사항 적용)가 성공한 최신 ID 반환
    """
    try:
        jobs = client.list_jobs(project_id)
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return None
        # 생성 시간순 정렬하여 가장 최신 것 선택
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except: return None


# 마지막으로 처리한 Job ID를 저장하는 메모리 캐시
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 통합 모니터링 시작...")

# 무한 루프 감시 모드
while True:
    try:
        current_projects = get_all_projects()
        for p in current_projects:
            p_id, project_path = p['id'], get_project_dir(p['id'])
            # 현재 프로젝트의 최신 작업 ID 확인
            current_job_id = get_latest_job_id(p_id)
            
            # 1. 처음 보는 프로젝트이거나
            # 2. 로컬에 폴더가 없거나
            # 3. 클라우드에서 새로운 변경 작업(Job ID가 바뀜)이 발생한 경우 동기화 수행
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (Job: {current_job_id})")
                if p_id in last_jobs_cache: time.sleep(5) # 잠시 대기
                sync_single_project(p)
                # 캐시 업데이트
                last_jobs_cache[p_id] = current_job_id
    except Exception as e: print(f"⚠️ 에러: {e}")
    # 설정된 주기만큼 대기 후 다시 클라우드 체크
    time.sleep(CHECK_INTERVAL)