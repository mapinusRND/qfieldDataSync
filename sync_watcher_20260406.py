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
import disaster2convert as dc  # 음성 파일을 텍스트로 변환하는 외부 모듈 (Speech-To-Text)
import requests

# ========== 1. 설정 (Configuration) ==========
# QFieldCloud 접속 정보
URL = "https://qfield.mapinus.com/api/v1/"
USERNAME = "admin"
PASSWORD = "mapinus098!"

# 로컬 저장소 및 DB 접속 정보
BASE_OUTPUT_DIR = "./qfield"         # 다운로드된 프로젝트 파일이 저장될 경로
DB_HOST = "10.10.10.215"
DB_PORT = 5432
DB_NAME = "rnddb"
DB_USER = "postgres"
DB_PASS = "1q2w3e4r"
# SQLAlchemy용 DB URL (엔진 생성용)
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TARGET_SCHEMA = "qfield"             # 데이터를 저장할 DB 스키마 이름
VIEW_NAME = "v_qfield_total_data"    # 모든 프로젝트 데이터를 통합해서 보여줄 뷰 이름
CHECK_INTERVAL = 30                  # 동기화 확인 주기 (초 단위)
# ============================================

# QFieldCloud 클라이언트 객체 생성 및 로그인
client = sdk.Client(url=URL)
client.login(username=USERNAME, password=PASSWORD)

# DB 연결 엔진 생성 (연결 유지 및 재연결 설정 포함)
db_engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=1800)

def get_pg_conn():
    """psycopg2를 이용한 직접적인 DB 연결 객체 반환 (트랜잭션 및 대량 insert용)"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000" # 타임아웃 설정
    )

# 시작 시 대상 스키마가 없으면 생성
with db_engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

def update_unified_view():
    """여러 테이블로 흩어진 프로젝트 데이터들을 하나의 'VIEW'로 통합하는 함수"""
    print(f"    📊 통합 뷰({VIEW_NAME}) 갱신 중...")
    try:
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 관리 테이블에서 현재 등록된 모든 테이블 목록을 가져옴
        cur.execute(f"SELECT id, name, gpkg_name, table_name FROM {TARGET_SCHEMA}.qfield_data_manage")
        rows = cur.fetchall()
        if not rows: return

        view_parts = []
        for r in rows:
            t_name = r['table_name']
            # 실제로 DB에 해당 테이블이 존재하는지 확인
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s)", (t_name,))
            if cur.fetchone()[0]:
                # 고유 번호(seq)를 제외한 모든 컬럼명을 조회
                cur.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = %s AND column_name != 'seq'
                    ORDER BY ordinal_position
                """, (t_name,))
                
                columns = [f'd."{col[0]}"' for col in cur.fetchall()]
                column_string = ", ".join(columns)
                
                # UNION ALL을 통해 각 테이블의 데이터를 하나로 합치는 쿼리문 작성
                part = f"""
                SELECT 
                    '{r['id']}'::text as manage_id, '{r['name']}'::text as project_name,
                    '{r['gpkg_name']}'::text as source_gpkg, '{t_name}'::text as source_table,
                    {column_string}
                FROM {TARGET_SCHEMA}."{t_name}" d
                """
                view_parts.append(part)

        # 수집된 쿼리 파트들을 UNION ALL로 묶어서 뷰 생성
        if view_parts:
            create_view_sql = f"CREATE OR REPLACE VIEW {TARGET_SCHEMA}.{VIEW_NAME} AS " + " UNION ALL ".join(view_parts)
            cur.execute(create_view_sql)
            conn.commit()
            print(f"      ✅ 통합 뷰 생성 완료: {TARGET_SCHEMA}.{VIEW_NAME}")
    except Exception as e:
        print(f"      ⚠️ 뷰 생성 오류: {e}")
    finally:
        cur.close(); conn.close()

def save_gdf_direct(gdf, table_name, schema, project_path):
    """
    GeoDataFrame(공간데이터)을 DB에 물리적으로 저장하는 핵심 로직.
    'record' 컬럼(음성파일명)이 있으면 STT를 실행해 'audio_txt' 컬럼에 저장함.
    """
    conn = get_pg_conn()
    conn.autocommit = False # 트랜잭션 수동 제어
    cur = conn.cursor()
    try:
        # 공간 데이터 여부 확인 (도형 정보가 있는지)
        is_geo = (isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None and not gdf.geometry.isnull().all())
        geom_col = gdf.geometry.name if is_geo else None
        
        # 1. 컬럼 리스트 구성 (음성 텍스트를 담을 audio_txt 컬럼 정의 추가)
        final_cols = []
        for c in gdf.columns:
            if c == geom_col: continue # 도형 컬럼은 마지막에 별도 처리
            final_cols.append(c)
            # 'record'(음성파일경로) 컬럼이 발견되면 바로 뒤에 텍스트 변환용 컬럼 추가
            if c == 'record':
                final_cols.append('audio_txt')

        # 2. DB 테이블 생성 또는 기존 데이터 초기화
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", (schema, table_name))
        if cur.fetchone()[0]:
            cur.execute(f'DELETE FROM {schema}."{table_name}"') # 기존 데이터 삭제 (Full Refresh)
        else:
            # 테이블이 없을 경우 컬럼 타입 정의
            col_defs = ['seq SERIAL PRIMARY KEY', 'platform_type SMALLINT DEFAULT 1']
            for col in final_cols:
                if col == 'audio_txt':
                    col_defs.append(f'"{col}" TEXT')
                else:
                    dtype = str(gdf[col].dtype)
                    if 'int' in dtype: col_defs.append(f'"{col}" BIGINT')
                    elif 'float' in dtype: col_defs.append(f'"{col}" DOUBLE PRECISION')
                    elif 'datetime' in dtype or 'date' in dtype: col_defs.append(f'"{col}" TIMESTAMP')
                    else: col_defs.append(f'"{col}" TEXT') # 기본은 텍스트
            # 공간 데이터면 PostGIS Geometry 컬럼(Web Mercator 3857) 추가
            if is_geo:
                col_defs.append(f'"{geom_col}" GEOMETRY(Geometry, 3857)')
            cur.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({", ".join(col_defs)})')

        # 3. 데이터 행(row)별 반복 처리 및 STT 수행
        for _, row in gdf.iterrows():
            values = [1] # platform_type 기본값
            placeholders = ['%s']
            cols = ['platform_type']

            for col in final_cols:
                cols.append(f'"{col}"')
                placeholders.append('%s')

                # 만약 현재 컬럼이 STT 텍스트 컬럼이라면?
                if col == 'audio_txt':
                    record_file = row.get('record')
                    stt_result = ""
                    # record 컬럼에 파일명이 기재되어 있는지 확인
                    if record_file and isinstance(record_file, str) and record_file.strip():
                        # QField가 다운로드된 로컬 경로에서 실제 오디오 파일 탐색
                        audio_path = os.path.join(project_path, record_file)
                        if os.path.exists(audio_path):
                            print(f"      🎙️ STT 변환 중: {record_file}")
                            try:
                                # 외부 모듈을 호출하여 음성을 텍스트로 변환
                                stt_result = dc.read_audio(audio_path)
                            except Exception as stt_e:
                                print(f"      ⚠️ STT 에러: {stt_e}")
                    values.append(stt_result)
                else:
                    # 일반 데이터 컬럼 처리 (NaN 값은 None으로 변환하여 DB NULL 처리)
                    val = row[col]
                    if pd.isna(val) if not hasattr(val, '__iter__') or isinstance(val, str) else False: val = None
                    values.append(val)
            
            # 도형 데이터가 있으면 WKB(Well-Known Binary) 형식으로 변환하여 추가
            if is_geo:
                geom = row[geom_col]
                if geom is not None and not pd.isna(str(geom)):
                    values.append(wkb_dumps(geom, hex=True, include_srid=True))
                else: values.append(None)
                placeholders.append('%s::geometry'); cols.append(f'"{geom_col}"')
            
            # 최종적으로 DB에 Insert
            cur.execute(f'INSERT INTO {schema}."{table_name}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})', values)
        
        conn.commit() # 전체 성공 시 커밋
    except Exception as e:
        conn.rollback(); raise e # 오류 시 롤백
    finally:
        cur.close(); conn.close()

def process_gpkg_to_db(project_id, project_path, project_name, owner):
    """다운로드된 프로젝트 내의 GeoPackage(.gpkg) 파일들을 분석해 DB로 전송하는 함수"""
    print(f"    🐘 DB 작업 시작 (ID: {project_id[:13]}...)")
    short_id = project_id[:13]
    now = datetime.now()
    # 테이블 이름으로 사용 가능하도록 특수문자 제거
    clean_owner = owner.lower().replace(' ', '_').replace('-', '_')

    # 메타데이터 관리용 테이블 생성 (프로젝트 목록 관리)
    with db_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.qfield_data_manage (
                seq SERIAL PRIMARY KEY, id TEXT, name TEXT, gpkg_name TEXT,
                table_name TEXT, owner TEXT, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_gpkg_per_project UNIQUE (id, gpkg_name)
            );
        """))

    global_table_index = 1
    any_updated = False

    # 프로젝트 폴더 내 모든 gpkg 파일 순회
    for file in os.listdir(project_path):
        if not file.endswith(".gpkg"): continue
        gpkg_path = os.path.join(project_path, file)
        file_stem_only = os.path.splitext(file)[0] # 확장자 제외 파일명
        representative_table_name = None

        try:
            import fiona
            layers = fiona.listlayers(gpkg_path) # gpkg 내부의 레이어 목록 추출
            for layer_name in layers:
                # QGIS 설정 레이어는 제외
                if layer_name.lower() in ['layer_styles', 'geopackage_contents']: continue
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                if gdf.empty: continue
                
                # 좌표계 처리 (기본 3857로 변환, 없으면 5186(중부원점)으로 가정 후 변환)
                if gdf.crs is not None: gdf = gdf.to_crs(epsg=3857)
                else:
                    gdf.set_crs(epsg=5186, inplace=True)
                    gdf = gdf.to_crs(epsg=3857)

                # 메타데이터 추가
                gdf = gdf.assign(owner=owner, reg_date=now, update_at=now)
                table_name = f"{clean_owner}_{short_id}_{global_table_index}"
                if representative_table_name is None: representative_table_name = table_name

                # 안정성을 위해 최대 3번까지 DB 저장 시도
                for attempt in range(3):
                    try:
                        save_gdf_direct(gdf, table_name, TARGET_SCHEMA, project_path)
                        any_updated = True
                        break
                    except Exception as e:
                        if attempt < 2: time.sleep(3)
                        else: raise e
                global_table_index += 1

            # 저장이 완료되면 관리 테이블에 해당 프로젝트/테이블 정보 기록 (이미 있으면 업데이트)
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

    # 데이터가 하나라도 업데이트되었다면 통합 뷰 다시 생성
    if any_updated:
        update_unified_view()

def get_org_projects():
    try:
        resp = requests.get(
            f"{URL}projects/",
            params={"owner": "disaster"},
            headers={"Authorization": f"Token {client.token}"}
        )

        print("📡 projects API status:", resp.status_code)
        print("📡 projects API raw:", resp.text)   # 🔥 이거 핵심

        if resp.status_code == 200:
            data = resp.json()
            print("📡 parsed:", data)

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get('results', [])

        return []
    except Exception as e:
        print(f"⚠️ org 프로젝트 조회 실패: {e}")
        return []

def get_project_collaborators(project_id):
    """프로젝트 참여자 조회"""
    try:
        resp = requests.get(
            f"{URL}projects/{project_id}/collaborators/",
            headers={"Authorization": f"Token {client.token}"}
        )

        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception as e:
        print(f"⚠️ collaborator 조회 실패: {e}")
        return []

def get_all_projects():
    try:
        resp = requests.get(
            f"{URL}projects/",
            headers={"Authorization": f"Token {client.token}"}
        )

        if resp.status_code != 200:
            print(f"⚠️ 전체 프로젝트 조회 실패: {resp.status_code}")
            return []

        data = resp.json()

        projects = data if isinstance(data, list) else data.get('results', [])

        print(f"📦 전체 프로젝트 수: {len(projects)}")

        for p in projects:
            print(f"  - [{p.get('owner')}] {p.get('name')}")

        return projects

    except Exception as e:
        print(f"⚠️ 프로젝트 조회 실패: {e}")
        return []

def get_project_dir(project_id):
    """프로젝트별 로컬 저장 경로 반환"""
    return os.path.join(BASE_OUTPUT_DIR, project_id)

def sync_single_project(project_data):
    """특정 프로젝트의 최신 파일을 다운로드하고 DB 처리 프로세스 호출"""
    p_id, p_name, p_owner = project_data['id'], project_data['name'], project_data.get('owner', 'unknown')
    project_path = get_project_dir(p_id)
    
    # 동기화 전 로컬 폴더 초기화 (클린 다운로드)
    if os.path.exists(project_path): shutil.rmtree(project_path)
    os.makedirs(project_path, exist_ok=True)
    
    try:
        # 클라우드에서 모든 파일 다운로드 (현장 사진, 음성파일 포함)
        client.download_project(project_id=p_id, local_dir=project_path, filter_glob="*", show_progress=False, force_download=True)
        # DB 저장 및 STT 처리 시작
        process_gpkg_to_db(p_id, project_path, p_name, p_owner)
    except Exception as e: print(f"    ⚠️ 다운로드 실패: {e}")

def get_latest_job_id(project_id):
    """QFieldCloud에서 'delta_apply'(변경사항 반영) 작업 중 가장 최근 완료된 작업 ID 조회"""
    try:
        jobs = client.list_jobs(project_id)
        # 'delta_apply' 타입이면서 'finished' 상태인 작업만 필터링
        delta_jobs = [j for j in jobs if j.get('type') == 'delta_apply' and j.get('status') == 'finished']
        if not delta_jobs: return None
        # 생성 시간순 정렬 후 최신 것 반환
        delta_jobs.sort(key=lambda j: j.get('created_at', ''), reverse=True)
        return delta_jobs[0]['id']
    except: return None

# 이전에 확인한 작업 ID를 저장하여 중복 처리를 방지하기 위한 캐시
last_jobs_cache = {}
print(f"[{datetime.now()}] 🚀 통합 모니터링 시작...")

# 무한 루프를 돌며 QFieldCloud의 변경사항을 주기적으로 감시
while True:
    try:
        current_projects = get_all_projects()
        for p in current_projects:
            p_id, project_path = p['id'], get_project_dir(p['id'])
            current_job_id = get_latest_job_id(p_id)
            
            # 다음 조건 중 하나라도 만족하면 동기화 시작:
            # 1. 처음 보는 프로젝트일 때
            # 2. 로컬에 폴더가 없을 때
            # 3. 클라우드에서 새로운 '작업(Job)'이 완료되었을 때 (데이터가 새로 업로드됨)
            if p_id not in last_jobs_cache or not os.path.exists(project_path) or current_job_id != last_jobs_cache[p_id]:
                print(f"[{datetime.now()}] 🔄 변경 감지: {p['name']} (Job: {current_job_id})")
                # 서버 부하 방지를 위한 약간의 대기
                if p_id in last_jobs_cache: time.sleep(5)
                sync_single_project(p)
                # 캐시 갱신
                last_jobs_cache[p_id] = current_job_id
    except Exception as e: print(f"⚠️ 에러: {e}")
    # 설정된 주기만큼 대기 후 다시 확인
    time.sleep(CHECK_INTERVAL)