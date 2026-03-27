from qfieldcloud_sdk import sdk
from datetime import datetime
import pytz

KST = pytz.timezone('Asia/Seoul')

def to_kst(utc_str):
    if not utc_str:
        return None
    dt = datetime.fromisoformat(utc_str)
    return dt.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S KST')

client = sdk.Client(url="https://qfield.mapinus.com/api/v1/")
client.login(username="admin", password="mapinus098!")

project_id = "459ecb1f-5ec1-409b-becc-350449bea177"

print("=== 전체 잡 목록 (KST) ===")
jobs = client.list_jobs(project_id)
for j in jobs:
    print(f"  ID: {j.get('id')}")
    print(f"  타입: {j.get('type')}")
    print(f"  상태: {j.get('status')}")
    print(f"  생성시간(KST): {to_kst(j.get('created_at'))}")
    print(f"  ---")