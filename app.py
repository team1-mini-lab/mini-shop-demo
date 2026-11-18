"""미니 쇼핑몰 서버: Mini Redis와 Mini DBMS를 함께 쓰는 발표용 데모.

  [데모 페이지] → [쇼핑몰 서버 :8100] → [Mini Redis :8000]
                                      → [Mini DBMS  :9000 (8스레드) / :9001 (1스레드)]

1막  상품 번호 조회: 같은 id를 인덱스 없이(풀스캔) / B+ 트리로 찾아 비교
2막  인기 상품 검색: Cache-Aside. Redis를 먼저 보고, 없으면 DBMS 조회 후 TTL을 걸어 저장
3막  스레드풀: 무거운 풀스캔 여러 개가 도는 중에 상품 상세(id 조회)가 얼마나 기다리는지 비교
"""
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request, send_from_directory

MINI_REDIS_URL = os.environ.get("MINI_REDIS_URL", "http://127.0.0.1:8000")
DBMS_URL_MULTI = os.environ.get("DBMS_URL_MULTI", "http://127.0.0.1:9000")    # 스레드 N개
DBMS_URL_SINGLE = os.environ.get("DBMS_URL_SINGLE", "http://127.0.0.1:9001")  # 스레드 1개
TABLE = os.environ.get("TABLE", "products")
PORT = int(os.environ.get("PORT", "8100"))

SEARCH_CACHE_TTL = 60          # 인기 상품 검색 결과를 캐시에 두는 시간(초)
REQUEST_TIMEOUT = 60
HEAVY_HEAD_START = 0.05        # 3막: 무거운 요청이 먼저 줄을 서도록 주는 시간차(초)
MAX_HEAVY = 12                 # 1스레드 서버 큐(기본 16)를 넘기지 않도록 제한

app = Flask(__name__, static_folder="static")
app.json.ensure_ascii = False


# ---------------------------------------------------------------- 공통

def now_ms():
    return time.perf_counter() * 1000


def run_sql(sql, use_index=True, base_url=DBMS_URL_MULTI):
    """DBMS에 SQL 하나를 보내고 (첫 번째 결과, lock 정보, 쇼핑몰 서버에서 잰 왕복 시간)을 돌려준다."""
    started = now_ms()
    response = requests.post(
        f"{base_url}/query",
        json={"sql": sql, "use_index": use_index},
        timeout=REQUEST_TIMEOUT,
    )
    round_trip_ms = now_ms() - started
    body = response.json()
    if response.status_code != 200 or "error" in body:
        raise RuntimeError(f"DBMS 오류: {body.get('error')}")
    return body["results"][0], body.get("lock"), round_trip_ms


def sql_text(value):
    """SQL 문자열 리터럴로 쓸 값. 따옴표가 들어오면 파서가 깨지므로 거절한다."""
    if not value or "'" in value or ";" in value:
        raise ValueError("검색어에 따옴표(')나 세미콜론(;)은 쓸 수 없습니다")
    return f"'{value}'"


def summarize(result, round_trip_ms, lock=None):
    return {
        "rows": result["rows"],
        "row_count": len(result["rows"]),
        "used_index": result.get("used_index"),
        "rows_scanned": result.get("rows_scanned"),
        "engine_ms": result.get("elapsed_ms"),     # DBMS 엔진 안에서 실행한 시간
        "total_ms": round(round_trip_ms, 3),       # 쇼핑몰 서버에서 잰 전체 왕복 시간
        "lock_wait_ms": (lock or {}).get("wait_ms"),
    }


@app.errorhandler(Exception)
def handle_error(error):
    status = 400 if isinstance(error, ValueError) else 502
    if isinstance(error, requests.RequestException):
        return jsonify({"error": f"서버에 연결할 수 없습니다: {error}"}), 502
    return jsonify({"error": str(error)}), status


# ---------------------------------------------------------------- 페이지

@app.get("/")
def demo_page():
    return send_from_directory(app.static_folder, "index.html")


# ---------------------------------------------------------------- 1막: 인덱스

@app.get("/api/act1")
def act1_product_detail():
    product_id = int(request.args.get("id", "999999"))
    sql = f"SELECT * FROM {TABLE} WHERE id = {product_id};"

    scan_result, scan_lock, scan_ms = run_sql(sql, use_index=False)
    index_result, index_lock, index_ms = run_sql(sql, use_index=True)

    return jsonify({
        "sql": sql,
        "full_scan": summarize(scan_result, scan_ms, scan_lock),
        "index": summarize(index_result, index_ms, index_lock),
        "same_result": scan_result["rows"] == index_result["rows"],
    })


# ---------------------------------------------------------------- 2막: 캐시

def search_cache_key(name):
    return f"search:name:{name}"


@app.get("/api/act2")
def act2_popular_search():
    """Cache-Aside: ① Redis 확인 → ② 없으면 DBMS 조회 → ③ 결과를 TTL과 함께 Redis에 저장."""
    name = request.args.get("name", "에어팟 프로")
    key = search_cache_key(name)
    started = now_ms()

    redis_started = now_ms()
    cached = requests.get(f"{MINI_REDIS_URL}/get", params={"key": key}, timeout=5)
    redis_get_ms = now_ms() - redis_started

    if cached.status_code == 200:
        return jsonify({
            "source": "cache",
            "key": key,
            "rows": cached.json()["value"],
            "redis_ms": round(redis_get_ms, 3),
            "db_ms": None,
            "total_ms": round(now_ms() - started, 3),
        })

    sql = f"SELECT * FROM {TABLE} WHERE name = {sql_text(name)};"
    result, lock, db_ms = run_sql(sql)

    set_started = now_ms()
    requests.post(
        f"{MINI_REDIS_URL}/set",
        json={"key": key, "value": result["rows"], "ttl": SEARCH_CACHE_TTL},
        timeout=5,
    )
    redis_set_ms = now_ms() - set_started

    return jsonify({
        "source": "db",
        "key": key,
        "sql": sql,
        "rows": result["rows"],
        "rows_scanned": result.get("rows_scanned"),
        "engine_ms": result.get("elapsed_ms"),
        "redis_ms": round(redis_get_ms + redis_set_ms, 3),
        "db_ms": round(db_ms, 3),
        "total_ms": round(now_ms() - started, 3),
        "ttl": SEARCH_CACHE_TTL,
    })


@app.post("/api/act2/reset")
def act2_reset():
    """리허설을 반복할 수 있도록 검색 결과 캐시를 지운다 (무효화)."""
    name = request.args.get("name", "에어팟 프로")
    response = requests.delete(
        f"{MINI_REDIS_URL}/delete", params={"key": search_cache_key(name)}, timeout=5
    )
    return jsonify(response.json())


# ---------------------------------------------------------------- 3막: 스레드풀

_heavy_counter = 0
_heavy_counter_lock = threading.Lock()


def unique_rare_name():
    """3막의 무거운 검색어. 매번 다른 이름이라 어떤 캐시에도 걸리지 않고 반드시 풀스캔한다."""
    global _heavy_counter
    with _heavy_counter_lock:
        _heavy_counter += 1
        return f"검색어_{int(time.time())}_{_heavy_counter}"


@app.get("/api/act3")
def act3_thread_pool():
    mode = request.args.get("mode", "multi")
    base_url = DBMS_URL_SINGLE if mode == "single" else DBMS_URL_MULTI
    heavy_count = min(int(request.args.get("heavy", "4")), MAX_HEAVY)
    product_id = int(request.args.get("id", "123"))

    heavy_sql = [
        f"SELECT * FROM {TABLE} WHERE name = '{unique_rare_name()}';" for _ in range(heavy_count)
    ]
    light_sql = f"SELECT * FROM {TABLE} WHERE id = {product_id};"

    with ThreadPoolExecutor(max_workers=heavy_count + 1) as pool:
        heavy_futures = [pool.submit(run_sql, sql, True, base_url) for sql in heavy_sql]
        time.sleep(HEAVY_HEAD_START)   # 무거운 요청이 먼저 도착하게 한다
        light_future = pool.submit(run_sql, light_sql, True, base_url)

        light_result, light_lock, light_ms = light_future.result()
        heavy = [summarize(*_reorder(f.result())) for f in heavy_futures]

    light = summarize(light_result, light_ms, light_lock)
    engine_ms = light["engine_ms"] or 0
    return jsonify({
        "mode": mode,
        "dbms": base_url,
        "heavy_count": heavy_count,
        "light_sql": light_sql,
        "light": light,
        # 처리 자체는 금방 끝났는데 전체 시간이 길다면, 그 차이는 줄 서서 기다린 시간이다
        "light_waiting_ms": round(light["total_ms"] - engine_ms, 3),
        "heavy": [{k: h[k] for k in ("engine_ms", "total_ms", "rows_scanned")} for h in heavy],
    })


def _reorder(run_sql_output):
    result, lock, round_trip_ms = run_sql_output
    return result, round_trip_ms, lock


@app.get("/api/status")
def status():
    """세 서버가 켜져 있는지 확인한다. 발표 직전 점검용."""
    def alive(url, path):
        try:
            return requests.get(f"{url}{path}", timeout=2).status_code < 500
        except requests.RequestException:
            return False

    return jsonify({
        "mini_redis": alive(MINI_REDIS_URL, "/"),
        "dbms_multi": alive(DBMS_URL_MULTI, "/stats"),
        "dbms_single": alive(DBMS_URL_SINGLE, "/stats"),
    })


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=PORT, threaded=True)
