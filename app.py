"""미니 쇼핑몰 서버: Mini Redis와 Mini DBMS를 함께 쓰는 발표용 데모.

  [데모 페이지] → [쇼핑몰 서버 :8100] → [Mini Redis :8000]
                                      → [Mini DBMS  :9000 (8스레드) / :9001 (1스레드)]

1막  상품 번호 조회: 같은 id 한 개 / id 범위를 인덱스 없이(풀스캔) / B+ 트리로 찾아 비교
2막  인기 상품 검색: Cache-Aside. Redis를 먼저 보고, 없으면 DBMS 조회 후 TTL을 걸어 저장
3막  스레드풀: 무거운 풀스캔 여러 개가 도는 중에 상품 상세(id 조회)가 얼마나 기다리는지 비교
"""
import os
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request, send_from_directory
from werkzeug.exceptions import HTTPException

MINI_REDIS_URL = os.environ.get("MINI_REDIS_URL", "http://127.0.0.1:8000")
DBMS_URL_MULTI = os.environ.get("DBMS_URL_MULTI", "http://127.0.0.1:9000")    # 스레드 N개
DBMS_URL_SINGLE = os.environ.get("DBMS_URL_SINGLE", "http://127.0.0.1:9001")  # 스레드 1개
TABLE = os.environ.get("TABLE", "products")
PORT = int(os.environ.get("PORT", "8100"))

SEARCH_CACHE_TTL = 60          # 인기 상품 검색 결과를 캐시에 두는 시간(초)
REQUEST_TIMEOUT = 60
HEAVY_HEAD_START = 0.05        # 3막: 무거운 요청이 먼저 줄을 서도록 주는 시간차(초)
RANGE_MAX_ROWS = 10_000        # 1막 범위 검색 `id <= N`의 N 상한 (응답 JSON이 너무 커지지 않게)
RANGE_PREVIEW_ROWS = 100       # 1막 범위 검색 결과 중 화면 표에 보여 줄 행 수
MAX_HEAVY = 12                # 1스레드 서버 큐(기본 16)를 넘기지 않도록 제한

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
    try:
        body = response.json()
    except ValueError:   # 큐가 꽉 찼을 때의 HTML 에러 페이지처럼 JSON이 아닌 응답
        raise RuntimeError(
            f"DBMS가 JSON이 아닌 응답을 보냈습니다 (HTTP {response.status_code}): {response.text[:200]}"
        )
    if response.status_code != 200 or "error" in body:
        raise RuntimeError(f"DBMS 오류: {body.get('error')}")
    return body["results"][0], body.get("lock"), round_trip_ms


def sql_text(value):
    """SQL 문자열 리터럴로 쓸 값. 따옴표가 들어오면 파서가 깨지므로 거절한다."""
    if not value or not value.strip():
        raise ValueError("검색어를 입력해 주세요")
    if "'" in value or ";" in value:
        raise ValueError("검색어에 따옴표(')나 세미콜론(;)은 쓸 수 없습니다")
    return f"'{value}'"


def summarize(result, round_trip_ms, lock=None, include_rows=True):
    summary = {
        "columns": result.get("columns"),
        "row_count": len(result["rows"]),
        "used_index": result.get("used_index"),
        "rows_scanned": result.get("rows_scanned"),
        "engine_ms": result.get("elapsed_ms"),     # DBMS 엔진 안에서 실행한 시간
        "total_ms": round(round_trip_ms, 3),       # 쇼핑몰 서버에서 잰 전체 왕복 시간
        "lock_wait_ms": (lock or {}).get("wait_ms"),
    }
    if include_rows:
        summary["rows"] = result["rows"]
    return summary


@app.errorhandler(Exception)
def handle_error(error):
    if isinstance(error, HTTPException):   # 404, 405 같은 HTTP 오류는 원래 상태 코드 그대로
        return jsonify({"error": f"{error.code} {error.name}: {request.path}"}), error.code
    # requests.JSONDecodeError는 RequestException이기도 해서 "연결 불가"보다 먼저 걸러야 한다
    if isinstance(error, requests.JSONDecodeError):
        return jsonify({"error": f"서버 응답이 JSON 형식이 아닙니다: {error}"}), 502
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


@app.get("/api/act1/range")
def act1_product_range():
    """범위 검색: 같은 `id <= N`을 풀스캔 / B+ 트리(시작 리프를 찾은 뒤 옆 리프를 따라감)로 비교.

    Mini DBMS는 WHERE 조건을 하나만 받아서 BETWEEN이나 AND 대신 `id <= N`을 쓴다.
    id가 1부터 시작하므로 결과는 최대 N행이고, 그래서 N에 상한을 둔다.
    """
    upper = int(request.args.get("to", "100"))
    if not 1 <= upper <= RANGE_MAX_ROWS:
        raise ValueError(f"끝 번호는 1 ~ {RANGE_MAX_ROWS:,} 사이로 입력해 주세요")
    sql = f"SELECT * FROM {TABLE} WHERE id <= {upper};"

    scan_result, scan_lock, scan_ms = run_sql(sql, use_index=False)
    index_result, index_lock, index_ms = run_sql(sql, use_index=True)

    return jsonify({
        "sql": sql,
        # 행이 많을 수 있어 전체 행은 보내지 않는다. 결과가 같은지는 여기서 비교한다.
        "full_scan": summarize(scan_result, scan_ms, scan_lock, include_rows=False),
        "index": summarize(index_result, index_ms, index_lock, include_rows=False),
        "same_result": scan_result["rows"] == index_result["rows"],
        # 화면 표에 보여 줄 앞부분만 (B+ 트리 결과 기준, 두 결과가 같으므로 어느 쪽이든 상관없다)
        "preview_rows": index_result["rows"][:RANGE_PREVIEW_ROWS],
    })


# ---------------------------------------------------------------- 2막: 캐시

def search_cache_key(name):
    return f"search:name:{name}"


@app.get("/api/act2")
def act2_popular_search():
    """Cache-Aside: ① Redis 확인 → ② 없으면 DBMS 조회 → ③ 결과를 TTL과 함께 Redis에 저장."""
    name = request.args.get("name", "에어팟 프로")
    literal = sql_text(name)   # 잘못된 검색어는 Redis에 묻기 전에 거절
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

    sql = f"SELECT * FROM {TABLE} WHERE name = {literal};"
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
    """3막의 무거운 검색어. DB에 없는 상품명이라 결과는 0건이다.

    무거운 이유는 '없어서'가 아니라 name에 인덱스가 없어서다. Mini DBMS는 name 조건이면
    (있는 상품이든 없는 상품이든) 100만 행을 끝까지 다 읽는다. 결과 0건이 화면에 보이면
    '하나도 못 찾았는데 전부 읽었다'가 잘 드러나서 없는 이름을 쓴다.
    """
    global _heavy_counter
    with _heavy_counter_lock:
        _heavy_counter += 1
        return f"없는상품_{_heavy_counter}"


# 3막 실행 기록. 응답을 보낸 뒤에도 뒤에서 도는 무거운 검색의 진행 상황을 화면이 다시 물어볼 수 있게 남겨 둔다.
# run_id -> {"t0": 기준 시각, "heavy": [기록...], "light": 기록}. 오래된 것부터 지워 RUN_HISTORY개만 둔다.
RUN_HISTORY = 20
_runs = OrderedDict()
_runs_lock = threading.Lock()


def remember_run(run):
    run_id = uuid.uuid4().hex[:12]
    with _runs_lock:
        _runs[run_id] = run
        while len(_runs) > RUN_HISTORY:
            _runs.popitem(last=False)
    return run_id


def timeline_snapshot(run):
    """지금 시점의 타임라인. 아직 안 끝난 검색은 end_ms가 null이고, now_ms까지 진행 중으로 그린다."""
    heavy = [dict(e) for e in run["heavy"]]   # 뒤에서 도는 스레드가 고치는 중일 수 있어 복사해서 보낸다
    return {
        "now_ms": round(now_ms() - run["t0"], 3),
        "heavy": heavy,
        "light": run["light"],
        "done": all(h["end_ms"] is not None for h in heavy),
    }


@app.get("/api/act3/runs/<run_id>")
def act3_run_status(run_id):
    """3막 실행 하나의 현재 타임라인. 화면이 무거운 검색이 다 끝날 때까지 주기적으로 부른다."""
    with _runs_lock:
        run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "실행 기록이 없습니다 (서버가 다시 켜졌거나 오래된 기록)"}), 404
    return jsonify(timeline_snapshot(run))


@app.get("/api/act3")
def act3_thread_pool():
    mode = request.args.get("mode", "multi")
    base_url = DBMS_URL_SINGLE if mode == "single" else DBMS_URL_MULTI
    heavy_count = max(0, min(int(request.args.get("heavy", "2")), MAX_HEAVY))
    product_id = int(request.args.get("id", "123"))

    heavy_names = [unique_rare_name() for _ in range(heavy_count)]
    light_sql = f"SELECT * FROM {TABLE} WHERE id = {product_id};"

    # 타임라인용 기록. 각 요청을 보낸 시각과 응답을 받은 시각을 t0 기준 ms로 남긴다.
    t0 = now_ms()
    heavy_entries = [
        {
            "name": name,
            "sql": f"SELECT * FROM {TABLE} WHERE name = '{name}';",
            "start_ms": None, "end_ms": None, "engine_ms": None,
            "row_count": None, "rows_scanned": None,   # 끝나면 '결과 0건 · 100만 행 확인'으로 보여 준다
            "error": None,
        }
        for name in heavy_names
    ]

    def timed(entry):
        entry["start_ms"] = round(now_ms() - t0, 3)
        try:
            result, _lock, _ms = run_sql(entry["sql"], True, base_url)
            entry["engine_ms"] = result.get("elapsed_ms")
            entry["row_count"] = len(result["rows"])
            entry["rows_scanned"] = result.get("rows_scanned")
        except Exception as error:   # 뒤에서 실패해도 '끝남'으로 기록해야 화면이 계속 기다리지 않는다
            entry["error"] = str(error)
        entry["end_ms"] = round(now_ms() - t0, 3)

    pool = ThreadPoolExecutor(max_workers=heavy_count + 1)
    try:
        for entry in heavy_entries:
            pool.submit(timed, entry)
        time.sleep(HEAVY_HEAD_START)   # 무거운 요청이 먼저 도착하게 한다
        light_start = round(now_ms() - t0, 3)
        light_result, light_lock, light_ms = pool.submit(run_sql, light_sql, True, base_url).result()
        light_end = round(now_ms() - t0, 3)
    finally:
        # 상세 조회가 끝나면 바로 응답한다. 무거운 검색은 뒤에서 마저 끝나게 두고 기다리지 않는다.
        pool.shutdown(wait=False)

    light = summarize(light_result, light_ms, light_lock)
    engine_ms = light["engine_ms"] or 0
    run = {
        "t0": t0,
        "heavy": heavy_entries,   # 뒤에서 도는 스레드가 계속 채워 넣는 바로 그 리스트
        "light": {"sql": light_sql, "start_ms": light_start, "end_ms": light_end, "engine_ms": engine_ms},
    }
    run_id = remember_run(run)
    timeline = timeline_snapshot(run)
    timeline["now_ms"] = light_end   # 응답을 돌려준 시점. 이때까지 안 끝난 검색은 end_ms가 null (진행 중)
    return jsonify({
        "run_id": run_id,
        "mode": mode,
        "dbms": base_url,
        "heavy_count": heavy_count,
        "light_id": product_id,
        "light_sql": light_sql,
        "light": light,
        # 처리 자체는 금방 끝났는데 전체 시간이 길다면, 그 차이는 줄 서서 기다린 시간이다
        "light_waiting_ms": round(light["total_ms"] - engine_ms, 3),
        # done이 false면 화면이 /api/act3/runs/<run_id>로 나머지 진행 상황을 이어서 받는다
        "timeline": timeline,
    })


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
