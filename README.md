# 미니 쇼핑몰 데모 (Mini Redis × Mini DBMS)

직접 만든 **Mini Redis**(해시 테이블 키-값 저장소)와 **Mini DBMS**(SQL 처리기 + B+ 트리 + 스레드풀 API 서버)를
쇼핑몰 서버 하나로 엮어서, 실제 서비스에서 두 시스템이 각각 어떤 역할을 하는지 보여 주는 데모입니다.

```
[데모 페이지] → [쇼핑몰 서버 :8100] → [Mini Redis :8000]
                                    → [Mini DBMS  :9000 (8스레드) / :9001 (1스레드)]
```

| 막 | 상황 | 비교 |
|---|---|---|
| 1막 · 인덱스 | 상품 번호로 상세 페이지 조회 | 같은 `WHERE id = ?`를 풀스캔 vs B+ 트리 |
| 2막 · 캐시 | 인기 상품 검색이 몰릴 때 | 인덱스 없는 `WHERE name = ?` 결과를 캐시 MISS vs HIT (Cache-Aside, TTL 60초) |
| 3막 · 스레드풀 | 무거운 검색이 도는 중에 상세 페이지 조회 | DBMS 스레드 1개 vs 8개에서 상세 조회가 기다리는 시간 |

## 필요한 저장소

| 저장소 | 역할 |
|---|---|
| [mini-redis](https://github.com/team1-mini-lab/mini-redis) | Mini Redis 서버 |
| mini-dbms | Mini DBMS 서버와 상품 데이터 생성 스크립트 |
| 이 저장소 | 쇼핑몰 서버와 데모 페이지 |

## 폴더 구성

```
├── app.py              쇼핑몰 서버 (Flask)
├── static/index.html   데모 페이지
└── requirements.txt
```

## 실행 방법

Python 3이 필요합니다. 아래 명령은 macOS 기준이며, 가상환경 안에서는 `python`, 밖에서는 `python3`를 씁니다.
(`pip`이나 `python`이 없다고 나오면 가상환경이 켜져 있지 않은 것입니다.)

### 1. 상품 데이터 100만 건 만들기 (처음 한 번만)

DBMS 서버가 **꺼진 상태**에서, mini-dbms 폴더에서 실행합니다. 약 2초 걸립니다.

```bash
python3 bench/make_products.py            # 이미 있으면 건너뜀
python3 bench/make_products.py --reset    # 지우고 새로 만들기
```

### 2. 쇼핑몰 서버 준비 (처음 한 번만)

이 저장소 폴더에서 실행합니다.

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. 서버 4개 켜기 (터미널 4개)

| 터미널 | 폴더 | 명령 |
|---|---|---|
| 1 | mini-redis | `source .venv/bin/activate` 후 `python -m mini_redis.server` |
| 2 | mini-dbms | `python3 server.py --port 9000 --workers 8 --read-only` |
| 3 | mini-dbms | `python3 server.py --port 9001 --workers 1 --queue 128 --read-only` |
| 4 | 이 저장소 | `source .venv/bin/activate` 후 `python app.py` |

- DBMS 서버는 켤 때 100만 건으로 B+ 트리를 만드느라 2~3초 걸립니다.
- 두 DBMS 서버는 같은 데이터 파일을 보므로 반드시 `--read-only`로 켭니다.

### 4. 데모 페이지 열기

브라우저에서 `http://localhost:8100`을 엽니다. 맨 위에 세 서버가 모두 **켜짐**으로 나오면 준비 완료입니다.

## 예상 결과 (상품 100만 건)

| 막 | 느린 쪽 | 빠른 쪽 |
|---|---|---|
| 1막 | 풀스캔 약 1.8초 (100만 행 확인) | B+ 트리 약 0.03ms (1행 확인) |
| 2막 | 캐시 MISS 약 1.8초 | 캐시 HIT 수 ms |
| 3막 | 스레드 1개: 약 3.6초 (무거운 검색 2개를 기다림) | 스레드 8개: 수 ms |

## 데모 상품

| id | 이름 |
|---|---|
| 500,000 | 에어팟 프로 |
| 500,001 | 갤럭시 버즈 |
| 750,000 | 맥북 에어 |
| 900,000 | 아이패드 미니 |
| 990,000 | 다이슨 에어랩 |

## API

| 엔드포인트 | 하는 일 |
|---|---|
| `GET /api/act1?id=999999` | 같은 id를 `use_index=false`, `true`로 한 번씩 조회해서 비교 |
| `GET /api/act2?name=에어팟 프로` | Redis HIT면 바로 응답, MISS면 DBMS 조회 후 TTL 60초로 저장 |
| `POST /api/act2/reset?name=에어팟 프로` | 해당 검색 캐시 삭제 (무효화) |
| `GET /api/act3?mode=single\|multi&heavy=2&id=123` | 캐시에 없는 풀스캔 `heavy`개를 먼저 보내고 0.05초 뒤 id 조회를 보내서, id 조회의 응답 시간과 기다린 시간을 반환 |
| `GET /api/status` | 세 서버가 켜져 있는지 확인 |

## 설정 (환경 변수, 생략하면 기본값)

| 이름 | 기본값 | 설명 |
|---|---|---|
| `MINI_REDIS_URL` | `http://127.0.0.1:8000` | Mini Redis 주소 |
| `DBMS_URL_MULTI` | `http://127.0.0.1:9000` | 스레드 N개 DBMS |
| `DBMS_URL_SINGLE` | `http://127.0.0.1:9001` | 스레드 1개 DBMS |
| `TABLE` | `products` | 조회할 테이블 |
| `PORT` | `8100` | 쇼핑몰 서버 포트 |

## 시연 체크리스트

- [ ] 2막 전에 **캐시 비우기**를 눌러 첫 검색이 MISS가 되게 하기 (TTL이 60초라 리허설 직후면 HIT가 남아 있을 수 있음)
- [ ] 3막은 **스레드 1개 → 스레드 8개** 순서로 누르기
- [ ] 3막 무거운 검색은 2개면 충분 (1스레드에서 약 3.6초 대기)
- [ ] 리허설을 반복할 때는 3막 뒤에 5초쯤 쉬기 (8스레드 모드의 무거운 검색이 뒤에서 마저 실행 중)
- [ ] 시연 녹화 영상 백업
