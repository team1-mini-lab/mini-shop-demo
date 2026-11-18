# 미니 쇼핑몰 데모 (Mini Redis × Mini DBMS)

```
[데모 페이지] → [쇼핑몰 서버 :8100] → [Mini Redis :8000]
                                    → [Mini DBMS  :9000 (8스레드) / :9001 (1스레드)]
```

## 폴더 구성

```
showcase/
├── app.py              쇼핑몰 서버 (Flask)
└── static/index.html   데모 페이지
```

## 실행 순서 (터미널 4개)

```bash
# 1. Mini Redis  (mini-redis 폴더에서)
python -m mini_redis.server

# 2. Mini DBMS 8스레드  (mini-dbms 폴더에서, 상품 데이터 생성이 끝난 뒤에 켜기)
python3 server.py --port 9000 --workers 8 --read-only

# 3. Mini DBMS 1스레드  (mini-dbms 폴더에서)
python3 server.py --port 9001 --workers 1 --queue 128 --read-only

# 4. 쇼핑몰 서버  (showcase 폴더에서, Flask와 requests 필요: pip install flask requests)
python app.py
```

브라우저에서 `http://localhost:8100`을 열면 됩니다. 페이지 맨 위에 세 서버가 켜져 있는지 표시됩니다.

## 설정 (환경 변수, 생략하면 기본값)

| 이름 | 기본값 | 설명 |
|---|---|---|
| `MINI_REDIS_URL` | `http://127.0.0.1:8000` | Mini Redis 주소 |
| `DBMS_URL_MULTI` | `http://127.0.0.1:9000` | 스레드 N개 DBMS |
| `DBMS_URL_SINGLE` | `http://127.0.0.1:9001` | 스레드 1개 DBMS |
| `TABLE` | `products` | 조회할 테이블 |
| `PORT` | `8100` | 쇼핑몰 서버 포트 |

## API

| 엔드포인트 | 하는 일 |
|---|---|
| `GET /api/act1?id=999999` | 같은 id를 `use_index=false`, `true`로 한 번씩 조회해서 비교 |
| `GET /api/act2?name=에어팟 프로` | Cache-Aside. Redis HIT면 바로 응답, MISS면 DBMS 조회 후 TTL 60초로 저장 |
| `POST /api/act2/reset?name=에어팟 프로` | 해당 검색 캐시 삭제 (리허설 반복용) |
| `GET /api/act3?mode=single\|multi&heavy=2&id=123` | 캐시에 없는 풀스캔 `heavy`개를 먼저 보내고 0.05초 뒤 id 조회를 보내서, id 조회의 응답 시간과 기다린 시간을 반환 |
| `GET /api/status` | 세 서버가 켜져 있는지 확인 |

## 발표 전 체크리스트

- [ ] 2막 시연 전에 **캐시 비우기**를 눌러 첫 검색이 MISS가 되게 하기 (TTL이 60초라 리허설 직후면 HIT가 남아 있을 수 있음)
- [ ] 3막은 **스레드 1개 → 스레드 8개** 순서로 누르기
- [ ] 3막 무거운 검색은 2개(1스레드에서 약 3.6초 대기)면 충분 (1스레드 서버 큐가 넘치지 않게 최대 12개로 제한)
- [ ] 시연 녹화 영상 백업
