# AI News Pipeline

주가 이벤트와 관련된 경제 뉴스를 수집하고, 분류와 감성 분석을 거쳐 DB에 적재한 뒤, 이벤트 요약까지 생성하는 FastAPI 기반 파이프라인입니다.

이 프로젝트는 크게 두 가지 목적을 가집니다.

- 이벤트 기반 뉴스 수집: DB에 등록된 이벤트 구간 또는 당일 활성 종목 기준으로 뉴스 수집
- 이벤트 요약 생성: 수집된 뉴스 중 관련성이 높은 기사만 골라 OpenAI로 이벤트 요약 생성

## 주요 기능

- 연합뉴스 기사 크롤링
  - 네이버 뉴스 검색 결과에서 연합뉴스 기사 URL을 찾고 본문을 파싱합니다.
- 이벤트 단위 수집 오케스트레이션
  - DB의 `events` 테이블 상태를 기준으로 크롤링 대상과 기간을 생성합니다.
- CSV 기반 중간 산출물 관리
  - `crawled -> tagged -> senti` 순서로 단계별 결과를 파일로 남깁니다.
- 뉴스 주제 분류
  - 로컬에 포함된 계층형 분류 모델로 기사 태그를 예측합니다.
- 뉴스 감성 점수 계산
  - `snunlp/KR-FinBert-SC` 기반으로 제목/본문 감성 점수를 계산합니다.
- DB 적재 및 관계 연결
  - `news`, `news_stocks`, `news_tags`, `event_news` 테이블로 데이터를 적재합니다.
- 이벤트 요약 생성
  - 관련 뉴스 후보를 평가하고, 최종 선택된 기사들만 사용해 이벤트 요약을 생성합니다.

## 전체 흐름

```text
events / stocks 조회
  -> 크롤링 대상 생성
  -> 연합뉴스 기사 수집
  -> files/crawled CSV 저장
  -> 태그 분류
  -> files/tagged CSV 저장
  -> 감성 점수 계산
  -> files/senti CSV 저장
  -> DB 적재 및 event-news 연결
  -> OpenAI 기반 이벤트 요약 생성
```

## 프로젝트 구조

```text
ai/
├─ pipeline/
│  ├─ main.py                         # FastAPI 진입점
│  ├─ api/v1/news.py                 # 크롤링/요약 API
│  ├─ crawlers/
│  │  ├─ base.py
│  │  └─ yonhap_crawler.py           # 연합뉴스 크롤러
│  ├─ models/
│  │  └─ econ_news_hierarchical/     # 뉴스 태깅 모델 파일
│  ├─ schemas/                       # 요청/응답 스키마
│  └─ services/
│     ├─ container.py                # 서비스 조립
│     ├─ crawl_orchestrator.py       # 전체 파이프라인 실행
│     ├─ crawl_service.py
│     ├─ csv_service.py
│     ├─ event_news_relevance_service.py
│     ├─ relevance_scorer.py
│     ├─ request_generator.py        # DB에서 크롤링 대상 생성
│     ├─ sentiment_predictor.py      # 감성 점수 계산
│     ├─ summarize_service.py        # OpenAI 요약 생성
│     ├─ tag_service.py              # 기사 태깅
│     └─ transfer_service.py         # DB 적재
├─ files/
│  ├─ crawled/                       # 원본 수집 CSV
│  ├─ tagged/                        # 태깅 완료 CSV
│  └─ senti/                         # 감성 점수 반영 CSV
├─ requirements.txt
├─ test_logic.py
└─ verify_transfer_optimization.py
```

## 기술 스택

- Python 3.12 계열 가정
- FastAPI / Uvicorn
- httpx / BeautifulSoup4
- pandas / numpy
- torch / transformers
- PyMySQL
- OpenAI Python SDK

## 사전 요구사항

- Python 가상환경
- MySQL 호환 DB
- OpenAI API Key
- 로컬 모델 실행이 가능한 환경
  - 태깅 모델 파일이 저장소에 포함되어 있어 용량이 큽니다.
  - 감성 모델은 실행 시 Hugging Face 모델을 로드합니다.

## 설치

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 환경변수

루트의 `.env` 파일에 아래 값을 설정해야 합니다.

```env
OPENAI_API_KEY=
OPENAI_MODEL=gpt-5.4-mini

DB_HOST=
DB_PORT=3306
DB_USER=
DB_PASSWORD=
DB_NAME=
DB_CHARSET=utf8mb4
```

코드상 선택적으로 사용할 수 있는 값도 있습니다.

- `OPENAI_TIMEOUT`
- `OPENAI_MAX_CONCURRENCY`
- `OPENAI_MAX_RETRIES`
- `USER_AGENT`

## 실행 방법

서버 실행:

```bash
python pipeline/main.py
```

기본 주소:

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`

## API 엔드포인트

### `GET /api/v1/news/crawl`

DB의 `events` 테이블에서 `crawl_status = INACTIVE` 이고 `status = ACTIVE` 인 이벤트를 읽어 전체 크롤링 파이프라인을 백그라운드로 실행합니다.

포함 작업:

- 이벤트 대상 생성
- 기사 수집
- CSV 저장
- 태깅
- 감성 점수 계산
- DB 적재
- 이벤트 상태 전환

### `GET /api/v1/news/crawl-today`

`stocks` 테이블에서 활성 종목을 읽어 전일 기준 하루치 뉴스 수집을 백그라운드로 실행합니다.

특징:

- 이벤트 연결 없이 종목별 일간 뉴스 수집
- 결과는 CSV와 DB 적재까지 이어짐

### `GET /api/v1/news/summary`

요약이 비어 있고 크롤링이 완료된 활성 이벤트를 대상으로 요약을 생성합니다.

### `GET /api/v1/news/summary/stock/{stockId}`

특정 종목의 이벤트만 대상으로 요약을 생성합니다.

### `GET /api/v1/news/summary/event/{eventId}`

특정 이벤트 하나를 강제로 다시 요약합니다.

## DB 동작 개요

`RequestGenerator`와 `TransferService`는 MySQL 연결을 사용해 아래 흐름으로 동작합니다.

- `events`에서 크롤링 대상 조회
- 처리 시작 시 `crawl_status`를 `PENDING`으로 전환
- 적재 완료 후 `crawl_status`를 `ACTIVE`로 전환
- 실패 시 이벤트 상태를 복구
- 뉴스/태그/종목/이벤트 관계 테이블에 `INSERT IGNORE` 방식으로 적재

README만으로 전체 스키마를 복원할 수는 없지만, 코드상 최소한 아래 테이블들이 필요합니다.

- `stocks`
- `events`
- `news`
- `tags`
- `news_stocks`
- `news_tags`
- `event_news`

## 생성되는 파일

기본 출력 디렉터리는 아래와 같습니다.

- `files/crawled`: 크롤링 원본 CSV
- `files/tagged`: 태그 예측이 반영된 CSV
- `files/senti`: 감성 점수가 반영된 최종 CSV

파일명 예시:

- 일반 일간 수집: `삼성전자_20260402_20260402.csv`
- 이벤트 수집: `유한양행_event_444_20181101_20181107.csv`

## 요약 로직

요약은 단순 기사 합치기가 아니라 아래 순서로 진행됩니다.

1. `event_news`에서 이벤트별 뉴스 후보를 조회합니다.
2. OpenAI 모델이 기사별 관련성을 다시 평가합니다.
3. 조정된 relevance score를 DB에 반영합니다.
4. 관련성이 높은 기사만 선택합니다.
5. 선택 기사 기반으로 이벤트 summary를 생성합니다.

즉, `/summary` 엔드포인트는 크롤링 이후의 후처리 단계입니다.

## 테스트 및 점검 스크립트

- `test_logic.py`
  - 오케스트레이터, 요청 생성기, 크롤러 메서드 존재 여부와 상태 전환 흐름을 점검하는 스크립트입니다.
- `verify_transfer_optimization.py`
  - `RequestGenerator`와 `TransferService`의 DB 연결 공유 흐름을 검증하는 점검 스크립트입니다.

실행 예시:

```bash
python test_logic.py
python verify_transfer_optimization.py
```

## 주의사항

- 현재 크롤러는 연합뉴스 소스에 맞춰 구현되어 있습니다.
- `.env`와 DB 스키마가 준비되지 않으면 API는 정상 동작하지 않습니다.
- 감성 분석과 요약 생성에는 외부 모델 로딩 및 API 호출 비용이 발생할 수 있습니다.
- 저장소에 이미 많은 산출 CSV가 포함되어 있어 작업 디렉터리 용량이 커질 수 있습니다.
- 기존 `README.md`는 인코딩이 깨져 있었기 때문에, 현재 코드를 기준으로 새로 정리했습니다.

## 빠른 시작

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python pipeline/main.py
```

서버가 뜬 뒤 `http://127.0.0.1:8000/docs` 에서 API를 바로 호출할 수 있습니다.
