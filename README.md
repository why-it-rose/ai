# AI Project

## 프로젝트 개요
이 프로젝트는 **why-it-rose의 뉴스 크롤링, 데이터 처리, 태깅**을 제공하는 FastAPI 기반의 웹 애플리케이션입니다.

## 주요 기능
- **API 엔드포인트**: `/api/v1` 경로를 통해 뉴스 데이터를 제공
- **FastAPI**를 사용한 경량화된 웹 서버
- **Uvicorn**을 사용한 ASGI 서버 실행

## 기술 스택
- **언어**: Python
- **프레임워크**: FastAPI
- **서버**: Uvicorn

## 실행 방법
1. 프로젝트를 클론합니다:
   ```bash
   git lfs install
   git clone <repository-url>
   cd <repository-folder>"# ai" 
   ```

2. 의존성을 설치합니다:  
   ```bash
   pip install -r requirements.txt
   ```
3. 서버를 실행합니다:  
    ```bash
   python pipeline/main.py
   ```
4. 브라우저에서 API 문서를 확인합니다:
   ```
   - Swagger UI: http://127.0.0.1:8000/docs
   - ReDoc: http://127.0.0.1:8000/redoc
   ```
## 디렉토리 구조
   ```
   project/
   ├── pipeline/
   │   ├── main.py       # FastAPI 애플리케이션 진입점
   │   └── api/
   │       └── v1/
   │           └── news.py  # 뉴스 관련 라우터
   ├── README.md          # 프로젝트 설명 파일
   └── requirements.txt   # 의존성 목록
   ```
