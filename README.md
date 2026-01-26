# PUBG MOBILE Collab Analysis Pipeline

PUBG MOBILE 공식 유튜브 채널에서 콜라보 콘텐츠를 수집, 분류, 집계하는 데이터 파이프라인입니다.

## 주요 기능

- **데이터 수집**: YouTube Data API v3를 통해 영상 메타데이터와 댓글 수집
- **콜라보 분류**: 룰 기반 + GPT API를 활용한 콜라보 콘텐츠 분류
- **지표 집계**: 파트너별 조회수, 좋아요, 댓글 등 핵심 지표 집계
- **리포트 생성**: CSV 형식의 상세 리포트 생성
- **클라우드 업로드**: 결과물 클라우드 스토리지 업로드 지원

## 설치

### 1. 의존성 설치

```bash
cd pubg_collab_pipeline
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env.example`을 복사하여 `.env` 파일을 생성하고 API 키를 입력합니다:

```bash
cp .env.example .env
```

`.env` 파일 내용:

```env
# 필수
YOUTUBE_API_KEY=your_youtube_api_key_here
GPT_API_KEY=your_openai_api_key_here

# 선택 (클라우드 업로드용)
CLOUD_API_KEY=your_cloud_api_key_here
CLOUD_UPLOAD_URL=https://your-cloud-storage.example.com/upload

# 선택 (설정 커스터마이징)
GPT_MODEL=gpt-4o-mini
DB_PATH=./data/pubg_collab.db
OUTPUT_DIR=./output
```

## 사용법

### 전체 파이프라인 실행

```bash
python main.py run --days 365
```

### 개별 단계 실행

```bash
# 1. 데이터 수집 (영상 + 댓글)
python main.py fetch --days 365

# 2. 콜라보 분류
python main.py classify

# 3. 지표 집계
python main.py aggregate --days 365

# 4. CSV 내보내기
python main.py export --out ./output/collab_report.csv
```

### 상세 옵션

```bash
# 전체 데이터 재수집 (증분 아님)
python main.py fetch --days 365 --full

# GPT 없이 룰 기반만 사용
python main.py classify --no-gpt

# 전체 영상 재분류
python main.py classify --reclassify

# 전체 리포트 생성 (여러 CSV 파일)
python main.py export --full --out ./output

# 감성 분석 포함 (GPT 비용 발생)
python main.py export --full --sentiment

# 클라우드 업로드
python main.py export --out ./output/report.csv --upload

# 파이프라인 상태 확인
python main.py status
```

## 출력 파일

### 메인 리포트 (collab_report.csv)

| 컬럼명 | 설명 |
|--------|------|
| partner_name | 콜라보 파트너명 |
| category | 분류 (IP/Brand/Artist/Game/Anime/Movie/Other) |
| region | 대상 지역 (Global/KR/JP/NA/EU/SEA/etc.) |
| video_count | 영상 수 |
| total_views | 총 조회수 |
| total_video_likes | 총 영상 좋아요 수 |
| total_comments | 총 댓글 수 |
| total_comment_likes | 총 댓글 좋아요 수 |
| avg_views | 평균 조회수 |
| like_rate_pct | 좋아요율 (%) |
| comment_rate_pct | 댓글율 (%) |
| top_videos | 조회수 TOP 3 영상 |
| date_range_start | 집계 시작일 |
| date_range_end | 집계 종료일 |

### 샘플 출력

```csv
partner_name,category,region,video_count,total_views,total_video_likes,total_comments,avg_views,like_rate_pct,comment_rate_pct,top_videos
BLACKPINK,Artist,Global,5,45000000,1200000,85000,9000000.0,2.6667,0.1889,"abc123|PUBG x BLACKPINK M/V; def456|Ready For Love"
Lamborghini,Brand,Global,3,28000000,750000,42000,9333333.33,2.6786,0.15,"ghi789|Drive the Legend"
Jujutsu Kaisen,Anime,Global,4,22000000,580000,65000,5500000.0,2.6364,0.2955,"jkl012|Gojo Satoru Arrives"
```

## 프로젝트 구조

```
pubg_collab_pipeline/
├── clients/                    # API 클라이언트
│   ├── youtube_client.py       # YouTube Data API
│   ├── gpt_client.py           # OpenAI GPT API
│   └── cloud_client.py         # 클라우드 스토리지 API
├── pipeline/                   # 파이프라인 모듈
│   ├── fetch.py                # 데이터 수집
│   ├── classify.py             # 콜라보 분류
│   ├── aggregate.py            # 지표 집계
│   └── export.py               # 내보내기/업로드
├── db/                         # 데이터베이스
│   ├── schema.sql              # SQLite 스키마
│   └── models.py               # 데이터 모델
├── prompts/                    # GPT 프롬프트
│   └── collab_classifier.md    # 콜라보 분류 프롬프트
├── output/                     # 출력 디렉토리
├── main.py                     # CLI 진입점
├── config.py                   # 설정 관리
├── requirements.txt            # 의존성
├── .env.example                # 환경변수 예시
└── README.md                   # 이 문서
```

## 데이터베이스 스키마

### videos 테이블
- 영상 메타데이터 및 콜라보 분류 결과 저장
- 주요 컬럼: video_id, title, view_count, like_count, comment_count, collab_partner, collab_category

### comments 테이블
- 영상별 댓글 저장 (최대 200개/영상)
- 주요 컬럼: comment_id, video_id, author_name, text_original, like_count

### collab_agg 테이블
- 파트너별 집계 데이터
- 주요 컬럼: partner_name, total_views, video_count, like_rate, comment_rate

## 콜라보 분류 로직

### 1단계: 룰 기반 분류
- 제목/설명에서 키워드 탐지: `collab`, `x`, `×`, `with`, `콜라보`, `コラボ` 등
- 파트너명 패턴 매칭: `[Partner]`, `x Partner`, `feat. Partner`
- 알려진 파트너 매핑 (BLACKPINK, Lamborghini, Jujutsu Kaisen 등)

### 2단계: GPT 분류 (애매한 케이스)
- 룰 기반으로 확신도가 낮은 경우 GPT API 호출
- 결과 캐싱으로 중복 호출 방지
- 프롬프트: `prompts/collab_classifier.md`

### 분류 카테고리
| 카테고리 | 설명 | 예시 |
|----------|------|------|
| IP | 지식재산권/프랜차이즈 | 캐릭터, 유니버스 |
| Brand | 상업 브랜드 | Lamborghini, McLaren |
| Artist | 아티스트/뮤지션 | BLACKPINK, Alan Walker |
| Game | 게임 크로스오버 | Resident Evil, Metro |
| Anime | 애니메이션/만화 | Dragon Ball, Jujutsu Kaisen |
| Movie | 영화/TV | Godzilla, The Boys |
| Other | 기타 | - |

## 증분 업데이트

파이프라인은 증분 업데이트를 지원합니다:

```bash
# 첫 실행: 전체 365일 수집
python main.py fetch --days 365

# 이후 실행: 마지막 수집 이후 새 영상만 수집
python main.py fetch --days 365
```

- `last_fetched_at` 기준으로 새 영상 탐지
- 기존 영상 통계(조회수 등)는 업데이트됨
- 분류 결과는 유지됨 (`--reclassify` 옵션으로 재분류 가능)

## API 쿼터 관리

### YouTube Data API
- 일일 쿼터: 기본 10,000 units
- 주요 호출 비용:
  - playlistItems.list: 1 unit
  - videos.list: 1 unit
  - commentThreads.list: 1 unit
- 50개씩 배치 호출로 최적화
- 지수 백오프로 레이트 리밋 대응

### GPT API
- 룰 기반으로 확실한 케이스는 GPT 생략
- 결과 캐싱으로 중복 호출 방지
- `gpt-4o-mini` 모델 권장 (비용 효율)

## 클라우드 업로드 설정

현재 구현은 placeholder입니다. 실제 Cloud API 스펙에 맞게 `clients/cloud_client.py`를 수정하세요:

```python
# cloud_client.py의 _do_upload() 메서드를 수정
def _do_upload(self, file_path: Path, metadata: UploadMetadata) -> UploadResult:
    # 실제 API 호출 구현
    ...
```

## 문제 해결

### YouTube API 오류
```
Error: googleapiclient.errors.HttpError 403
```
- API 키 확인
- 일일 쿼터 초과 확인 (Google Cloud Console)
- 댓글 비활성화된 영상은 자동 스킵됨

### GPT API 오류
```
Error: openai.error.AuthenticationError
```
- GPT_API_KEY 확인
- OpenAI 계정 크레딧 확인

### 파이프라인 중단 시
```bash
# 상태 확인
python main.py status

# 증분으로 재실행 (중단 지점부터)
python main.py fetch --days 365
```

## 라이선스

내부 사용 목적 프로젝트입니다.

## 변경 이력

- v1.0.0 (2026-01-26): 초기 버전
