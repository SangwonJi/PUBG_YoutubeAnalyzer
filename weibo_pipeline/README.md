# PUBG Weibo Collab Analysis Pipeline

和平精英(Game for Peace) 공식 웨이보 계정에서 콜라보/联动 콘텐츠를 수집, 분류, 집계하는 데이터 파이프라인입니다.

> YouTube 버전 파이프라인(`PUBG_YoutubeAnalyzer`)의 Weibo 대응 프로젝트

## 주요 기능

* **데이터 수집**: m.weibo.cn 모바일 API를 통해 게시물 메타데이터와 댓글 수집
* **콜라보 분류**: 룰 기반 + GPT API를 활용한 联动/합작 콘텐츠 분류
* **지표 집계**: 파트너별 转发/评论/点赞 등 핵심 지표 집계
* **리포트 생성**: CSV 형식의 상세 리포트 생성
* **클라우드 업로드**: 결과물 클라우드 스토리지 업로드 지원

## 대상 계정

| 필드 | 값 |
|------|-----|
| 계정명 | 和平精英 |
| UID | 7095404909 |
| URL | https://weibo.com/u/7095404909 |
| 플랫폼 | Sina Weibo (新浪微博) |

## 설치

### 1. 의존성 설치

```bash
cd pubg_weibo_analyzer
pip install -r requirements.txt
```

### 2. 환경 변수 설정

```bash
cp .env.example .env
```

`.env` 파일에 API 키를 입력합니다:

```env
# 필수 (콜라보 분류용)
GPT_API_KEY=your_openai_api_key_here

# 선택 (레이트 리밋 완화)
WEIBO_COOKIE=your_weibo_cookie_here
```

### 3. 웨이보 쿠키 설정 (선택)

쿠키 없이도 기본 수집이 가능하지만, 쿠키를 설정하면 레이트 리밋이 완화됩니다:

1. 브라우저에서 `m.weibo.cn`에 로그인
2. DevTools > Application > Cookies에서 쿠키 값 복사
3. `.env`의 `WEIBO_COOKIE`에 붙여넣기

## 사용법

### 전체 파이프라인 실행

```bash
python main.py run --days 365
```

### 개별 단계 실행

```bash
# 1. 데이터 수집 (게시물 + 댓글)
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
# 전체 데이터 재수집
python main.py fetch --days 365 --full

# 댓글 없이 게시물만 수집
python main.py fetch --days 365 --no-comments

# GPT 없이 룰 기반만 사용
python main.py classify --no-gpt

# 전체 게시물 재분류
python main.py classify --reclassify

# 전체 리포트 생성 (여러 CSV 파일)
python main.py export --full --out ./output

# 클라우드 업로드
python main.py export --out ./output/report.csv --upload

# 파이프라인 상태 확인
python main.py status
```

## YouTube 파이프라인과의 차이점

| 항목 | YouTube 버전 | Weibo 버전 |
|------|-------------|-----------|
| API | YouTube Data API v3 (공식) | m.weibo.cn JSON API (비공식) |
| 인증 | API Key | Cookie (선택) |
| 콘텐츠 단위 | 영상 (video) | 게시물 (post/微博) |
| 조회수 | view_count | ❌ (비공개, 일부 read_count만) |
| 좋아요 | like_count | attitudes_count (点赞) |
| 댓글 | comment_count | comments_count (评论) |
| 공유/전달 | ❌ | reposts_count (转发) |
| 콜라보 키워드 | collab, x, with | 联动, 合作, ×, 携手, 跨界 |
| 레이트 리밋 | 일일 10,000 units | 요청 간 딜레이 기반 |
| 페이지네이션 | pageToken | since_id / page |

## 출력 파일

### 메인 리포트 (collab_report.csv)

| 컬럼명 | 설명 |
|--------|------|
| partner_name | 콜라보 파트너명 |
| category | 분류 (IP/Brand/Artist/Game/Anime/Movie/Other) |
| region | 대상 지역 |
| post_count | 게시물 수 |
| total_reposts | 총 转发 수 |
| total_comments | 총 댓글 수 |
| total_attitudes | 총 좋아요 수 |
| total_comment_likes | 총 댓글 좋아요 수 |
| avg_reposts | 평균 转发 |
| avg_comments | 평균 댓글 |
| avg_attitudes | 평균 좋아요 |
| engagement_rate | 인게이지먼트 수치 |
| top_posts | 좋아요 TOP 3 게시물 |

### Full Export 시 추가 파일

* `all_posts.csv` — 전체 게시물 목록
* `collab_posts_detail.csv` — 콜라보 게시물 상세
* `video_posts.csv` — 비디오 포함 게시물만

## 프로젝트 구조

```
pubg_weibo_analyzer/
├── clients/
│   ├── weibo_client.py       # Weibo m.weibo.cn API 클라이언트
│   ├── gpt_client.py         # OpenAI GPT API
│   └── cloud_client.py       # 클라우드 스토리지 (placeholder)
├── pipeline/
│   ├── fetch.py              # 데이터 수집
│   ├── classify.py           # 콜라보 분류
│   ├── aggregate.py          # 지표 집계
│   └── export.py             # CSV 내보내기/업로드
├── db/
│   ├── schema.sql            # SQLite 스키마
│   └── models.py             # 데이터 모델
├── prompts/
│   └── collab_classifier.md  # GPT 분류 프롬프트
├── output/                   # 출력 디렉토리
├── data/                     # DB 저장 디렉토리
├── main.py                   # CLI 진입점
├── config.py                 # 설정 관리
├── requirements.txt
├── .env.example
└── README.md
```

## 콜라보 분류 로직

### 1단계: 룰 기반 분류

* 게시물 텍스트 + 해시태그에서 키워드 탐지:
  * 联动, 合作, 联名, 跨界, 携手, ×, ✕ 등
* 알려진 파트너 매핑 (兰博基尼, 龙珠, BLACKPINK 등)
* `#和平精英×파트너#` 해시태그 패턴 매칭

### 2단계: GPT 분류 (애매한 케이스)

* 룰 기반으로 confidence가 낮은 경우 GPT API 호출
* 결과 캐싱으로 중복 호출 방지
* `gpt-4o-mini` 모델 권장 (비용 효율)

### 분류 카테고리

| 카테고리 | 설명 | 예시 |
|----------|------|------|
| IP | 지식재산권/캐릭터 | Hello Kitty, 小黄人 |
| Brand | 상업 브랜드 | 兰博基尼, 保时捷 |
| Artist | 아티스트/뮤지션 | 周杰伦, 田曦薇 |
| Game | 게임 크로스오버 | 生化危机, 仙剑奇侠传 |
| Anime | 애니메이션 | 龙珠, 喜羊羊与灰太狼 |
| Movie | 영화/TV | 哥斯拉, 哪吒 |
| Other | 기타 | - |

## 레이트 리밋 & 안정성

### 웨이보 API 특성
* 공식 API가 아닌 모바일 사이트 JSON API 사용
* 빈번한 요청 시 쿠키 무효화 또는 验证码 요구 가능
* 서버 불안정으로 페이지네이션 데이터 누락 가능

### 대응 전략
* 요청 간 2~5초 딜레이 (jitter 포함)
* 50건마다 추가 대기 (5~15초)
* 지수 백오프 (최대 5회 재시도)
* `since_id` 기반 커서 페이지네이션 우선 사용

## 문제 해결

### 418 에러 (rate limited)
```
[Rate] Got 418 (I'm a teapot) - likely rate limited
```
* `WEIBO_REQUEST_DELAY`를 5.0 이상으로 늘리기
* 쿠키를 새로 갱신하기
* 60초 후 자동 재시도됨

### 로그인 페이지로 리다이렉트
```
[Auth] Redirected to login page
```
* `WEIBO_COOKIE` 갱신 필요
* 브라우저에서 m.weibo.cn 재로그인 후 쿠키 복사

### 데이터 누락
* `since_id` 기반 페이지네이션이 page 번호보다 안정적
* 여러 번 실행하면 증분으로 누락분 보충 가능

## 라이선스

내부 사용 목적 프로젝트입니다.

## 변경 이력

* v1.0.0 (2026-03): 초기 버전 (YouTube 파이프라인 기반 Weibo 포팅)
