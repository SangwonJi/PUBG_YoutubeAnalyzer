# PUBGM Social Media Collab Analysis

소셜 미디어 채널에서 콜라보(Collaboration) 콘텐츠를 수집, 분류, 집계하는 데이터 파이프라인입니다.

## 분석 대상

| 소스 | 플랫폼 | 채널/계정 | 파이프라인 |
|------|--------|-----------|------------|
| PUBG MOBILE | YouTube | [@PUBGMOBILE](https://www.youtube.com/@PUBGMOBILE) | `youtube_pipeline/` |
| Free Fire | YouTube | [@GarenaFreeFireGlobal](https://www.youtube.com/@GarenaFreeFireGlobal) | `youtube_pipeline/` (`--channel freefire`) |
| 和平精英 (Game for Peace) | Weibo | [和平精英](https://weibo.com/u/7095404909) | `weibo_pipeline/` |

## 주요 기능

- **데이터 수집**: YouTube Data API v3 / Weibo API를 통해 영상/게시물 메타데이터 및 댓글 수집
- **콜라보 분류**: 룰 기반 + GPT API를 활용한 콜라보 콘텐츠 자동 분류
- **지표 집계**: 파트너별 조회수/좋아요/댓글 등 핵심 지표 집계
- **리포트 생성**: CSV 형식의 상세 리포트 생성
- **클라우드 업로드**: 결과물 클라우드 스토리지 업로드 지원

## 프로젝트 구조

```
PUBGM_Social_Analyzer/
├── youtube_pipeline/          # YouTube 파이프라인 (PUBG MOBILE + Free Fire)
│   ├── clients/               # YouTube API 클라이언트
│   ├── pipeline/              # 수집/분류/집계/내보내기
│   ├── db/                    # SQLite 스키마 및 모델
│   ├── prompts/               # GPT 분류 프롬프트
│   ├── scripts/               # 유틸리티 스크립트
│   ├── main.py                # CLI 진입점
│   ├── config.py              # 설정 관리
│   ├── requirements.txt       # 의존성
│   └── .env.example           # 환경변수 예시
├── weibo_pipeline/            # Weibo 파이프라인 (和平精英)
│   ├── clients/               # Weibo API 클라이언트
│   ├── pipeline/              # 수집/분류/집계/내보내기
│   ├── db/                    # SQLite 스키마 및 모델
│   ├── prompts/               # GPT 분류 프롬프트
│   ├── main.py                # CLI 진입점
│   ├── config.py              # 설정 관리
│   ├── requirements.txt       # 의존성
│   └── .env.example           # 환경변수 예시
├── docs/                      # GitHub Pages 대시보드
├── .github/workflows/         # CI/CD (GitHub Pages 배포)
└── README.md
```

## 설치 및 사용법

### YouTube 파이프라인

```bash
cd youtube_pipeline
pip install -r requirements.txt
cp .env.example .env   # YOUTUBE_API_KEY, GPT_API_KEY 설정
```

```bash
# PUBG MOBILE 전체 파이프라인 실행
python main.py run --days 365

# Free Fire 전체 파이프라인 실행
python main.py run --days 365 --channel freefire

# 전체 채널 (PUBG MOBILE + Free Fire)
python main.py run --days 365 --channel all

# 개별 단계 실행
python main.py fetch --days 365          # 데이터 수집
python main.py classify                  # 콜라보 분류
python main.py aggregate --days 365      # 지표 집계
python main.py export --full --out ./output  # CSV 내보내기
python main.py status                    # 상태 확인
```

### Weibo 파이프라인

```bash
cd weibo_pipeline
pip install -r requirements.txt
cp .env.example .env   # GPT_API_KEY 설정 (WEIBO_COOKIE 선택)
```

```bash
# 전체 파이프라인 실행
python main.py run --days 365

# 개별 단계 실행
python main.py fetch --days 365          # 데이터 수집
python main.py classify                  # 콜라보 분류
python main.py aggregate --days 365      # 지표 집계
python main.py export --full --out ./output  # CSV 내보내기
python main.py status                    # 상태 확인
```

## 콜라보 분류 카테고리

| 카테고리 | 설명 | 예시 |
|----------|------|------|
| IP | 지식재산권/프랜차이즈 | 캐릭터, 유니버스 |
| Brand | 상업 브랜드 | Lamborghini, McLaren |
| Artist | 아티스트/뮤지션 | BLACKPINK, Alan Walker, 华晨宇 |
| Game | 게임 크로스오버 | Resident Evil, Metro Exodus |
| Anime | 애니메이션/만화 | Dragon Ball, Jujutsu Kaisen |
| Movie | 영화/TV | Godzilla, The Boys |
| Other | 기타 | - |

## 출력 리포트

### YouTube (`collab_report.csv`)

| 컬럼 | 설명 |
|------|------|
| partner_name | 콜라보 파트너명 |
| category | 분류 카테고리 |
| region | 대상 지역 |
| video_count | 영상 수 |
| total_views | 총 조회수 |
| total_video_likes | 총 좋아요 수 |
| total_comments | 총 댓글 수 |
| avg_views | 평균 조회수 |

### Weibo (`collab_report.csv`)

| 컬럼 | 설명 |
|------|------|
| partner_name | 콜라보 파트너명 |
| category | 분류 카테고리 |
| post_count | 게시물 수 |
| total_reposts | 총 转发 수 |
| total_comments | 총 评论 수 |
| total_attitudes | 총 点赞 수 |
| engagement_rate | 인게이지먼트율 |

## API 쿼터

### YouTube Data API
- 일일 쿼터: 기본 10,000 units
- 50개씩 배치 호출로 최적화

### Weibo API
- 비공식 API (m.weibo.cn)
- 요청 간격 3초 기본 설정

## 라이선스

내부 사용 목적 프로젝트입니다.
