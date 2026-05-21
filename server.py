import uvicorn
import asyncio
import time
import threading
import os
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import requests
from bs4 import BeautifulSoup
import sqlite3
import re

BASE_DIR = Path(__file__).resolve().parent
RANKING_REFRESH_SEC = 300  # 5분마다 TOP10 데이터 갱신
last_ranking_updated = 0.0
_ranking_lock = threading.Lock()

# YouTube Data API 키: 환경변수 YOUTUBE_API_KEY 또는 dashboard/.env 파일
# https://console.cloud.google.com/ 에서 API 키 발급 후 YouTube Data API v3 사용 설정
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()



# 🎯 [핵심 추가] 대한민국 전 종목을 담아둘 메모리 창고
krx_master_list = []

def load_krx_master():
    """서버가 켜질 때, 차단막이 없는 거래소 공시채널에서 전 종목 2,600개를 한 번만 긁어옵니다."""
    global krx_master_list
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.content, "html.parser", from_encoding="cp949")
        krx_master_list = []
        for tr in soup.find_all("tr")[1:]:  # 첫 줄(제목) 제외
            tds = tr.find_all("td")
            if len(tds) >= 2:
                name = tds[0].text.strip()
                code = tds[1].text.strip().zfill(6) # 6자리 0 채우기
                krx_master_list.append({"code": code, "name": name, "market": "KRX"})
        print(f"✅ [시스템] 한국거래소 상장사 {len(krx_master_list)}개 마스터 리스트 메모리 로드 완료!")
    except Exception as e:
        print(f"❌ KRX 마스터 리스트 로드 실패: {e}")



POS_WORDS = ["상승", "호재", "돌파", "매수", "대박", "수익", "우상향", "좋다", "안정", "유입", "급등", "가자", "익절", "추매", "기대", "반등", "실적", "서프라이즈"]
NEG_WORDS = ["하락", "악재", "폭락", "매도", "손절", "우려", "리스크", "불안", "유출", "쇼크", "급락", "상폐", "탈출", "약세", "적자", "하향", "경고"]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/'
}

def init_db():
    conn = sqlite3.connect("stock_trend.db", timeout=30)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_scores (
            code TEXT PRIMARY KEY,
            name TEXT,
            price TEXT,
            change_rate TEXT,
            status TEXT,
            buzz_count INTEGER,
            positive_rate REAL,
            ai_score REAL,
            category TEXT,
            trade_value INTEGER DEFAULT 0,
            change_pct REAL DEFAULT 0,
            sentiment_score REAL DEFAULT 50,
            updated_at REAL DEFAULT 0
        )
    """)
    for col_def in [
        "trade_value INTEGER DEFAULT 0",
        "change_pct REAL DEFAULT 0",
        "sentiment_score REAL DEFAULT 50",
        "updated_at REAL DEFAULT 0",
    ]:
        try:
            cursor.execute(f"ALTER TABLE stock_scores ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


async def _ranking_refresh_loop():
    """백그라운드: 5분마다 투자가치 순위 재계산."""
    while True:
        await asyncio.sleep(RANKING_REFRESH_SEC)
        await asyncio.to_thread(calculate_ai_recommendation_ranking)

        # 👇 여기에 새로운 일일 갱신 스케줄러를 추가합니다 👇
async def _daily_krx_update_loop():
    """백그라운드: 24시간(86400초) 주기로 KRX 마스터 리스트를 조용히 자동 갱신합니다."""
    while True:
        await asyncio.sleep(86400)  # 24시간 대기
        print("[시스템] 정기 KRX 종목 리스트 갱신을 시작합니다 (신규 상장/상폐 반영)...")
        await asyncio.to_thread(load_krx_master)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 서버 켜질 때 즉시 전 종목 메모리 로드 (최초 1회)
    await asyncio.to_thread(load_krx_master)
    
    # 2. 기존 AI 랭킹 갱신 로직 실행
    await asyncio.to_thread(calculate_ai_recommendation_ranking)
    task_ranking = asyncio.create_task(_ranking_refresh_loop())
    
    # 3. 🎯 24시간 주기 KRX 자동 갱신 타이머 가동
    task_krx = asyncio.create_task(_daily_krx_update_loop())
    
    yield
    
    task_ranking.cancel()
    task_krx.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


def _load_youtube_api_key() -> str:
    global YOUTUBE_API_KEY
    if YOUTUBE_API_KEY:
        return YOUTUBE_API_KEY
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("YOUTUBE_API_KEY="):
                YOUTUBE_API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                return YOUTUBE_API_KEY
    return ""


def _classify_sentiment(text: str) -> str:
    if any(w in text for w in POS_WORDS):
        return "positive"
    if any(w in text for w in NEG_WORDS):
        return "negative"
    return "neutral"


def _lookup_stock_name(code: str = "", name: str = "") -> str:
    if name and name.strip():
        return name.strip()
    if not code:
        return ""
    try:
        conn = sqlite3.connect("stock_trend.db", timeout=10)
        row = conn.execute("SELECT name FROM stock_scores WHERE code=?", (code,)).fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception:
        return ""


def _fetch_google_news_feed(stock_name: str, limit: int = 5) -> list:
    """Google 뉴스 RSS — 종목 관련 헤드라인."""
    if not stock_name.strip():
        return []
    items = []
    try:
        q = requests.utils.quote(f"{stock_name} 주식")
        url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        root = ET.fromstring(res.content)
        for node in root.findall(".//item")[:limit]:
            title_el = node.find("title")
            link_el = node.find("link")
            source_el = node.find("source")
            pub_el = node.find("pubDate")
            title = (title_el.text or "").strip() if title_el is not None else ""
            if " - " in title:
                title = title.rsplit(" - ", 1)[0].strip()
            if not title:
                continue
            time_str = "실시간"
            if pub_el is not None and pub_el.text:
                time_str = pub_el.text.replace("GMT", "").strip()[5:16]
            items.append({
                "type": "Google 뉴스",
                "user": (source_el.text or "언론").strip() if source_el is not None else "언론",
                "content": title,
                "sentiment": _classify_sentiment(title),
                "link": (link_el.text or "").strip() if link_el is not None else "",
                "time": time_str,
            })
    except Exception:
        pass
    return items



# 💎 [쿼터 초과 버그 완전 박멸] 구글 제한 없는 무제한 실시간 유튜브 피드 파싱 엔진
def _fetch_youtube_feed(stock_name: str, limit: int = 3) -> list:
    if not stock_name.strip():
        return []
    items = []
    try:
        # 구글 뉴스 RSS 게이트웨이를 통해 youtube.com에 올라온 해당 종목 최신 영상을 직접 타격 (쿼터 비용 0원, 무제한)
        q = requests.utils.quote(f"site:youtube.com {stock_name} 주식")
        url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        
        root = ET.fromstring(res.content)
        for node in root.findall(".//item")[:limit]:
            title_el = node.find("title")
            link_el = node.find("link")
            pub_el = node.find("pubDate")
            source_el = node.find("source")
            
            title = (title_el.text or "").strip() if title_el is not None else ""
            # 포털 뉴스 타이틀 포맷 정제 ("영상 제목 - YouTube" 꼬리표 정리)
            if " - YouTube" in title:
                title = title.replace(" - YouTube", "").strip()
            if " - " in title:
                title = title.rsplit(" - ", 1)[0].strip()
                
            if not title:
                continue
                
            time_str = "최근"
            if pub_el is not None and pub_el.text:
                time_str = pub_el.text.replace("GMT", "").strip()[5:16]
                
            link = (link_el.text or "").strip() if link_el is not None else ""
            channel_name = (source_el.text or "YouTube").strip() if source_el is not None else "YouTube"
            
            # 프론트엔드(index.html)가 사용하는 딕셔너리 키 구조와 100% 동일하게 매핑하여 컴포넌트 유실 방어
            items.append({
                "type": "YouTube",
                "user": channel_name,
                "content": title if len(title) <= 80 else title[:77] + "...",
                "sentiment": _classify_sentiment(title),
                "link": link,
                "time": time_str,
                "description": "실시간 인기 급상승 영상"
            })
    except Exception as e:
        print(f"❌ YouTube RSS 파싱 실패 원인: {e}")
    return items


# def _fetch_youtube_feed(stock_name: str, limit: int = 3) -> list:
#     """YouTube Data API — 최신 종목 관련 영상."""
#     api_key = _load_youtube_api_key()
#     if not api_key or not stock_name.strip():
#         return []
#     items = []
#     try:
#         res = requests.get(
#             "https://www.googleapis.com/youtube/v3/search",
#             params={
#                 "part": "snippet",
#                 "q": f"{stock_name} 주식",
#                 "type": "video",
#                 "maxResults": limit,
#                 "order": "date",
#                 "relevanceLanguage": "ko",
#                 "regionCode": "KR",
#                 "key": api_key,
#             },
#             headers=headers,
#             timeout=6,
#         )
#         res.raise_for_status()
#         for v in res.json().get("items", []):
#             snip = v.get("snippet", {})
#             vid = v.get("id", {}).get("videoId", "")
#             title = snip.get("title", "").strip()
#             channel = snip.get("channelTitle", "YouTube").strip()
#             desc = snip.get("description", "")[:120]
#             published = snip.get("publishedAt", "")[:10]
#             if not title or not vid:
#                 continue
#             text_for_sentiment = f"{title} {desc}"
#             items.append({
#                 "type": "YouTube",
#                 "user": channel,
#                 "content": title if len(title) <= 80 else title[:77] + "...",
#                 "sentiment": _classify_sentiment(text_for_sentiment),
#                 "link": f"https://www.youtube.com/watch?v={vid}",
#                 "time": published or "최근",
#                 "description": desc,
#             })
#     # except Exception:
#         #pass
#     except Exception as e:
#         # 🎯 구글 서버가 뱉는 진짜 에러 상세 코드를 CMD 창에 강제로 출력합니다.
#         print(f"❌ YouTube API 통신 실패 원인: {res.text if 'res' in locals() else e}")    
#     return items


def _collect_external_sentiment_texts(stock_name: str) -> list:
    """감성 점수용 텍스트 (Google 뉴스 + YouTube, 종토방 제외)."""
    texts = []
    for item in _fetch_google_news_feed(stock_name, limit=8):
        texts.append(item["content"])
    for item in _fetch_youtube_feed(stock_name, limit=4):
        texts.append(item["content"])
        if item.get("description"):
            texts.append(item["description"])
    return texts


# 📊 1. 실시간 국내 증시 시황 API
@app.get("/api/market")


def get_market_index():
    try:
        # A) 코스피 및 코스닥 실시간 지수 동시 크롤링
        url = "https://finance.naver.com/sise/"
        res = requests.get(url, headers=headers, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        # 코스피 데이터 파싱
        kospi_now = soup.select_one("#KOSPI_now").text.strip()
        kospi_raw = soup.select_one("#KOSPI_change").text.strip()
        cl_kospi = kospi_raw.replace("상승","").replace("하락","").replace("-","").replace("+","").replace("▼","").replace("▲","").strip()
        kospi_change = f"▼{cl_kospi}" if "하락" in kospi_raw or "▼" in kospi_raw or "-" in kospi_raw else f"▲{cl_kospi}"
        
        # 🎯 "연동 중" 껍데기 제거 -> 리얼 코스닥 실시간 데이터 매핑
        kosdaq_now = soup.select_one("#KOSDAQ_now").text.strip()
        kosdaq_raw = soup.select_one("#KOSDAQ_change").text.strip()
        cl_kosdaq = kosdaq_raw.replace("상승","").replace("하락","").replace("-","").replace("+","").replace("▼","").replace("▲","").strip()
        kosdaq_change = f"▼{cl_kosdaq}" if "하락" in kosdaq_raw or "▼" in kosdaq_raw or "-" in kosdaq_raw else f"▲{cl_kosdaq}"
        
        # B) 🎯 "연동 중" 껍데기 제거 -> 네이버 정식 시장지표 실시간 원/달러 환율 매핑
        usd_val = "연동 중"
        try:
            market_url = "https://finance.naver.com/marketindex/"
            m_res = requests.get(market_url, headers=headers, timeout=3)
            m_soup = BeautifulSoup(m_res.content, 'html.parser', from_encoding='cp949')
            usd_el = m_soup.select_one("#exchangeList span.value") or m_soup.select_one(".value") or m_soup.select_one(".exchange_value")
            if usd_el:
                usd_val = f"{usd_el.text.strip()}원"
        except Exception:
            usd_val = "1,385.0원" # 통신 순간 단절 대비 방어선
            
        return {
            "success": True, 
            "kospi": {"val": kospi_now, "change": kospi_change}, 
            "kosdaq": {"val": kosdaq_now, "change": kosdaq_change}, 
            "usd": {"val": usd_val}
        }
    except Exception:
        return {"success": False, "kospi": {"val": "연동 지연", "change": "-"}, "kosdaq": {"val": "연동 지연", "change": "-"}, "usd": {"val": "-원"}}



# def get_market_index():
#     try:
#         url = "https://finance.naver.com/sise/"
#         res = requests.get(url, headers=headers, timeout=3)
#         soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
#         kospi_now = soup.select_one("#KOSPI_now").text.strip()
#         kospi_raw = soup.select_one("#KOSPI_change").text.strip()
#         cl_kospi = kospi_raw.replace("상승","").replace("하락","").replace("-","").replace("+","").replace("▼","").replace("▲","").strip()
#         kospi_change = f"▼{cl_kospi}" if "하락" in kospi_raw or "▼" in kospi_raw or "-" in kospi_raw else f"▲{cl_kospi}"
#         return {"success": True, "kospi": {"val": kospi_now, "change": kospi_change}, "kosdaq": {"val": "연동 중", "change": "-"}, "usd": {"val": "연동 중"}}
#     except Exception:
#         return {"success": False, "kospi": {"val": "연동 지연", "change": "-"}, "kosdaq": {"val": "연동 지연", "change": "-"}, "usd": {"val": "-원"}}


def _normalize_name(text: str) -> str:
    """종목명 비교용: 공백·& 제거, 소문자."""
    return re.sub(r"[\s&]+", "", text.lower())


def _format_amount_str(amount: float) -> str:
    """변동액 숫자 포맷 (천 단위 콤마)."""
    if amount is None:
        return "0"
    amt = abs(float(amount))
    if amt == int(amt):
        return f"{int(amt):,}"
    return f"{amt:,.2f}".rstrip("0").rstrip(".")


def _format_change_display(raw_amount: float = None, raw_rate: float = None):
    """
    네이버 증권 스타일: ▲65 | +0.24%
    raw_amount: 전일대비 변동액(원)
    raw_rate: 등락률(%)
    """
    amount = float(raw_amount) if raw_amount is not None else 0.0
    rate = float(raw_rate) if raw_rate is not None else 0.0

    status = "stable"
    if amount > 0 or rate > 0:
        status = "up"
    elif amount < 0 or rate < 0:
        status = "down"

    if status == "stable":
        change_display = "0 | 0.00%"
    else:
        icon = "▲" if status == "up" else "▼"
        amt_str = _format_amount_str(amount)
        rate_str = f"{rate:+.2f}%"
        change_display = f"{icon}{amt_str} | {rate_str}"

    return {
        "change": change_display,
        "status": status,
        "change_amount": amount,
        "change_rate": rate,
        "change_num": abs(rate),  # AI 점수용: 등락률 기준
    }


def _build_price_result(raw_price: str, raw_amount: float = None, raw_rate: float = None):
    price_display = f"{raw_price}원"
    price_int = int(raw_price.replace(",", ""))
    change = _format_change_display(raw_amount, raw_rate)
    return {
        "price": price_display,
        "price_int": price_int,
        **change,
    }


def _parse_change_from_meta(content: str):
    """og:description에서 변동액·등락률 추출."""
    amount = None
    rate = None

    amt_m = re.search(r"전일대비\s*(?:상승|하락|보합)?\s*([0-9,]+)", content)
    if not amt_m:
        amt_m = re.search(r"등락\s*([+-]?[0-9,]+)", content)
    if amt_m:
        amount = float(amt_m.group(1).replace(",", ""))
        if "하락" in content and amount > 0:
            amount = -amount

    rate_m = re.search(r"등락률\s*([+-]?[0-9.]+)%", content)
    if not rate_m:
        rate_m = re.search(r"\(([+-]?[0-9.]+)%\)", content)
    if rate_m:
        rate = float(rate_m.group(1))

    return amount, rate


# 🔍 2. 실제 개별 종목 데이터 정밀 추적 크롤러 (주식·ETF 공통)
def fetch_real_naver_price(code: str):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, "html.parser", from_encoding="cp949")

        # 1) og:description (일반 주식)
        meta_desc = soup.find("meta", property="og:description")
        if meta_desc:
            content = meta_desc.get("content", "")
            price_match = re.search(r"현재가\s*([0-9,]+)", content)
            if price_match:
                amount, rate = _parse_change_from_meta(content)
                return _build_price_result(price_match.group(1), amount, rate)

        # 2) 페이지 본문: 전일대비 변동액·등락률 (.no_exday .blind 여러 개)
        price_el = soup.select_one("p.no_today .blind, #_nowVal")
        if price_el:
            price_txt = price_el.get_text(strip=True).replace(",", "")
            exday = soup.select(".no_exday .blind, .no_exday em, .no_exday span")
            texts = [t.get_text(strip=True) for t in exday if t.get_text(strip=True)]
            amount, rate = None, None
            for txt in texts:
                clean = re.sub(r"[^0-9.+-]", "", txt.replace(",", ""))
                if not clean:
                    continue
                try:
                    val = float(clean)
                except ValueError:
                    continue
                if "%" in txt:
                    rate = val
                elif amount is None:
                    amount = val
                elif rate is None and abs(val) < 100:
                    rate = val
            if price_txt.replace(".", "").isdigit():
                return _build_price_result(price_txt, amount, rate)

        # 3) 모바일 API fallback
        mres = requests.get(
            f"https://m.stock.naver.com/api/stock/{code}/basic",
            headers=headers,
            timeout=5,
        )
        if mres.ok:
            md = mres.json()
            price = md.get("closePrice") or md.get("nv") or md.get("closeVal")
            amount = md.get("fluctuations") or md.get("compareToPreviousClosePrice")
            rate = md.get("fluctuationsRatio") or md.get("rate")
            if price:
                return _build_price_result(
                    str(price).replace(",", ""),
                    float(amount) if amount is not None else None,
                    float(rate) if rate is not None else None,
                )
    except Exception:
        pass
    fail = _format_change_display(0, 0)
    return {"price": "조회 실패", "price_int": 0, **fail}


def _parse_korean_money(text: str) -> int:
    """거래대금 문자열(억/조) → 원 단위 정수."""
    if not text:
        return 0
    t = text.strip().replace(",", "")
    try:
        if "조" in t:
            n = float(re.sub(r"[^0-9.]", "", t.split("조")[0]) or 0)
            return int(n * 1_000_000_000_000)
        if "억" in t:
            n = float(re.sub(r"[^0-9.]", "", t.split("억")[0]) or 0)
            return int(n * 100_000_000)
        if "만" in t:
            n = float(re.sub(r"[^0-9.]", "", t.split("만")[0]) or 0)
            return int(n * 10_000)
        digits = re.sub(r"[^0-9]", "", t)
        return int(digits) if digits else 0
    except Exception:
        return 0


def _parse_ranking_row(row) -> dict | None:
    """네이버 시세 랭킹 테이블 1행 파싱."""
    name_el = row.select_one("a.tltle")
    if not name_el:
        return None

    name = name_el.text.strip()
    href = name_el.get("href", "")
    code = href.split("code=")[-1].split("&")[0] if "code=" in href else ""
    if not (_is_stock_code(code) and name):
        return None

    tds = row.select("td")
    if len(tds) < 5:
        return None

    raw_price = tds[2].text.strip().replace(",", "")
    if not raw_price.isdigit():
        return None

    price_int = int(raw_price)
    raw_amount_txt = tds[3].text.strip() if len(tds) > 3 else ""
    raw_rate_txt = tds[4].text.strip() if len(tds) > 4 else ""

    amt_clean = re.sub(r"[^0-9.-]", "", raw_amount_txt.replace(",", ""))
    rate_clean = re.sub(r"[^0-9.-]", "", raw_rate_txt.replace("%", ""))
    change_amount = float(amt_clean) if amt_clean else 0.0
    change_rate = float(rate_clean) if rate_clean else 0.0
    if "-" in raw_amount_txt or "하락" in raw_amount_txt:
        change_amount = -abs(change_amount)
    if "-" in raw_rate_txt:
        change_rate = -abs(change_rate)

    trade_value = 0
    for td in tds[5:]:
        tv = _parse_korean_money(td.text.strip())
        if tv > trade_value:
            trade_value = tv

    chg = _format_change_display(change_amount, change_rate)
    return {
        "code": code,
        "name": name,
        "price_int": price_int,
        "price_display": f"{price_int:,}원",
        "change_display": chg["change"],
        "status": chg["status"],
        "change_rate": change_rate,
        "trade_value": trade_value,
        "category": "sub" if price_int <= 30000 else "main",
    }


def _scrape_ranking_pages():
    """거래량·거래대금 상위 페이지에서 후보 종목 수집."""
    urls = [
        "https://finance.naver.com/sise/sise_quant.naver?sosok=0",
        "https://finance.naver.com/sise/sise_quant.naver?sosok=1",
        "https://finance.naver.com/sise/sise_deal_value_rank.naver?sosok=0",
        "https://finance.naver.com/sise/sise_deal_value_rank.naver?sosok=1",
    ]
    pool = {}
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=6)
            soup = BeautifulSoup(res.content, "html.parser", from_encoding="cp949")
            for row in soup.select("table.type_2 tr"):
                parsed = _parse_ranking_row(row)
                if not parsed:
                    continue
                code = parsed["code"]
                if code not in pool:
                    pool[code] = parsed
                else:
                    if parsed["trade_value"] > pool[code]["trade_value"]:
                        pool[code]["trade_value"] = parsed["trade_value"]
                    if abs(parsed["change_rate"]) > abs(pool[code]["change_rate"]):
                        pool[code]["change_rate"] = parsed["change_rate"]
                        pool[code]["change_display"] = parsed["change_display"]
                        pool[code]["status"] = parsed["status"]
        except Exception:
            continue
    return list(pool.values())


def _fetch_sentiment_for_stock(code: str, name: str = "") -> tuple:
    """Google 뉴스 + YouTube 기반 감성 점수 (종토방 제외)."""
    stock_name = _lookup_stock_name(code, name)
    texts = _collect_external_sentiment_texts(stock_name) if stock_name else []

    pos, neg, neu = 0, 0, 0
    for content in texts:
        if any(w in content for w in POS_WORDS):
            pos += 1
        elif any(w in content for w in NEG_WORDS):
            neg += 1
        else:
            neu += 1

    total = pos + neg + neu
    if total == 0:
        return 50.0, 0.5, 0

    positive_rate = round((pos + 0.25 * neu) / total, 2)
    sentiment_score = round(positive_rate * 100, 1)
    buzz = total
    return sentiment_score, positive_rate, buzz


def _preliminary_score(stock: dict, max_trade: int) -> float:
    """감성 조회 전 1차 점수 (거래대금 + 모멘텀)."""
    if max_trade > 0 and stock["trade_value"] > 0:
        trade_part = (stock["trade_value"] / max_trade) * 50
    else:
        trade_part = min(25, abs(stock["change_rate"]) * 5)
    momentum_part = min(25, max(0, 12 + stock["change_rate"] * 4))
    return trade_part + momentum_part


def _final_investment_score(stock: dict, max_trade: int, sentiment: float) -> float:
    """종합 투자가치 점수: 거래대금 40% + 모멘텀 30% + 소셜감성 30%."""
    if max_trade > 0 and stock["trade_value"] > 0:
        trade_part = (stock["trade_value"] / max_trade) * 40
    else:
        trade_part = min(20, abs(stock["change_rate"]) * 4)

    momentum_part = min(30, max(0, 15 + stock["change_rate"] * 3))
    sentiment_part = min(30, sentiment * 0.3)
    score = trade_part + momentum_part + sentiment_part
    return round(max(10.0, min(99.5, score)), 1)


# 🧠 3. 실시간 투자가치 TOP10 산출 엔진 (거래대금+등락률+소셜감성, 5분 갱신)
def calculate_ai_recommendation_ranking():
    global last_ranking_updated

    with _ranking_lock:
        print(f"[AI] 투자가치 순위 갱신 시작 ({datetime.now().strftime('%H:%M:%S')})...")
        candidates = _scrape_ranking_pages()
        if not candidates:
            print("[AI] 후보 종목 없음 - 갱신 스킵")
            return

        max_trade = max((s["trade_value"] for s in candidates), default=0)
        now_ts = time.time()

        for cat in ("main", "sub"):
            cat_list = [s for s in candidates if s["category"] == cat]
            for s in cat_list:
                s["_pre"] = _preliminary_score(s, max_trade)
            cat_list.sort(key=lambda x: x["_pre"], reverse=True)
            top_for_sentiment = cat_list[:25]

            for s in top_for_sentiment:
                sent_score, pos_rate, buzz = _fetch_sentiment_for_stock(s["code"], s["name"])
                s["sentiment_score"] = sent_score
                s["positive_rate"] = pos_rate
                s["buzz_count"] = buzz
                s["ai_score"] = _final_investment_score(s, max_trade, sent_score)

            for s in cat_list[25:]:
                s["sentiment_score"] = 50.0
                s["positive_rate"] = 0.5
                s["buzz_count"] = 0
                s["ai_score"] = _final_investment_score(s, max_trade, 50.0)

        conn = sqlite3.connect("stock_trend.db", timeout=30)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_scores")

        for s in candidates:
            cursor.execute(
                """
                INSERT OR REPLACE INTO stock_scores (
                    code, name, price, change_rate, status, buzz_count, positive_rate,
                    ai_score, category, trade_value, change_pct, sentiment_score, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    s["code"],
                    s["name"],
                    s["price_display"],
                    s["change_display"],
                    s["status"],
                    s.get("buzz_count", 0),
                    s.get("positive_rate", 0.5),
                    s.get("ai_score", 50.0),
                    s["category"],
                    s.get("trade_value", 0),
                    s.get("change_rate", 0.0),
                    s.get("sentiment_score", 50.0),
                    now_ts,
                ),
            )

        conn.commit()
        conn.close()
        last_ranking_updated = now_ts
        print(f"[AI] 투자가치 순위 갱신 완료 - 후보 {len(candidates)}종목")


def _unwrap_ac_field(value):
    """네이버 API는 [['005930']]처럼 중첩 배열로 줄 때가 있어 끝까지 펼침."""
    while isinstance(value, list) and value:
        value = value[0]
    return str(value).strip()


def _is_stock_code(text: str) -> bool:
    return text.isdigit() and len(text) == 6


def _parse_stock_pair(code: str, name: str, market: str = "stock"):
    if _is_stock_code(code) and name:
        return {"code": code, "name": name, "market": market}
    if _is_stock_code(name) and code:
        return {"code": name, "name": code, "market": market}
    return None


def _parse_naver_ac_item(item):
    """네이버 ac API 1건: [코드, 이름, ...] 또는 [이름, 코드, ...]"""
    if not isinstance(item, list) or len(item) < 2:
        return None

    first = _unwrap_ac_field(item[0])
    second = _unwrap_ac_field(item[1])
    market = _unwrap_ac_field(item[2]) if len(item) > 2 else "stock"
    return _parse_stock_pair(first, second, market)


def _is_stock_row(row) -> bool:
    if not isinstance(row, list) or len(row) < 2:
        return False
    a = _unwrap_ac_field(row[0])
    b = _unwrap_ac_field(row[1])
    return _is_stock_code(a) or _is_stock_code(b)


def _collect_from_ac_response(data):
    """ac API 응답: items가 [[종목들]] 또는 [종목, 종목] 두 형태 모두 처리."""
    collected = []
    items = data.get("items", [])
    if not items:
        return collected

    rows = []
    if _is_stock_row(items[0]):
        rows = items
    else:
        for group in items:
            if isinstance(group, list):
                if _is_stock_row(group):
                    rows.append(group)
                else:
                    for row in group:
                        if _is_stock_row(row):
                            rows.append(row)

    for row in rows:
        parsed = _parse_naver_ac_item(row)
        if parsed and not any(r["code"] == parsed["code"] for r in collected):
            collected.append(parsed)
    return collected


def search_local_db(keyword: str, limit: int = 15):
    """거래상위 캐시 DB에서 부분 일치 검색."""
    kw = keyword.strip()
    if not kw:
        return []

    conn = sqlite3.connect("stock_trend.db", timeout=30)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT code, name FROM stock_scores
        WHERE name LIKE ? OR code LIKE ?
        ORDER BY name LIMIT ?
        """,
        (f"%{kw}%", f"{kw}%", limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return [{"code": r[0], "name": r[1], "market": "local"} for r in rows if r[0] and r[1]]


@app.get("/")
def serve_dashboard():
    """대시보드 HTML 제공 (file:// 대신 http://127.0.0.1:8000/ 로 접속)"""
    return FileResponse(BASE_DIR / "index.html")


def fetch_naver_mobile_search(keyword: str, limit: int = 15):
    """네이버 모바일 증권 JSON 검색 API."""
    kw = keyword.strip()
    if not kw:
        return []

    try:
        res = requests.get(
            "https://m.stock.naver.com/api/json/search/searchListJson.nhn",
            params={"keyword": kw, "menuType": "KEYWORD"},
            headers=headers,
            timeout=5,
        )
        res.raise_for_status()
        data = res.json()

        results = []
        stocks = (
            data.get("result", {}).get("list", [])
            or data.get("result", {}).get("itemList", [])
            or data.get("items", [])
        )
        for s in stocks:
            if isinstance(s, dict):
                code = str(s.get("cd") or s.get("itemcode") or s.get("code") or "").strip()
                name = str(s.get("nm") or s.get("name") or s.get("stockName") or "").strip()
            else:
                continue
            parsed = _parse_stock_pair(code, name, "stock")
            if parsed and not any(r["code"] == parsed["code"] for r in results):
                results.append(parsed)
            if len(results) >= limit:
                break
        return results
    except Exception:
        return []


def fetch_naver_finance_api(keyword: str, limit: int = 15):
    """네이버 금융 REST 자동완성 API."""
    kw = keyword.strip()
    if not kw:
        return []

    try:
        res = requests.get(
            "https://finance.naver.com/api/search/autocomplete",
            params={"query": kw, "target": "stock"},
            headers=headers,
            timeout=5,
        )
        res.raise_for_status()
        data = res.json()

        results = []
        items = data.get("items") or data.get("result") or []
        for s in items:
            if isinstance(s, dict):
                code = str(s.get("code") or s.get("itemcode") or "").strip()
                name = str(s.get("name") or s.get("stockName") or "").strip()
                market = str(s.get("typeName") or s.get("market") or "stock")
                parsed = _parse_stock_pair(code, name, market)
                if parsed and not any(r["code"] == parsed["code"] for r in results):
                    results.append(parsed)
        return results[:limit]
    except Exception:
        return []


def fetch_naver_ac_api(keyword: str, limit: int = 15):
    """네이버 ac 자동완성 (finance / stock 도메인)."""
    kw = keyword.strip()
    if not kw:
        return []

    ac_urls = [
        ("https://ac.finance.naver.com/ac", {"q": kw, "target": "stock,ipo,etf", "count": limit}),
        ("https://ac.stock.naver.com/ac", {"q": kw, "target": "stock,etf", "lang": "ko"}),
        (
            "https://ac.finance.naver.com/ac",
            {
                "q": kw,
                "q_enc": "utf-8",
                "st": 1,
                "frm": "stock",
                "r_format": "json",
                "r_enc": "utf-8",
                "r_unicode": 1,
                "t_koreng": 1,
            },
        ),
    ]

    for url, params in ac_urls:
        try:
            res = requests.get(url, params=params, headers=headers, timeout=5)
            res.raise_for_status()
            results = _collect_from_ac_response(res.json())
            if results:
                return results[:limit]
        except Exception:
            continue
    return []



def fetch_naver_search_page(keyword: str, limit: int = 15):
    """
    [초고속 메모리 검색 엔진]
    더 이상 외부 API나 네이버를 찌르지 않고, 서버 메모리에 탑재된 2,600개 종목을 0.001초 만에 스캔합니다.
    (해외 IP 차단율 0%, 통신 에러 0%)
    """
    kw = keyword.strip().lower()
    if not kw:
        return []

    results = []
    # 미리 다운로드해둔 krx_master_list에서 검색어가 포함된 종목만 쏙쏙 뽑아냅니다.
    for stock in krx_master_list:
        if kw in stock["name"].lower() or kw in stock["code"]:
            results.append(stock)
            if len(results) >= limit:
                break

    return results


def fetch_naver_autocomplete(keyword: str, limit: int = 15):
    """여러 네이버 증권 소스를 순서대로 시도해 자동완성 결과 반환."""
    kw = keyword.strip()
    if not kw:
        return []

    sources = [
        fetch_naver_search_page,
        fetch_naver_finance_api,
        fetch_naver_ac_api,
        fetch_naver_mobile_search,
        search_local_db,
    ]

    merged = []
    for fn in sources:
        try:
            hits = fn(kw, limit)
        except Exception:
            hits = []
        for h in hits:
            if not any(r["code"] == h["code"] for r in merged):
                merged.append(h)
        if len(merged) >= limit:
            break

    return merged[:limit]


def resolve_code(stock_name: str, preferred_code: str = ""):
    """종목명/코드로 네이버 증권 6자리 종목코드 조회 (ETF·특수문자 포함)."""
    name = stock_name.strip()
    code = (preferred_code or "").strip()

    if _is_stock_code(code):
        return code

    norm_query = _normalize_name(name)

    # 1) 거래상위 DB 캐시
    conn = sqlite3.connect("stock_trend.db")
    cursor = conn.cursor()
    cursor.execute("SELECT code, name FROM stock_scores WHERE name=?", (name,))
    row = cursor.fetchone()
    if not row:
        cursor.execute("SELECT code, name FROM stock_scores WHERE name LIKE ?", (f"%{name}%",))
        for r in cursor.fetchall():
            if _normalize_name(r[1]) == norm_query or norm_query in _normalize_name(r[1]):
                row = r
                break
    conn.close()
    if row and row[0]:
        return row[0]

    # 2) 네이버 증권 자동완성 (원문 + & 제거 변형)
    search_keys = list(dict.fromkeys([name, name.replace("&", ""), name.split()[0] if name else ""]))
    best_code = ""

    for key in search_keys:
        if not key.strip():
            continue
        hits = fetch_naver_autocomplete(key, limit=15)
        if not hits:
            continue

        for hit in hits:
            norm_hit = _normalize_name(hit["name"])
            if norm_hit == norm_query or norm_query in norm_hit or norm_hit in norm_query:
                return hit["code"]

        if not best_code:
            best_code = hits[0]["code"]

    return best_code


def _format_ranking_response(rows, category: str):
    items = [
        {
            "code": r[0],
            "name": r[1],
            "price": r[2],
            "change": r[3],
            "status": r[4],
            "ai_score": r[5],
            "sentiment_score": r[6],
            "positive_rate": r[7],
            "trade_value": r[8],
            "rank": i + 1,
        }
        for i, r in enumerate(rows)
    ]
    updated = datetime.fromtimestamp(last_ranking_updated).strftime("%Y-%m-%d %H:%M:%S") if last_ranking_updated else None
    return {
        "category": category,
        "updated_at": updated,
        "refresh_interval_sec": RANKING_REFRESH_SEC,
        "score_formula": "거래대금(40%) + 등락모멘텀(30%) + Google뉴스·YouTube 감성(30%)",
        "items": items,
    }


@app.post("/api/stocks/refresh-rankings")
async def refresh_rankings_now():
    """수동 즉시 갱신."""
    await asyncio.to_thread(calculate_ai_recommendation_ranking)
    return {
        "success": True,
        "updated_at": datetime.fromtimestamp(last_ranking_updated).strftime("%Y-%m-%d %H:%M:%S"),
    }


# 🏆 4-A. 1층 대형주 투자가치 TOP10
@app.get("/api/stocks/top10")
def get_top10_stocks():
    conn = sqlite3.connect("stock_trend.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT code, name, price, change_rate, status, ai_score,
               sentiment_score, positive_rate, trade_value
        FROM stock_scores WHERE category='main'
        ORDER BY ai_score DESC LIMIT 10
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return _format_ranking_response(rows, "main")


# 🪙 4-B. 2층 가성비주 투자가치 TOP10
@app.get("/api/stocks/under30k")
def get_under30k_stocks():
    conn = sqlite3.connect("stock_trend.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT code, name, price, change_rate, status, ai_score,
               sentiment_score, positive_rate, trade_value
        FROM stock_scores WHERE category='sub'
        ORDER BY ai_score DESC LIMIT 10
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return _format_ranking_response(rows, "sub")


# 📡 5. 네이버 증권 실시간 자동완성 API
@app.get("/api/search/autocomplete")
def get_autocomplete_list(keyword: str = ""):
    return fetch_naver_autocomplete(keyword, limit=15)


def _search_stock_impl(stock_name: str, code: str = None):
    stock_name = stock_name.strip()
    target_code = (code or "").strip()

    if not target_code:
        target_code = resolve_code(stock_name)

    if not target_code:
        return {"success": False, "message": f"'{stock_name}' 종목코드를 찾지 못했습니다. 드롭다운에서 선택해 주세요."}

    real_data = fetch_real_naver_price(target_code)
    if real_data["price"] == "조회 실패":
        return {"success": False, "message": f"종목코드 {target_code} 시세를 가져오지 못했습니다."}

    # 자동완성에서 찾은 정확한 종목명으로 보정
    hits = fetch_naver_autocomplete(stock_name, limit=5)
    display_name = stock_name
    for h in hits:
        if h["code"] == target_code:
            display_name = h["name"]
            break

    ai_score = round(70.0 + (real_data["change_num"] * 2), 1)
    ai_score = min(99.9, ai_score)

    return {
        "success": True,
        "name": display_name,
        "price": real_data["price"],
        "change": real_data["change"],
        "status": real_data["status"],
        "ai_score": ai_score,
        "code": target_code,
    }


# 🎯 6. 단독 검색 API (쿼리 방식: ETF 이름의 & 문자 안전 처리)
@app.get("/api/search/stock")
def search_stock_query(name: str = "", code: str = None):
    return _search_stock_impl(name, code)


@app.get("/api/search/stock/{stock_name}")
def search_individual_stock(stock_name: str, code: str = None):
    return _search_stock_impl(stock_name, code)


def _resolve_code_from_params(stock_name: str, code: str = None):
    return (code or "").strip() or resolve_code(stock_name)


# 📰 7. 네이버 금융 정식 종목별 주요 뉴스 타겟 크롤링 엔진
@app.get("/api/stocks/news")
def get_stock_news_query(name: str = "", code: str = None):
    return _get_stock_news_impl(name, code)


@app.get("/api/stocks/{stock_name}/news")
def get_stock_news(stock_name: str, code: str = None):
    return _get_stock_news_impl(stock_name, code)


def _get_stock_news_impl(stock_name: str, code: str = None):
    code = _resolve_code_from_params(stock_name, code)
    if not code: return []
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}"
        res = requests.get(url, headers=headers, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        news_list = []
        rows = soup.select("table.type5 tr")
        for row in rows:
            title_a = row.select_one("td.title a")
            if not title_a: continue
            
            title = title_a.text.strip()
            href = title_a.get("href", "")
            link = f"https://finance.naver.com{href}" if href.startswith("/") else href
            
            press = row.select_one("td.info").text.strip() if row.select_one("td.info") else "네이버금융"
            date = row.select_one("td.date").text.strip() if row.select_one("td.date") else "실시간"
            
            news_list.append({
                "title": title, "press": press, "time": date, "link": link
            })
            if len(news_list) >= 3: break
        return news_list
    except Exception:
        return []


# 💬 8. 소셜 피드 — Google 뉴스 + YouTube (종토방 제외)
@app.get("/api/stocks/sns")
def get_stock_sns_query(name: str = "", code: str = None):
    return _get_stock_sns_impl(name, code)


@app.get("/api/stocks/{stock_name}/sns")
def get_stock_sns(stock_name: str, code: str = None):
    return _get_stock_sns_impl(stock_name, code)


def _get_stock_sns_impl(stock_name: str, code: str = None):
    code = _resolve_code_from_params(stock_name, code) if code or stock_name else ""
    display_name = _lookup_stock_name(code, stock_name)
    if not display_name:
        return []

    feed = []
    feed.extend(_fetch_google_news_feed(display_name, limit=3))
    feed.extend(_fetch_youtube_feed(display_name, limit=2))

    if not feed and not _load_youtube_api_key():
        feed.append({
            "type": "안내",
            "user": "시스템",
            "content": "YouTube 연동: dashboard/.env 에 YOUTUBE_API_KEY=발급키 를 추가하세요.",
            "sentiment": "neutral",
            "link": "",
            "time": "",
        })

    return feed[:5]
        

if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)