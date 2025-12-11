# -*- coding: utf-8 -*-
import json
import logging
import re
import os
import time
import uuid
import unicodedata
import threading
import requests
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


import tempfile, shutil

_thread_local = threading.local()

from reaction_map import REACTION_MAP

# ======================== LOGGING ========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NewsCrawlerVNE_All")

# ======================== OPTIONS ========================
USE_INGESTED_DATE_FALLBACK = False
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
HTTP_TIMEOUT = (5, 15)
HTTP_RETRIES = 3
PER_PAGE_SLEEP = 0.6

# Comments
ENABLE_COMMENTS = os.getenv("ENABLE_COMMENTS", "true").lower() == "true"
COMMENT_WORKERS = int(os.getenv("COMMENT_WORKERS", "2"))
COMMENT_LIMIT = int(os.getenv("COMMENT_LIMIT", "50"))
COMMENT_WAIT = int(os.getenv("COMMENT_WAIT", "6"))

# GCS


GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "my-crawl-bucket-bronze")
GCS_CREATE_IF_NOT_EXISTS = os.getenv("GCS_CREATE_IF_NOT_EXISTS", "true").lower() == "true"
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")

# --- START: explicit service-account credentials (paste here) ---
import os
from google.oauth2 import service_account
from google.cloud import storage

#Tren Server
# SA_PATH = r"C:\Users\Administrator\Documents\DA2\mythical-bazaar-475215-i7-337ee07f878c.json"
# PROJECT_ID = "mythical-bazaar-475215-i7"

SA_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "mythical-bazaar-475215-i7")

if not os.path.exists(SA_PATH):
    raise RuntimeError(f"Service account JSON not found: {SA_PATH}")

creds = service_account.Credentials.from_service_account_file(SA_PATH)
storage_client = storage.Client(project=PROJECT_ID, credentials=creds)
# --- END ---

# storage_client = storage.Client(project=GCP_PROJECT_ID)

def _get_or_create_bucket(bucket_name: str):
    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket:
        return bucket
    if not GCS_CREATE_IF_NOT_EXISTS:
        raise RuntimeError(f"GCS bucket '{bucket_name}' không tồn tại.")
    if not (GCP_PROJECT_ID or storage_client.project):
        raise RuntimeError("Thiếu GOOGLE_CLOUD_PROJECT để tạo bucket.")
    bucket = storage_client.create_bucket(storage_client.bucket(bucket_name))
    logger.info(f"Created GCS bucket: {bucket.name}")
    return bucket

_gcs_bucket = _get_or_create_bucket(GCS_BUCKET_NAME)

def save_to_gcs(data_bytes: bytes, object_name: str, retries: int = 3):
    blob = _gcs_bucket.blob(object_name)
    for i in range(retries):
        try:
            blob.upload_from_string(data_bytes, content_type="application/json; charset=utf-8")
            logger.info(f"Uploaded to GCS: gs://{GCS_BUCKET_NAME}/{object_name}")
            return
        except Exception as e:
            logger.warning(f"GCS upload failed ({i+1}/{retries}): {e}")
            time.sleep(2)
    logger.error(f"Failed to upload after {retries} retries: {object_name}")

def generate_file_name():
    return f"{uuid.uuid4().hex}.json"

def slugify_topic(topic: str) -> str:
    s = (topic or "").lower().strip()
    s = s.replace("đ", "d")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "unknown"

def build_object_path(topic: str, year: int = None, month: int = None, day: int = None, prefix: str = "") -> str:
    safe_topic = slugify_topic(topic)
    fname = prefix + generate_file_name()
    if year and month and day:
        return f"vnexpress/{safe_topic}/{year:04d}/{month:02d}/{day:02d}/{fname}"
    return f"vnexpress/{safe_topic}/undated/{fname}"

# ======================== TIMEZONE ========================
try:
    from zoneinfo import ZoneInfo
    VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    VN_TZ = None

def now_vn():
    if VN_TZ:
        return datetime.now(VN_TZ)
    return datetime.now(timezone.utc) + timedelta(hours=7)

def _dt_from_iso_local(iso_str: str) -> Optional[datetime]:
    if not iso_str:
        return None
    s = iso_str.strip().replace("Z", "")
    try:
        dt = datetime.fromisoformat(s)
        if VN_TZ:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=VN_TZ)
            return dt.astimezone(VN_TZ)
        return dt
    except Exception:
        return None

# ======================== CATEIDS ========================
CATEIDS = {
    "Thời sự": "1001005",
    "Góc nhìn": "1003450",
    "Thế giới": "1001002",
    "Kinh doanh": "1003159",
    "Bất động sản": "1002565",
    "Giáo dục": "1003497",
    "Pháp luật": "1001007",
    "Sức khỏe": "1003750",
    "Đời sống": "1002966",
    "Du lịch": "1003231",
    "Khoa học": "1006219",
}

EXCLUDE_PATTERNS = (
    r"/interactive/", r"/video/", r"/podcast/", r"/photo/", r"/anh/",
    r"/infographic", r"/tra-cuu", r"/tracuu", r"/lien-he", r"/contact",
    r"/tin-tuc-anh", r"/multimedia", r"/#",
)
exclude_re = re.compile("|".join(EXCLUDE_PATTERNS), re.IGNORECASE)

# ======================== SELENIUM (Thread-local) ========================
_thread_local = threading.local()

def get_driver():
    if not hasattr(_thread_local, "driver"):
        UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--user-agent={UA}")
        options.set_capability("pageLoadStrategy", "eager")

        # TẮT ảnh, font, css để đỡ tốn RAM/bandwidth
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
            "profile.managed_default_content_settings.cookies": 1,
            "profile.managed_default_content_settings.javascript": 1,  # JS vẫn bật (cần cho comment)
        }
        options.add_experimental_option("prefs", prefs)


        # Tạo thư mục tạm cho profile Chrome
        profile_dir = tempfile.mkdtemp(prefix="vne-selenium-")
        options.add_argument(f"--user-data-dir={profile_dir}")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(25)
        driver.implicitly_wait(0)

        _thread_local.driver = driver
        _thread_local.profile_dir = profile_dir

    return _thread_local.driver

def cleanup_driver():
    """Đóng Chrome và xóa thư mục tạm."""
    driver = getattr(_thread_local, "driver", None)
    profile_dir = getattr(_thread_local, "profile_dir", None)

    if driver is not None:
        try:
            driver.quit()
        except Exception as e:
            logger.warning(f"Lỗi khi quit driver: {e}")

    if profile_dir and os.path.exists(profile_dir):
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
            logger.info(f"Đã xóa thư mục tạm: {profile_dir}")
        except Exception as e:
            logger.warning(f"Lỗi khi xóa thư mục tạm {profile_dir}: {e}")

    # Xóa thuộc tính để tránh reuse driver đã hỏng
    if hasattr(_thread_local, "driver"):
        del _thread_local.driver
    if hasattr(_thread_local, "profile_dir"):
        del _thread_local.profile_dir

import atexit
atexit.register(cleanup_driver)


def safe_get(driver, url, retries=3, wait_css=None, timeout=65) -> bool:
    """
    Load URL an toàn:
    - Không để bất kỳ exception nào thoát ra ngoài.
    - Thử lại tối đa `retries` lần.
    - Nếu driver "chết" (mất kết nối localhost) thì tạo lại driver.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempt {attempt} to load: {url}")
            driver.set_page_load_timeout(timeout)
            driver.get(url)

            if wait_css:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )

            return True

        except Exception as e:
            # BẮT TẤT CẢ: TimeoutException, WebDriverException, ReadTimeoutError, v.v.
            logger.warning(f"Attempt {attempt} failed for {url}: {e}")

            # Nếu driver có vấn đề (thường là lỗi localhost:xxxxx), thử tạo lại driver
            try:
                cleanup_driver()
                driver = get_driver()
                logger.info("Đã tạo lại driver sau lỗi.")
            except Exception as e2:
                logger.error(f"Không tạo lại được driver: {e2}")

            time.sleep(3)

    logger.error(f"safe_get: hết {retries} lần thử mà vẫn lỗi: {url}")
    return False


# ======================== UTILITIES ========================
def to_absolute(base_url: str, href: str) -> str:
    return urljoin(base_url, href.strip())

def same_site(u: str, base_domain: str) -> bool:
    return urlparse(u).netloc.endswith(base_domain)

def contains_year(u: str) -> bool:
    return any(str(y) in u for y in range(2020, 2026))

def parse_datetime_maybe(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    fmts = [
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
        "%H:%M %d/%m/%Y", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).isoformat()
        except:
            pass
    return s

def _local_midnight_epoch(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=VN_TZ)
    return int(dt.timestamp())

def _local_end_of_day_epoch(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=VN_TZ)
    return int(dt.timestamp())

def _date_from_str(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

# ======================== URL BUILDER ========================
def build_day_url(cateid: str, from_epoch: int, to_epoch: int, page: Optional[int] = None) -> str:
    base = f"https://vnexpress.net/category/day/cateid/{cateid}/fromdate/{from_epoch}/todate/{to_epoch}/allcate/{cateid}"
    return base

def try_paginated_urls(base: str, page: int) -> List[str]:
    if page <= 1:
        return [base]
    return [f"{base}?page={page}", f"{base}?p={page}"]

# ======================== META & CONTENT ========================
def extract_common_meta(soup: BeautifulSoup, topic: str) -> Dict[str, str]:
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
    desc = soup.find("meta", {"name": "description"})
    description = desc["content"].strip() if desc and desc.get("content") else ""

    keywords = []
    tag = soup.find("meta", {"name": "keywords"})
    if tag and tag.get("content"):
        keywords = [k.strip() for k in tag["content"].split(",")]

    author = ""
    for css in [".author", "p.author", "span.author", ".article__author"]:
        tag = soup.select_one(css)
        if tag:
            author = tag.get_text(strip=True)
            break

    publish_date = ""
    meta_tag = soup.find("meta", {"property": "article:published_time"})
    if meta_tag and meta_tag.get("content"):
        publish_date = parse_datetime_maybe(meta_tag["content"])

    if not publish_date:
        t = soup.select_one("time[datetime]")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            publish_date = parse_datetime_maybe(t.get("datetime") or t.get_text(strip=True))

    if not publish_date:
        for css in [".date", "span.time", ".ArticleDateTime", ".breadcrumb_time"]:
            t = soup.select_one(css)
            if t:
                publish_date = parse_datetime_maybe(t.get_text(strip=True))
                if publish_date:
                    break

    image_url = ""
    og_img = soup.find("meta", {"property": "og:image"})
    if og_img and og_img.get("content"):
        image_url = og_img["content"]
    if not image_url:
        img_tag = soup.select_one("article.fck_detail img")
        if img_tag and img_tag.get("src"):
            image_url = img_tag["src"]

    sub_topic = extract_subtopic_vnexpress(soup, topic)
    return {
        "title": title,
        "description": description,
        "author": author,
        "publish_date": publish_date,
        "keywords": keywords,
        "topic": topic,
        "sub_topic": sub_topic,
        "image_url": image_url,
    }

def extract_content_vnexpress(soup: BeautifulSoup) -> str:
    body = soup.select_one("article.fck_detail")
    if body:
        paras = [p.get_text(" ", strip=True) for p in body.find_all("p")]
        return " ".join([p for p in paras if p])
    return ""

def extract_subtopic_vnexpress(soup: BeautifulSoup, topic: str) -> str:
    """
    Thử lấy subtopic từ breadcrumb.
    - Loại bỏ 'Trang chủ' và topic cha đang crawl.
    - Trả về hạng mục con sâu nhất (cuối breadcrumb).
    """
    # Các selector breadcrumb khả dĩ trên VnExpress
    crumbs = []
    for sel in ["ul.breadcrumb li a", "ul#breadcrumb li a", "nav.breadcrumb a"]:
        for a in soup.select(sel):
            txt = a.get_text(strip=True)
            if txt:
                crumbs.append(txt)

    if not crumbs:
        # Thử lấy từ meta
        meta_sec = soup.find("meta", {"property": "article:section"})
        if meta_sec and meta_sec.get("content"):
            sec = meta_sec["content"].strip()
            # Nếu section không trùng topic cha, coi như subtopic
            if topic and sec and sec.lower() != topic.lower():
                return sec
            return ""

    # Chuẩn hóa và lọc
    bad = {"trang chủ", "home", "video"}
    topic_l = (topic or "").strip().lower()
    candidates = [c for c in crumbs if c and c.strip().lower() not in bad]

    # Loại breadcrumb trùng topic cha
    candidates = [c for c in candidates if c.strip().lower() != topic_l]

    # Nếu còn nhiều cấp, lấy cấp sâu nhất (cuối danh sách)
    if candidates:
        return candidates[-1].strip()

    return ""

def extract_references_vnexpress(soup: BeautifulSoup) -> List[str]:
    """
    Tìm 'Nguồn/ Theo ...' trong bài viết.
    Heuristic:
      - Tìm các 'p' ở cuối bài có chứa từ khóa 'Theo', 'Nguồn'.
      - Tìm các cụm trong ngoặc (Ví dụ: '(Theo Reuters)').
    Trả về danh sách unique, giữ thứ tự xuất hiện.
    """
    body = soup.select_one("article.fck_detail") or soup.select_one("article") or soup
    texts = []

    # 1) Các 'p' cuối bài (ưu tiên 5 đoạn cuối)
    paras = body.find_all("p") if body else []
    for p in paras[-8:]:
        t = p.get_text(" ", strip=True)
        if not t:
            continue
        if re.search(r"(?i)\b(theo|nguồn)\b", t):
            texts.append(t)

    # 2) Tìm trong toàn văn các mẫu '(Theo ...)', '(Nguồn: ...)'
    full_text = body.get_text(" ", strip=True) if body else ""
    paren_matches = re.findall(r"\((?:\s*(?:Theo|Nguồn)\s*[:\-]?\s*)([^)]+)\)", full_text, flags=re.IGNORECASE)
    for m in paren_matches:
        texts.append(m.strip())

    # 3) Rút ra tên nguồn từ các dòng thu thập
    refs: List[str] = []
    for t in texts:
        # Biến thể: "Theo Reuters", "Theo AFP", "Nguồn: Bộ Y tế", "Theo báo cáo của WHO"
        for pat in [
            r"(?i)\btheo\s+([A-ZĐ][\w\s&\-.–—,:]+)$",
            r"(?i)\bnguồn[:\-]?\s+([A-ZĐ][\w\s&\-.–—,:]+)$",
            r"(?i)\btheo\s+([A-ZĐ][\w\s&\-.–—,:]+?),",  # lấy tới dấu phẩy
            r"(?i)\bnguồn[:\-]?\s+([A-ZĐ][\w\s&\-.–—,:]+?),",
        ]:
            m = re.search(pat, t)
            if m:
                cand = m.group(1).strip()
                # Làm gọn phần đuôi rác
                cand = re.sub(r"[\s,.;:–—\-]+$", "", cand).strip()
                # Giới hạn độ dài hợp lý
                if 2 <= len(cand) <= 80:
                    refs.append(cand)

    # 4) Unique giữ thứ tự
    seen: Set[str] = set()
    out: List[str] = []
    for r in refs:
        key = r.lower()
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def wait_for_comment_count(driver, timeout=20):
    """
    Đợi đến khi #total_comment có TEXT chứa số (ví dụ '115'),
    chứ không chỉ xuất hiện element.
    """
    def _has_number(drv):
        try:
            el = drv.find_element(By.ID, "total_comment")
            txt = el.text.strip()
            # Loại các trạng thái "đang load"
            if not txt or txt.lower() in ("xem thêm", "đang tải...", "đang tải", "dang tai...", "dang tai"):
                return False
            # Phải có ít nhất 1 chữ số
            return bool(re.search(r"\d", txt))
        except Exception:
            return False

    WebDriverWait(driver, timeout).until(_has_number)


# ======================== COMMENTS ========================
def extract_comments_vnexpress(driver, limit: int = 50) -> Tuple[int, List[Dict]]:
    """
    LẤY CHUẨN comment_count DÙ:
    - <label id="total_comment">3</label>
    - (<label id="total_comment">3</label>)
    - 1.2k, 15k, 0
    """
    comment_count = 0
    top_comments = []

    try:
        # BƯỚC 1: ĐỢI tới khi #total_comment có số (không còn 'Đang tải...')
        try:
            wait_for_comment_count(driver, timeout=30)
        except TimeoutException:
            logger.info("Không có hoặc không load xong phần comment → comment_count = 0")
            return 0, []

        # BƯỚC 2: SCROLL ĐẾN PHẦN COMMENT
        try:
            comment_section = driver.find_element(By.ID, "list_comment")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", comment_section)
        except:
            count_element = driver.find_element(By.ID, "total_comment")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", count_element)
        time.sleep(3)

        # BƯỚC 3: DÙNG BeautifulSoup ĐỌC HTML ĐỂ LẤY TEXT CHUẨN
        soup = BeautifulSoup(driver.page_source, "html.parser")
        label_tag = soup.find("label", id="total_comment")
        if not label_tag:
            logger.warning("Không tìm thấy label#total_comment trong HTML")
            return 0, []

        raw_text = label_tag.get_text(strip=True)
        logger.debug(f"[COMMENT_COUNT] Raw from HTML: '{raw_text}'")

        # BƯỚC 4: PARSE SỐ TỪ TEXT
        if not raw_text or raw_text in ["", "Xem thêm", "Đang tải..."]:
            logger.warning(f"total_comment rỗng hoặc đang load: '{raw_text}'")
            return 0, []

        # Xử lý: 3, 15, 1.2k, 15k
        cleaned = raw_text.lower().replace('k', '000').replace('.', '').replace(',', '').replace('(', '').replace(')', '')
        match = re.search(r'\d+', cleaned)
        if match:
            comment_count = int(match.group())
        else:
            comment_count = 0

        logger.info(f"[COMMENT_COUNT] Parsed: {comment_count}")

        # BƯỚC 5: Nếu có comment → parse danh sách
        if comment_count == 0:
            return 0, []

        comment_divs = soup.select("div#list_comment div.comment_item")[:limit]
        if not comment_divs:
            logger.warning("Có comment_count nhưng không có comment_item")
            return comment_count, []

        for div in comment_divs:
            # --- Tên ---
            name_tag = div.select_one("span.txt-name a.nickname") or div.select_one("span.txt-name")
            commenter_name = name_tag.get_text(strip=True) if name_tag else ""

            # --- Nội dung ---
            content_tag = div.select_one("p.full_content") or div.select_one("p.content_less")
            content_text = content_tag.get_text(" ", strip=True) if content_tag else ""
            if commenter_name and content_text.startswith(commenter_name):
                content_text = content_text[len(commenter_name):].strip()

            # --- Tổng like ---
            like_tag = div.select_one("div.reactions-total a.number")
            total_likes = 0
            if like_tag:
                like_text = like_tag.get_text(strip=True).lower().replace('k', '000').replace('.', '').replace(',', '')
                like_match = re.search(r'\d+', like_text)
                if like_match:
                    total_likes = int(like_match.group())

            # --- interaction_details ---
            interaction_details = {}
            detail_div = div.select_one("div.reactions-detail")
            if detail_div:
                for item in detail_div.select("div.item"):
                    img_tag = item.select_one("span.icons img")
                    label_vi = img_tag.get("alt", "").strip() if img_tag else ""
                    if not label_vi:
                        label_tag = item.find("span", class_="label")
                        if label_tag:
                            label_vi = label_tag.get_text(strip=True)
                    label_en = REACTION_MAP.get(label_vi, label_vi.lower().replace(" ", "_"))

                    num_tag = item.find("strong")
                    if label_en and num_tag:
                        num_text = num_tag.get_text(strip=True).lower().replace('k', '000').replace('.', '').replace(',', '')
                        num_match = re.search(r'\d+', num_text)
                        if num_match:
                            num = int(num_match.group())
                            if num > 0:
                                interaction_details[label_en] = num

            if commenter_name or content_text:
                top_comments.append({
                    "commenter_name": commenter_name,
                    "comment_content": content_text,
                    "total_likes": total_likes,
                    "interaction_details": interaction_details
                })

    except Exception as e:
        logger.warning(f"Lỗi lấy comment: {e}")
        comment_count = 0

    return comment_count, top_comments

# ======================== HTTP ========================
_thread_local_http = threading.local()

def _get_session() -> requests.Session:
    s = getattr(_thread_local_http, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,vi-VN;q=0.9,en;q=0.8",
        })
        _thread_local_http.session = s
    return s

def http_get(url: str, timeout=HTTP_TIMEOUT, retries=HTTP_RETRIES) -> Optional[str]:
    sess = _get_session()
    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }
    alt_uas = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    ]

    for i in range(retries + 1):
        try:
            r = sess.get(url, headers=base_headers, timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            elif r.status_code == 406:
                ua = alt_uas[min(i, len(alt_uas) - 1)]
                sess.headers.update({"User-Agent": ua})
                logger.warning(f"[HTTP] 406 -> đổi UA ({i+1}/{retries})")
            else:
                logger.warning(f"[HTTP] {r.status_code} for {url}")
        except Exception as e:
            logger.warning(f"[HTTP] err {e} ({i+1}/{retries})")
        time.sleep(0.4 * (i + 1))
    return None

# ======================== LINKS ========================
def looks_like_article(url: str, domain: str) -> bool:
    if exclude_re.search(url):
        return False
    if domain.endswith("vnexpress.net"):
        return url.endswith(".html") or contains_year(url)
    return contains_year(url)

def dedupe_preserve_order(xs: List[str]) -> List[str]:
    seen = set()
    return [x for x in xs if x not in seen and not seen.add(x)]

def get_links_from_listpage_http(list_url: str) -> List[str]:
    html = http_get(list_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    base = f"{urlparse(list_url).scheme}://{urlparse(list_url).netloc}"
    domain = urlparse(list_url).netloc
    links = []
    for a in soup.find_all("a", href=True):
        abs_url = to_absolute(base, a["href"])
        if same_site(abs_url, domain) and looks_like_article(abs_url, domain):
            links.append(abs_url)
    return dedupe_preserve_order(links)

def collect_links_in_daterange(cateid: str, d_from: date, d_to: date, max_pages: int = 20) -> List[str]:
    if d_to < d_from:
        raise ValueError("d_to phải >= d_from")
    from_epoch = _local_midnight_epoch(d_from)
    to_epoch = _local_end_of_day_epoch(d_to)
    base = build_day_url(cateid, from_epoch, to_epoch)

    all_links = []
    for page in range(1, max_pages + 1):
        page_variants = try_paginated_urls(base, page)
        page_links = []
        loaded = False
        for u in page_variants:
            links = get_links_from_listpage_http(u)
            if links:
                page_links = links
                loaded = True
                logger.info(f"Loaded page {page} with {len(page_links)} links")
                break
        if not loaded:
            break
        before = len(all_links)
        all_links = dedupe_preserve_order(all_links + page_links)
        if len(all_links) == before:
            logger.info("Hết link mới.")
            break
        time.sleep(PER_PAGE_SLEEP)
    return all_links

# ======================== PARSE ARTICLE ========================
def parse_article(url: str, topic: str, year: int, month: int, day: int) -> Dict:
    driver = get_driver()
    timeout = 30 if ENABLE_COMMENTS else 10
    if not safe_get(driver, url, wait_css="article.fck_detail, h1", timeout=timeout):
        logger.warning(f"Không tải được bài: {url}")
        return {}

    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        meta = extract_common_meta(soup, topic)
        main_content = extract_content_vnexpress(soup)

        # author = meta["author"]
        # if not author:
        #     author_tag = soup.select_one("p.Normal[style*='text-align:right;'] strong")
        #     if author_tag:
        #         author = author_tag.get_text(strip=True)

        author = meta["author"]
        if not author:
            p_candidates = soup.select("article.fck_detail p.Normal") or soup.select("p.Normal")
            for p in reversed(p_candidates):
                align = (p.get("align") or "").lower()
                style = (p.get("style") or "").replace(" ", "").lower()
                if align == "right" or "text-align:right" in style or "text-align:center" in style:
                    strong = p.find("strong")
                    if strong:
                        author = strong.get_text(strip=True)
                        break
        meta["author"] = author

        references = extract_references_vnexpress(soup)

        comment_count, top_comments = (extract_comments_vnexpress(driver, COMMENT_LIMIT)
                                      if ENABLE_COMMENTS else (0, []))

        return {
            "title": meta["title"],
            "url": url,
            "description": meta["description"],
            "topic": meta["topic"],
            "sub_topic": meta["sub_topic"],
            "keywords": meta["keywords"],
            "publish_date": meta["publish_date"],
            "author": author,
            "image_url": meta["image_url"],
            "references": references,
            "main_content": main_content,
            "comment_count": comment_count,
            "top_comments": top_comments,
        }
    except Exception as e:
        logger.error(f"Lỗi parse bài {url}: {e}")
        return {}

# ======================== CRAWL BY DATE ========================
def crawl_vnexpress_by_date(topic_name: str, the_date: str, max_articles: Optional[int] = None) -> List[Dict]:
    if topic_name not in CATEIDS:
        raise ValueError(f"Chưa biết cateid cho '{topic_name}'.")

    d = _date_from_str(the_date)
    y, m, dd = d.year, d.month, d.day

    cateid = CATEIDS[topic_name]
    links = collect_links_in_daterange(cateid, d, d)

    if max_articles:
        links = links[:max_articles]
    logger.info(f"[{topic_name}] {the_date} - {len(links)} links")

    items: List[Dict] = []

    driver = get_driver()
    articles_since_restart = 0
    RESTART_THRESHOLD = 20  # mỗi 20 bài reset driver một lần (bạn có thể chỉnh)

    for url in links:
        try:
            dct = parse_article(url, topic_name, y, m, dd)
        except Exception as e:
            logger.error(f"[FATAL_URL] Lỗi không mong đợi khi xử lý URL {url}: {e}")
            continue

        if dct and (dct.get("title") or dct.get("main_content")):
            items.append(dct)
        else:
            logger.info(f"Bỏ qua bài rỗng hoặc parse không được: {url}")

        # === RESET DRIVER ĐỊNH KỲ ĐỂ GIẢM RAM ===
        articles_since_restart += 1
        if articles_since_restart >= RESTART_THRESHOLD:
            logger.info(f"Đã xử lý {articles_since_restart} bài -> reset Chrome driver để giải phóng RAM.")
            cleanup_driver()
            driver = get_driver()
            articles_since_restart = 0

    if not items:
        return []

    # Phần sắp xếp & lưu GCS giữ nguyên
    items.sort(
        key=lambda x: (_dt_from_iso_local(x.get("publish_date")) or datetime.min),
        reverse=True,
    )

    results: List[Dict] = []
    for dct in items:
        ts = _dt_from_iso_local(dct.get("publish_date"))
        if ts:
            y, m, dd = ts.year, ts.month, ts.day
            prefix = ts.strftime("%Y%m%dT%H%M%S") + "_"
        else:
            prefix = ""
            if USE_INGESTED_DATE_FALLBACK:
                now = now_vn()
                y, m, dd = now.year, now.month, now.day

        dct["year"], dct["month"], dct["day"] = y, m, dd
        object_name = build_object_path(topic_name, y, m, dd, prefix=prefix)
        payload = json.dumps(dct, ensure_ascii=False).encode("utf-8")
        save_to_gcs(payload, object_name)
        results.append(dct)

    return results



def crawl_vnexpress_by_daterange(topic_name: str, date_from: str, date_to: Optional[str] = None,
                                 max_articles_per_day: Optional[int] = None) -> List[Dict]:
    d_start = _date_from_str(date_from)
    d_end = _date_from_str(date_to) if date_to else d_start
    cur = d_start
    all_items = []
    while cur <= d_end:
        day_str = cur.strftime("%Y-%m-%d")
        logger.info(f"[DAY] {topic_name} - {day_str}")

        try:
            day_items = crawl_vnexpress_by_date(
                topic_name,
                day_str,
                max_articles=max_articles_per_day
            )
        except Exception as e:
            # Ngày này có vấn đề (bug logic, v.v.) -> log rồi bỏ qua, không dừng chương trình
            logger.error(f"[FATAL_DAY] Lỗi khi crawl ngày {day_str} cho topic {topic_name}: {e}")
            day_items = []

        if day_items:
            all_items.extend(day_items)
        else:
            logger.info(f"[DAY] Không có item nào (hoặc lỗi) cho {topic_name} - {day_str}")

        cur += timedelta(days=1)

    return all_items


# ======================== MAIN ========================
if __name__ == "__main__":
    # Healthcheck GCS
    save_to_gcs(b'{"ok": true}', "healthcheck/ok.json")
    logger.info("Smoke test GCS: OK")

    start_date = os.getenv("START_DATE", "2025-09-01")
    end_date = os.getenv("END_DATE", "2025-09-30")     


    # topics_env = os.getenv("TOPICS")
    # if topics_env:
    #     topic_list = [t.strip() for t in topics_env.split(",") if t.strip()]
    # else:
    #     topic_list = list(CATEIDS.keys())

    

    # Lọc topic (bạn giữ nguyên logic của bạn)
    topics_env = os.getenv("TOPICS")
    if topics_env:
        topic_list = [t.strip() for t in topics_env.split(",") if t.strip()]
    else:
        excluded_topics = {"Sức khỏe", "Bất động sản"} 
        topic_list = [t for t in CATEIDS.keys() if t in excluded_topics]


    logger.info(f"[TOPICS] Sẽ crawl song song {len(topic_list)} topic: {topic_list}")
    logger.info(f"[DATE RANGE] {start_date} → {end_date}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # =============== FUNCTION để chạy song song cho từng topic ===============
    def crawl_one_topic(topic_name: str):
        try:
            logger.info(f"[START_TOPIC] {topic_name}")
            result = crawl_vnexpress_by_daterange(
                topic_name=topic_name,
                date_from=start_date,
                date_to=end_date,
                max_articles_per_day=None
            )
            logger.info(f"[DONE_TOPIC] {topic_name}: {len(result)} bài")
            return topic_name, result
        except Exception as e:
            logger.error(f"[ERR_TOPIC] {topic_name}: {e}")
            return topic_name, []
        finally:
            cleanup_driver()

    # =============== CHẠY SONG SONG ===============
    MAX_TOPIC_WORKERS = min(MAX_WORKERS, len(topic_list))
    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_TOPIC_WORKERS) as executor:
        future_map = {executor.submit(crawl_one_topic, topic): topic for topic in topic_list}

        for fut in as_completed(future_map):
            topic = future_map[fut]
            try:
                topic_name, items = fut.result()
                logger.info(f"[FINISHED] {topic_name}: {len(items)} bài")
                all_results.extend(items)
            except Exception as e:
                logger.error(f"[FATAL] Khi xử lý topic {topic}: {e}")

    logger.info("Hoàn tất crawl toàn bộ topic.")
    cleanup_driver()
    import atexit
    atexit.register(cleanup_driver)
