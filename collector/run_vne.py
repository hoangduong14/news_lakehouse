# run_vne.py
# Wrapper để tự tính START_DATE / END_DATE 15 ngày một lần
import os
import json
import subprocess
from datetime import datetime, date, timedelta

STATE_FILE = "crawl_state.json"   # file lưu ngày kết thúc lần trước
WINDOW_DAYS = 2                  # mỗi lần crawl 2 ngày
FIRST_START_DATE = "2025-05-01"   # ngày bắt đầu lần đầu (bạn chỉnh 1 lần)

CRAWLER_FILE = "test_moi_nhat.py"  # TÊN FILE CRAWLER CHÍNH CỦA BẠN


def load_next_date_range():
    """
    Tính (start_date, end_date) cho lần chạy này:
    - Nếu có STATE_FILE -> lấy last_end_date, +1 ngày làm start.
    - Nếu chưa có STATE_FILE -> dùng FIRST_START_DATE làm start.
    - end_date = start_date + WINDOW_DAYS - 1
    - Nếu end_date > hôm nay -> cắt về hôm nay.
    """
    today = date.today()

    # Lần sau trở đi: đọc từ state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            last_end_str = state.get("last_end_date")
            if last_end_str:
                last_end = datetime.strptime(last_end_str, "%Y-%m-%d").date()
                start = last_end + timedelta(days=1)
            else:
                # Nếu file lỗi -> fallback dùng FIRST_START_DATE
                start = datetime.strptime(FIRST_START_DATE, "%Y-%m-%d").date()
        except Exception as e:
            print(f"[STATE] Lỗi đọc {STATE_FILE}, dùng FIRST_START_DATE. Lỗi: {e}")
            start = datetime.strptime(FIRST_START_DATE, "%Y-%m-%d").date()
    else:
        # Lần chạy đầu tiên
        start = datetime.strptime(FIRST_START_DATE, "%Y-%m-%d").date()

    # Nếu start đã > hôm nay -> không còn gì để crawl nữa
    if start > today:
        print(f"[STATE] start_date ({start}) > hôm nay ({today}), không còn ngày nào để crawl.")
        return None, None

    end = start + timedelta(days=WINDOW_DAYS - 1)
    if end > today:
        end = today

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    return start_str, end_str


def save_last_end_date(end_date_str: str):
    state = {"last_end_date": end_date_str}
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        print(f"[STATE] Đã lưu last_end_date = {end_date_str} vào {STATE_FILE}")
    except Exception as e:
        print(f"[STATE] Lỗi ghi {STATE_FILE}: {e}")


def main():
    start_date, end_date = load_next_date_range()
    if not start_date or not end_date:
        return

    print(f"[RUNNER] Chạy crawler cho khoảng: {start_date} → {end_date}")

    # copy môi trường hiện tại rồi set thêm
    env = os.environ.copy()
    env["START_DATE"] = start_date
    env["END_DATE"] = end_date

    # Gọi file crawler chính
    # Nếu bạn dùng python3 cụ thể: thay "python" thành "python3" hoặc đường dẫn python.exe
    result = subprocess.run(
        ["python", CRAWLER_FILE],
        env=env,
    )

    if result.returncode == 0:
        print("[RUNNER] Crawl thành công, lưu state.")
        save_last_end_date(end_date)
    else:
        print(f"[RUNNER] Crawler trả về mã lỗi {result.returncode}, KHÔNG lưu state.")


if __name__ == "__main__":
    main()
