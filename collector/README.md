Backfill:
python3 collector/crawl_vnexpress.py --mode backfill --start 2024-01-01 --end 2025-01-02

Streaming (cập nhật liên tục, mặc định 5 phút/lần):
python collectors/crawl_vnexpress.py --mode streaming --interval 300

Resume (tiếp tục sau khi gián đoạn):
python collectors/crawl_vnexpress.py --mode resume
