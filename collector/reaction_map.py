# reaction_map.py
"""
Bản đồ ánh xạ tên phản ứng tiếng Việt → tiếng Anh (CHUẨN VnExpress 2025)
Dùng để chuẩn hóa interaction_details
"""

REACTION_MAP = {
    "Thích": "like",
    "Yêu thích": "love",        
    "Vui": "haha",              
    "Haha": "haha",            
    "Ngạc nhiên": "wow",
    "Buồn": "sad",
    "Thương thương": "care",
    "Phẫn nộ": "angry"
}

# (Tùy chọn) Dùng để thêm emoji vào báo cáo
EMOJI_MAP = {
    "like": "(thumbs up)",
    "love": "(red heart)",
    "haha": "(face with tears of joy)",
    "wow": "(open mouth)",
    "sad": "(loudly crying face)",
    "care": "(smiling face with smiling eyes)",
    "angry": "(pouting face)"
}