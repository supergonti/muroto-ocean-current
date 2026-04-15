"""
室戸沖潮流基盤データ V2.0 - 設定ファイル
Muroto Offshore Current Data v2.0 - Configuration

MDファイル: muroto_offshore_current_points.md 参照
"""

from datetime import date

# =============================================================================
# ■ 測定地点設定（室戸岬周辺5地点）
#   並び順: 北西 → 西 → 室戸沖 → 東 → 北東
# =============================================================================
MEASUREMENT_POINTS = {
    "北西":   {"lat": 33.3567, "lon": 134.1733},  # 北の緯度 × 西の経度
    "西":     {"lat": 33.2667, "lon": 134.1733},
    "室戸沖": {"lat": 33.2667, "lon": 134.2833},  # 旧: 中心
    "東":     {"lat": 33.2667, "lon": 134.3933},
    "北東":   {"lat": 33.3567, "lon": 134.3933},  # 北の緯度 × 東の経度
}

# =============================================================================
# ■ データ取得期間
# =============================================================================
START_DATE = date(2022, 1, 1)    # 取得開始日（2022年1月1日以降）
END_DATE   = date.today()        # 取得終了日（今日まで）

# =============================================================================
# ■ JCOPE2M アクセス設定
# =============================================================================

# --- OPeNDAP経由（推奨：認証不要でアクセス可能な場合） ---
# JCOPE2M データサーバー（JAMSTECへの申請後にURLを確認してください）
JCOPE2M_OPENDAP_BASE = "http://synthesis.jamstec.go.jp/JCOPE2M/opendap"

# --- JCOPE-T リアルタイムデータ ---
JCOPET_BASE_URL = "https://www.jamstec.go.jp/jcope/htdocs/j/distribution/index.html"

# --- FTP経由（申請後に認証情報を設定） ---
JCOPE_FTP_HOST     = "ftp.jamstec.go.jp"
JCOPE_FTP_USER     = ""  # ← JCOPE登録後に設定
JCOPE_FTP_PASS     = ""  # ← JCOPE登録後に設定
JCOPE_FTP_PATH     = "/pub/JCOPE2M/daily/"

# --- CMEMS（Copernicus Marine Service）代替ソース ---
# JCOPE申請完了前の代替として利用可能（要無料登録）
CMEMS_USE = True        # CMEMSを使用（copernicusmarine loginで認証済みの場合）
CMEMS_USERNAME = ""     # https://marine.copernicus.eu で登録
CMEMS_PASSWORD = ""
CMEMS_DATASET  = "cmems_mod_glo_phy_anfc_0.083deg_P1D-m"

# =============================================================================
# ■ NetCDFデータ変数名設定
# =============================================================================
# JCOPE2Mのデータ変数名（実際のファイルに合わせて調整）
VAR_U    = "u"      # 東西流速 (m/s)
VAR_V    = "v"      # 南北流速 (m/s)
VAR_TEMP = "temp"   # 水温 (°C)
VAR_SALT = "salt"   # 塩分 (PSU)
VAR_LON  = "lon"    # 経度
VAR_LAT  = "lat"    # 緯度
VAR_TIME = "time"   # 時刻
VAR_DEP  = "depth"  # 深度

# 取得する深度レベル（0 = 最上層・表層）
DEPTH_LEVEL_INDEX = 0

# =============================================================================
# ■ 出力設定
# =============================================================================
OUTPUT_DIR    = "output"
OUTPUT_PREFIX = "muroto_offshore_current"   # CSVファイル名のプレフィックス

# CSV列名設定
CSV_COLUMNS = [
    "date",       # 日付
    "point",      # 地点名（北西/西/室戸沖/東/北東）
    "lat",        # 緯度
    "lon",        # 経度
    "u_ms",       # 東西流速 (m/s)
    "v_ms",       # 南北流速 (m/s)
    "speed_ms",   # 流速 (m/s)
    "speed_kn",   # 流速 (knot)
    "direction",  # 流向 (度, 0=北, 時計回り)
    "temp_c",     # 水温 (°C)
    "salinity",   # 塩分 (PSU)
]

# =============================================================================
# ■ 処理設定
# =============================================================================
NEARBY_TOLERANCE_DEG = 0.15  # 最近傍グリッド検索の許容範囲 (度)
KNOT_FACTOR          = 1.944 # m/s → knot 換算係数
