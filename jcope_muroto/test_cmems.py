"""
室戸沖 海流データ収集システム - CMEMS 実データ テスト
Copernicus Marine Service (https://marine.copernicus.eu) を使用

【事前準備】
  ① https://marine.copernicus.eu でアカウント登録（無料）
  ② 以下を実行してログイン情報を保存:
       copernicusmarine login
       （ユーザー名とパスワードを入力 → ~/.copernicusmarine に保存される）
  ③ または config.py の CMEMS_USERNAME / CMEMS_PASSWORD に直接書く

【実行方法】
  python test_cmems.py

【使用データセット】
  - cmems_mod_glo_phy_my_0.083deg_P1D-m  ← 2022年〜の履歴データ（推奨）
    (GLORYS12 リアナリシス: 1993〜直近まで, 解像度 1/12°)
  - cmems_mod_glo_phy_anfc_0.083deg_P1D-m ← 予報+直近リアルタイム

【CMEMSの変数名】
  uo    = 東西流速 (m/s)  ← JCOPEの u に対応
  vo    = 南北流速 (m/s)  ← JCOPEの v に対応
  thetao = 水温 (°C)     ← JCOPEの temp に対応
  so    = 塩分 (PSU)     ← JCOPEの salt に対応
"""

import sys
import logging
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import MEASUREMENT_POINTS, OUTPUT_DIR
from processor import extract_daily_data, save_to_csv, generate_monthly_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# CMEMS 設定
# =============================================================================

# 使用するデータセット（2025年4月時点の正式名称）
# GLORYS12 リアナリシス: 1993〜2025年をカバー（全変数: u/v/temp/salt）
DATASET_REANALYSIS = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
# 直近・予報（2024年〜）: 流速のみ取得する場合はこちら
DATASET_FORECAST   = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"

# 取得する変数
CMEMS_VARS = ["uo", "vo", "thetao", "so"]

# 室戸沖エリアの境界（5地点をカバー）
LAT_MIN, LAT_MAX = 33.0, 33.6
LON_MIN, LON_MAX = 134.0, 134.6

# テスト期間（短めにして動作確認）
TEST_START = date(2023, 6, 1)
TEST_END   = date(2023, 6, 7)   # 7日間テスト

# 出力ディレクトリ
CMEMS_OUTPUT = Path(OUTPUT_DIR) / "cmems"


# =============================================================================
# CMEMS データセット選択（日付に応じて自動切替）
# =============================================================================

def select_dataset(target_date: date) -> str:
    """
    日付に応じて最適なCMEMSデータセットを選択

    GLORYS12 (my): 1993〜2025年  ← 2022年以降のデータもカバー
    Forecast    : 直近・予報
    """
    # GLORYS12 リアナリシスは2025年まで対応しているのでほぼ全期間使用可能
    if target_date <= date(2025, 12, 31):
        return DATASET_REANALYSIS
    else:
        return DATASET_FORECAST


# =============================================================================
# 変数名リマッピング（CMEMS → JCOPE形式）
# =============================================================================

CMEMS_TO_JCOPE = {
    "uo":     "u",
    "vo":     "v",
    "thetao": "temp",
    "so":     "salt",
}

def rename_cmems_vars(ds):
    """CMEMSの変数名をJCOPE形式にリネームする"""
    rename_map = {}
    for cmems_name, jcope_name in CMEMS_TO_JCOPE.items():
        if cmems_name in ds:
            rename_map[cmems_name] = jcope_name

    # 座標名もリネーム
    if "longitude" in ds.coords:
        ds = ds.rename({"longitude": "lon"})
    if "latitude" in ds.coords:
        ds = ds.rename({"latitude": "lat"})

    if rename_map:
        ds = ds.rename(rename_map)
    return ds


# =============================================================================
# 1日分のCMEMSデータ取得
# =============================================================================

def fetch_cmems_day(target_date: date, overwrite: bool = False):
    """
    CMEMSから指定日の室戸沖データをダウンロードしてxarray.Datasetを返す

    Args:
        target_date: 取得対象日
        overwrite:   既存キャッシュを上書きするか

    Returns:
        xarray.Dataset or None
    """
    try:
        import copernicusmarine as cm
        import xarray as xr
    except ImportError:
        print("❌ coperniusmarine がインストールされていません")
        print("   pip install copernicusmarine を実行してください")
        return None

    CMEMS_OUTPUT.mkdir(parents=True, exist_ok=True)

    date_str    = target_date.strftime("%Y%m%d")
    dataset_id  = select_dataset(target_date)
    output_file = CMEMS_OUTPUT / f"cmems_muroto_{date_str}.nc"

    # キャッシュ確認
    if output_file.exists() and not overwrite:
        logger.info(f"[{target_date}] キャッシュ使用: {output_file.name}")
        ds = xr.open_dataset(output_file)
        return rename_cmems_vars(ds)

    # CMEMS からダウンロード
    logger.info(f"[{target_date}] CMEMSダウンロード中... (データセット: {dataset_id})")
    start_str = f"{target_date.isoformat()}T00:00:00"
    end_str   = f"{target_date.isoformat()}T23:59:59"

    try:
        cm.subset(
            dataset_id         = dataset_id,
            variables          = CMEMS_VARS,
            start_datetime     = start_str,
            end_datetime       = end_str,
            minimum_longitude  = LON_MIN,
            maximum_longitude  = LON_MAX,
            minimum_latitude   = LAT_MIN,
            maximum_latitude   = LAT_MAX,
            minimum_depth      = 0.0,
            maximum_depth      = 1.0,        # 表層のみ
            output_filename    = output_file.name,
            output_directory   = str(CMEMS_OUTPUT),
        )
    except Exception as e:
        logger.error(f"  → CMEMSダウンロードエラー: {e}")
        logger.info("  ヒント: 'copernicusmarine login' で認証情報を保存してください")
        return None

    if not output_file.exists():
        logger.error(f"  → ファイルが作成されませんでした: {output_file}")
        return None

    logger.info(f"  → ダウンロード完了: {output_file.name} ({output_file.stat().st_size:,} bytes)")
    ds = xr.open_dataset(output_file)
    return rename_cmems_vars(ds)


# =============================================================================
# テスト実行
# =============================================================================

def check_login():
    """CMEMS ログイン状態を確認"""
    try:
        import copernicusmarine as cm
        print("✅ copernicusmarine ライブラリ: インストール済み")

        # ログイン確認（接続テスト）
        cred_dir = Path.home() / ".copernicusmarine"
        # ファイル名はバージョンによって異なる（ドットあり・なし両方確認）
        cred_file = cred_dir / ".copernicusmarine-credentials"
        cred_file2 = cred_dir / "copernicusmarine-credentials"
        if cred_file.exists() or cred_file2.exists():
            print(f"✅ 認証情報: 保存済み ({cred_file})")
            return True
        else:
            print("❌ 認証情報が見つかりません")
            print("   → 'copernicusmarine login' を実行してください")
            return False
    except ImportError:
        print("❌ copernicusmarine: 未インストール")
        print("   → 'pip install copernicusmarine' を実行してください")
        return False


def run_cmems_test():
    print("=" * 65)
    print("  室戸沖 海流データ収集 - CMEMS 実データ テスト")
    print("=" * 65)

    # --- 認証確認 ---
    print("\n【ステップ1】 CMEMS 認証確認")
    logged_in = check_login()
    if not logged_in:
        print("\n📋 CMEMS 登録・ログイン手順:")
        print("  1. https://marine.copernicus.eu にアクセス")
        print("  2. 右上「Login」→「Register」で無料アカウント作成")
        print("  3. メール認証後、ターミナルで:")
        print("       pip install copernicusmarine")
        print("       copernicusmarine login")
        print("  4. このスクリプトを再実行")
        return

    # --- データ取得テスト ---
    print(f"\n【ステップ2】 室戸沖データ取得テスト")
    print(f"  期間: {TEST_START} 〜 {TEST_END}")
    print(f"  エリア: 緯度 {LAT_MIN}〜{LAT_MAX}, 経度 {LON_MIN}〜{LON_MAX}")

    all_records = []
    current = TEST_START
    success_count = 0

    while current <= TEST_END:
        ds = fetch_cmems_day(current)

        if ds is None:
            logger.warning(f"[{current}] スキップ")
            current += timedelta(days=1)
            continue

        # 5地点のデータ抽出
        records = extract_daily_data(ds, current)
        all_records.extend(records)
        ds.close()
        success_count += 1
        current += timedelta(days=1)

    if not all_records:
        print("\n❌ データを取得できませんでした")
        return

    # --- CSV保存 ---
    csv_path = CMEMS_OUTPUT / f"cmems_muroto_{TEST_START.strftime('%Y%m%d')}_{TEST_END.strftime('%Y%m%d')}.csv"
    saved = save_to_csv(all_records, csv_path)

    import pandas as pd
    df = pd.read_csv(saved, encoding="utf-8-sig")

    print(f"\n【結果】")
    print(f"  取得成功: {success_count}日  /  {(TEST_END - TEST_START).days + 1}日")
    print(f"  レコード: {len(df)}行")
    print(f"  保存先:   {saved}")

    print(f"\n  抽出データ（中心地点）:")
    center = df[df["point"] == "中心"][["date","speed_kn","direction","temp_c","salinity"]]
    print(center.to_string(index=False))

    print(f"\n  全5地点 平均値:")
    summary = df.groupby("point")[["speed_kn","direction","temp_c"]].mean().round(3)
    print(summary.to_string())

    print(f"\n✅ CMEMS 実データテスト完了！")
    print(f"   次は python main.py --start 2022-01-01 で全期間データ取得ができます。")


if __name__ == "__main__":
    run_cmems_test()
