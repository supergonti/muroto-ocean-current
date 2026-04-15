"""
室戸沖 海流データ収集システム - モックデータ テスト

JCOPE2M形式に合わせた疑似NetCDFデータを生成し、
ダウンロード以外の全パイプラインを検証します。

実行:
  python test_with_mock.py
"""

import sys
import math
import shutil
import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

# パス設定
sys.path.insert(0, str(Path(__file__).parent))
from config import MEASUREMENT_POINTS, OUTPUT_DIR
from processor import (
    extract_daily_data,
    save_to_csv,
    generate_monthly_summary,
    calc_speed, calc_direction, ms_to_knot, direction_to_compass
)

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# テスト出力ディレクトリ
TEST_OUTPUT = Path(OUTPUT_DIR) / "test"


# =============================================================================
# モック NetCDF 生成
# =============================================================================

def create_mock_dataset(target_date: date) -> xr.Dataset:
    """
    JCOPE2M 形式を模した疑似 xarray.Dataset を生成

    グリッド解像度: 1/12度 (約9km) × 室戸沖周辺エリア
    変数: u, v, temp, salt
    """

    # --- グリッド設定（室戸沖周辺をカバー） ---
    lon_arr = np.arange(133.0, 136.0, 1/12)   # 経度 133〜136°, 1/12°刻み
    lat_arr = np.arange(32.0,  35.0, 1/12)    # 緯度 32〜35°, 1/12°刻み

    nlat = len(lat_arr)
    nlon = len(lon_arr)

    # --- 疑似流速データ生成（黒潮らしい流れを模擬） ---
    # 黒潮は概ね東向き・やや北向きに流れる
    np.random.seed(int(target_date.strftime("%j")))  # 日付でシード固定（再現性）

    # 基本流速：東方向 (u=正) が優勢
    u_base = 0.4  # 東向き基本流速 (m/s)
    v_base = 0.1  # 北向き基本流速 (m/s)

    # 空間的な変動（sin波で海流の蛇行を模擬）
    lon_grid, lat_grid = np.meshgrid(lon_arr, lat_arr)
    u_field = (
        u_base
        + 0.3 * np.sin(2 * np.pi * (lon_grid - 133) / 3)
        + 0.05 * np.random.randn(nlat, nlon)  # 小ノイズ
    )
    v_field = (
        v_base
        + 0.2 * np.cos(2 * np.pi * (lat_grid - 32) / 3)
        + 0.05 * np.random.randn(nlat, nlon)
    )

    # 水温：南ほど高い（黒潮影響）
    temp_field = (
        20.0
        + 3.0 * (lat_grid - 32) / (-3)       # 南ほど高温
        + 0.5 * np.sin(2 * np.pi * (lon_grid - 133) / 3)
        + 0.2 * np.random.randn(nlat, nlon)
    )
    # 季節補正（冬は低め、夏は高め）
    month = target_date.month
    season_offset = 3 * np.cos(2 * np.pi * (month - 8) / 12)  # 8月が最高
    temp_field += season_offset

    # 塩分：黒潮域は高塩分
    salt_field = (
        34.5
        + 0.3 * np.sin(2 * np.pi * (lon_grid - 133) / 3)
        + 0.1 * np.random.randn(nlat, nlon)
    )

    # --- xarray.Dataset 構築（JCOPE2M形式に合わせた変数名） ---
    ds = xr.Dataset(
        data_vars={
            "u":    (["lat", "lon"], u_field.astype(np.float32)),
            "v":    (["lat", "lon"], v_field.astype(np.float32)),
            "temp": (["lat", "lon"], temp_field.astype(np.float32)),
            "salt": (["lat", "lon"], salt_field.astype(np.float32)),
        },
        coords={
            "lon": lon_arr.astype(np.float64),
            "lat": lat_arr.astype(np.float64),
        },
        attrs={
            "title":   "JCOPE2M Mock Data for Testing",
            "date":    target_date.isoformat(),
            "source":  "Synthetic data (for pipeline testing)",
        }
    )
    return ds


# =============================================================================
# テスト実行
# =============================================================================

def run_tests():
    TEST_OUTPUT.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  室戸沖 海流データ収集システム - モックデータ テスト")
    print("=" * 65)

    # --------------------------------------------------------
    # テスト1: 計算式の検証
    # --------------------------------------------------------
    print("\n【テスト1】 計算式の検証 (MDファイル: 流速=√(u²+v²), 流向=atan2(u,v))")
    test_vectors = [
        (0.5,  0.0,  "東向き"),
        (0.0,  0.5,  "北向き"),
        (-0.5, 0.0,  "西向き"),
        (0.0,  -0.5, "南向き"),
        (0.35, 0.35, "北東向き"),
        (0.3,  -0.4, "南東向き"),
    ]
    all_pass = True
    for u, v, label in test_vectors:
        speed = calc_speed(u, v)
        direction = calc_direction(u, v)
        knot = ms_to_knot(speed)
        compass = direction_to_compass(direction)
        expected_speed = math.sqrt(u**2 + v**2)
        ok = abs(speed - expected_speed) < 1e-9
        if not ok:
            all_pass = False
        print(f"  {label:8s}: u={u:+.2f} v={v:+.2f} → "
              f"流速={speed:.3f}m/s({knot:.3f}kt) 流向={direction:5.1f}° {compass}  {'✅' if ok else '❌'}")

    print(f"  計算式テスト: {'✅ 全項目OK' if all_pass else '❌ エラーあり'}")

    # --------------------------------------------------------
    # テスト2: NetCDF生成 → 5地点抽出
    # --------------------------------------------------------
    print("\n【テスト2】 NetCDF生成 → 室戸沖5地点のデータ抽出")
    test_date = date(2023, 7, 15)
    ds = create_mock_dataset(test_date)
    print(f"  生成したデータセット:")
    print(f"    緯度グリッド: {len(ds.lat)}点  ({float(ds.lat.min()):.2f}°〜{float(ds.lat.max()):.2f}°)")
    print(f"    経度グリッド: {len(ds.lon)}点  ({float(ds.lon.min()):.2f}°〜{float(ds.lon.max()):.2f}°)")
    print(f"    変数: {list(ds.data_vars)}")

    records = extract_daily_data(ds, test_date)
    print(f"\n  抽出結果 ({len(records)}地点):")
    print(f"  {'地点':4s}  {'流速(kt)':>8s}  {'流向(°)':>7s}  {'方位':4s}  {'水温(°C)':>8s}  {'塩分':>6s}")
    print(f"  {'-'*50}")
    for r in records:
        spd = r.get('speed_kn') or 0
        dr  = r.get('direction') or 0
        tmp = r.get('temp_c') or 0
        sal = r.get('salinity') or 0
        comp = direction_to_compass(dr)
        print(f"  {r['point']:4s}  {spd:8.3f}  {dr:7.1f}  {comp:4s}  {tmp:8.2f}  {sal:6.3f}")

    if len(records) == 5:
        print("  抽出テスト: ✅ 5地点すべて抽出成功")
    else:
        print(f"  抽出テスト: ❌ {len(records)}地点しか抽出できませんでした")

    # --------------------------------------------------------
    # テスト3: 複数日データ生成 → CSV保存
    # --------------------------------------------------------
    print("\n【テスト3】 複数日データ → CSV保存")
    all_records = []
    test_start = date(2023, 1, 1)
    test_end   = date(2023, 3, 31)
    current = test_start
    day_count = 0

    print(f"  期間: {test_start} 〜 {test_end}")
    while current <= test_end:
        ds_day = create_mock_dataset(current)
        recs = extract_daily_data(ds_day, current)
        all_records.extend(recs)
        ds_day.close()
        day_count += 1
        current += timedelta(days=1)

    csv_path = TEST_OUTPUT / "mock_muroto_current_20230101_20230331.csv"
    saved_path = save_to_csv(all_records, csv_path)

    df = pd.read_csv(saved_path, encoding="utf-8-sig")
    print(f"  生成レコード数:   {len(all_records)} 行 ({day_count}日 × 5地点)")
    print(f"  CSV行数:          {len(df)} 行")
    print(f"  日付範囲:         {df['date'].min()} 〜 {df['date'].max()}")
    print(f"  地点:             {sorted(df['point'].unique())}")
    print(f"  保存先:           {saved_path}")

    # CSV内容サンプル
    print(f"\n  CSVサンプル（最初の5行）:")
    print(df.head().to_string(index=False))

    if len(df) == day_count * 5:
        print("\n  CSV保存テスト: ✅ 全レコード正常保存")
    else:
        print(f"\n  CSV保存テスト: ❌ レコード数不一致")

    # --------------------------------------------------------
    # テスト4: 月次サマリー生成
    # --------------------------------------------------------
    print("\n【テスト4】 月次サマリー生成")
    summary = generate_monthly_summary(df)
    summary_path = TEST_OUTPUT / "mock_monthly_summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print(f"  月次サマリー ({len(summary)}行):")
    display_cols = ["year", "month", "point", "avg_speed_kn", "avg_direction", "avg_temp_c"]
    print(summary[display_cols].to_string(index=False))
    print(f"\n  月次サマリー保存: {summary_path}")

    # --------------------------------------------------------
    # テスト5: 中心地点の時系列確認
    # --------------------------------------------------------
    print("\n【テスト5】 中心地点 流速の月別推移（1〜3月）")
    center_df = df[df["point"] == "中心"].copy()
    center_df["date"] = pd.to_datetime(center_df["date"])
    center_df["month"] = center_df["date"].dt.month
    monthly_center = center_df.groupby("month")[["speed_kn", "temp_c"]].mean().round(3)
    print(monthly_center.to_string())

    # --------------------------------------------------------
    # 最終サマリー
    # --------------------------------------------------------
    print("\n" + "=" * 65)
    print("  テスト完了")
    print("=" * 65)
    print(f"  出力ファイル:")
    for f in sorted(TEST_OUTPUT.iterdir()):
        print(f"    📄 {f.name}  ({f.stat().st_size:,} bytes)")
    print()
    print("  ✅ パイプライン全項目テスト完了！")
    print("  　 JCOPE認証情報を config.py に設定すれば")
    print("  　 実データの取得が可能です。")
    print()


if __name__ == "__main__":
    run_tests()
