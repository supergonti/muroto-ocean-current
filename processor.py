"""
室戸沖 海流データ収集システム - データ処理モジュール

機能:
  - NetCDFデータから室戸沖5地点のデータを抽出
  - 流速・流向の計算
  - CSV保存
  - 日次・月次サマリー生成

計算式（MDファイル参照）:
  流速 = √(u² + v²)
  流向 = atan2(u, v)  ※気象学的方向: 0=北, 時計回り
"""

import math
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import (
    MEASUREMENT_POINTS,
    VAR_U, VAR_V, VAR_TEMP, VAR_SALT,
    VAR_LON, VAR_LAT, VAR_TIME, VAR_DEP,
    DEPTH_LEVEL_INDEX,
    NEARBY_TOLERANCE_DEG,
    KNOT_FACTOR,
    OUTPUT_DIR, OUTPUT_PREFIX,
    CSV_COLUMNS,
)

logger = logging.getLogger(__name__)


# =============================================================================
# 変数名マッピング（データソースによって変数名が異なる場合に対応）
# =============================================================================
VARIABLE_NAME_ALIASES = {
    # u 東西流速
    "u": ["u", "uo", "u_velocity", "ucur", "uvel", "u10"],
    # v 南北流速
    "v": ["v", "vo", "v_velocity", "vcur", "vvel", "v10"],
    # 水温
    "temp": ["temp", "thetao", "temperature", "sst", "sea_water_potential_temperature"],
    # 塩分
    "salt": ["salt", "so", "salinity", "sal", "sea_water_salinity"],
    # 経度
    "lon": ["lon", "longitude", "x"],
    # 緯度
    "lat": ["lat", "latitude", "y"],
    # 深度
    "depth": ["depth", "deptht", "lev", "level", "z"],
}


def find_variable(ds, canonical_name: str) -> Optional[str]:
    """
    データセット中から変数名を検索（エイリアス対応）

    Args:
        ds: xarray.Dataset
        canonical_name: 正規変数名 (例: "u", "temp")

    Returns:
        データセット内の実際の変数名（見つからない場合はNone）
    """
    aliases = VARIABLE_NAME_ALIASES.get(canonical_name, [canonical_name])
    for alias in aliases:
        if alias in ds.data_vars or alias in ds.coords:
            return alias
    return None


# =============================================================================
# 最近傍グリッド点の検索
# =============================================================================

def find_nearest_grid(lon_arr, lat_arr, target_lon: float, target_lat: float):
    """
    グリッド配列から指定座標に最も近いインデックスを返す

    Args:
        lon_arr: 経度配列 (numpy array)
        lat_arr: 緯度配列 (numpy array)
        target_lon: 目標経度
        target_lat: 目標緯度

    Returns:
        (lat_idx, lon_idx) のタプル
    """
    # 1次元の場合は2Dグリッドを想定して処理
    if lon_arr.ndim == 1 and lat_arr.ndim == 1:
        lat_idx = int(np.argmin(np.abs(lat_arr - target_lat)))
        lon_idx = int(np.argmin(np.abs(lon_arr - target_lon)))
        actual_lat = float(lat_arr[lat_idx])
        actual_lon = float(lon_arr[lon_idx])
    else:
        # 2次元グリッドの場合
        dist = (lon_arr - target_lon) ** 2 + (lat_arr - target_lat) ** 2
        idx = np.unravel_index(np.argmin(dist), dist.shape)
        lat_idx, lon_idx = idx
        actual_lat = float(lat_arr[lat_idx, lon_idx])
        actual_lon = float(lon_arr[lat_idx, lon_idx])

    dist_deg = math.sqrt((actual_lat - target_lat) ** 2 + (actual_lon - target_lon) ** 2)
    logger.debug(
        f"    最近傍グリッド: ({target_lat:.4f}, {target_lon:.4f}) "
        f"→ ({actual_lat:.4f}, {actual_lon:.4f})  距離={dist_deg:.4f}°"
    )

    if dist_deg > NEARBY_TOLERANCE_DEG:
        logger.warning(
            f"    ⚠️ 最近傍グリッドが遠すぎます (dist={dist_deg:.3f}°): "
            f"目標({target_lat}, {target_lon})"
        )

    return lat_idx, lon_idx


# =============================================================================
# 流速・流向計算（MDファイルの計算式）
# =============================================================================

def calc_speed(u: float, v: float) -> float:
    """
    流速計算
    流速 = √(u² + v²)
    """
    if u is None or v is None or math.isnan(u) or math.isnan(v):
        return float("nan")
    return math.sqrt(u ** 2 + v ** 2)


def calc_direction(u: float, v: float) -> float:
    """
    流向計算（気象学的方向: 流れてくる方向）
    流向 = atan2(u, v)  → 0°=北, 90°=東, 180°=南, 270°=西

    Returns:
        度数法 (0-360度), NaNの場合はfloat("nan")
    """
    if u is None or v is None or math.isnan(u) or math.isnan(v):
        return float("nan")
    # atan2(u, v): 北を0°として時計回り
    direction_rad = math.atan2(u, v)
    direction_deg = math.degrees(direction_rad)
    # 0-360に正規化
    direction_deg = (direction_deg + 360) % 360
    return direction_deg


def ms_to_knot(speed_ms: float) -> float:
    """流速をm/sからknotに変換"""
    if math.isnan(speed_ms):
        return float("nan")
    return speed_ms * KNOT_FACTOR


def direction_to_compass(direction_deg: float) -> str:
    """
    度数法の流向を8方位に変換

    Returns:
        "北", "北東", "東", "南東", "南", "南西", "西", "北西"
    """
    if math.isnan(direction_deg):
        return ""
    directions = ["北", "北東", "東", "南東", "南", "南西", "西", "北西"]
    idx = int((direction_deg + 22.5) // 45) % 8
    return directions[idx]


# =============================================================================
# 1日分のデータ抽出
# =============================================================================

def extract_daily_data(ds, target_date: date) -> list[dict]:
    """
    xarray.Datasetから指定日の室戸沖5地点データを抽出

    Args:
        ds: xarray.Dataset (JCOPE2Mまたは互換フォーマット)
        target_date: 対象日付

    Returns:
        5地点分の辞書リスト（CSV行に対応）
    """
    rows = []

    # --- 変数名マッピング ---
    var_u    = find_variable(ds, "u")
    var_v    = find_variable(ds, "v")
    var_temp = find_variable(ds, "temp")
    var_salt = find_variable(ds, "salt")
    var_lon  = find_variable(ds, "lon")
    var_lat  = find_variable(ds, "lat")
    var_dep  = find_variable(ds, "depth")

    if not var_u or not var_v:
        logger.error(f"  → 流速変数 (u/v) が見つかりません。データセットの変数: {list(ds.data_vars)}")
        return rows

    # --- 経度・緯度配列取得 ---
    lons = ds[var_lon].values
    lats = ds[var_lat].values

    # --- 深度レベル選択 ---
    depth_sel = {}
    if var_dep and var_dep in ds.dims:
        depth_sel[var_dep] = DEPTH_LEVEL_INDEX
        depth_val = float(ds[var_dep].values[DEPTH_LEVEL_INDEX])
        logger.debug(f"  深度レベル {DEPTH_LEVEL_INDEX}: {depth_val:.1f}m")

    # --- 時刻インデックス選択 ---
    time_dim = find_variable(ds, "time") if "time" in ds.dims else None
    time_sel = {}
    if time_dim:
        time_sel[time_dim] = 0  # 通常は1ファイル1日分なのでindex=0

    # --- 5地点の抽出 ---
    for point_name, coords in MEASUREMENT_POINTS.items():
        target_lat = coords["lat"]
        target_lon = coords["lon"]

        logger.info(f"  [{target_date}] {point_name} ({target_lat}, {target_lon})")

        try:
            # 最近傍グリッド検索
            lat_idx, lon_idx = find_nearest_grid(lons, lats, target_lon, target_lat)

            def get_value(var_name, ds_obj, lat_i, lon_i, t_sel, d_sel):
                """変数値を安全に取得"""
                if var_name is None or var_name not in ds_obj:
                    return float("nan")

                da = ds_obj[var_name]

                # 次元に応じてインデックス選択
                sel_dict = {}
                for dim in da.dims:
                    if dim in t_sel:
                        sel_dict[dim] = t_sel[dim]
                    elif dim in d_sel:
                        sel_dict[dim] = d_sel[dim]

                if sel_dict:
                    da = da.isel(**sel_dict)

                # 空間インデックス取得
                # 次元名を確認して適切に選択
                dim_names = list(da.dims)
                if len(dim_names) == 2:
                    # (lat, lon) の2次元
                    val = float(da.values[lat_i, lon_i])
                elif len(dim_names) == 1:
                    # 残り1次元の場合
                    val = float(da.values[lon_i])
                else:
                    val = float("nan")

                return val

            u_val    = get_value(var_u,    ds, lat_idx, lon_idx, time_sel, depth_sel)
            v_val    = get_value(var_v,    ds, lat_idx, lon_idx, time_sel, depth_sel)
            temp_val = get_value(var_temp, ds, lat_idx, lon_idx, time_sel, depth_sel)
            salt_val = get_value(var_salt, ds, lat_idx, lon_idx, time_sel, depth_sel)

            # 流速・流向計算
            speed_ms  = calc_speed(u_val, v_val)
            speed_kn  = ms_to_knot(speed_ms)
            direction = calc_direction(u_val, v_val)

            row = {
                "date":      target_date.isoformat(),
                "point":     point_name,
                "lat":       target_lat,
                "lon":       target_lon,
                "u_ms":      round(u_val, 4)    if not math.isnan(u_val)    else None,
                "v_ms":      round(v_val, 4)    if not math.isnan(v_val)    else None,
                "speed_ms":  round(speed_ms, 4) if not math.isnan(speed_ms) else None,
                "speed_kn":  round(speed_kn, 4) if not math.isnan(speed_kn) else None,
                "direction": round(direction, 1) if not math.isnan(direction) else None,
                "temp_c":    round(temp_val, 2) if not math.isnan(temp_val) else None,
                "salinity":  round(salt_val, 3) if not math.isnan(salt_val) else None,
            }

            rows.append(row)
            logger.info(
                f"    → 流速: {speed_kn:.2f}kn  流向: {direction:.0f}°  水温: {temp_val:.1f}°C"
                if not math.isnan(speed_kn) else f"    → データなし（NaN）"
            )

        except Exception as e:
            logger.error(f"  [{target_date}] {point_name} 抽出エラー: {e}")
            rows.append({
                "date": target_date.isoformat(),
                "point": point_name,
                "lat": target_lat,
                "lon": target_lon,
                **{k: None for k in ["u_ms","v_ms","speed_ms","speed_kn","direction","temp_c","salinity"]}
            })

    return rows


# =============================================================================
# CSV 保存・読み込み
# =============================================================================

def save_to_csv(records: list[dict], output_path: Optional[Path] = None) -> Path:
    """
    レコードリストをCSVに保存

    Args:
        records: extract_daily_data() で得た辞書リスト
        output_path: 出力パス（Noneの場合は自動生成）

    Returns:
        保存したCSVファイルのパス
    """
    if not records:
        logger.warning("保存するレコードがありません")
        return None

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    if output_path is None:
        # 日付範囲からファイル名を生成
        dates = [r["date"] for r in records if r.get("date")]
        if dates:
            min_date = min(dates).replace("-", "")
            max_date = max(dates).replace("-", "")
            if min_date == max_date:
                output_path = output_dir / f"{OUTPUT_PREFIX}_{min_date}.csv"
            else:
                output_path = output_dir / f"{OUTPUT_PREFIX}_{min_date}_{max_date}.csv"
        else:
            output_path = output_dir / f"{OUTPUT_PREFIX}_data.csv"

    df = pd.DataFrame(records)

    # 列順を整理（CSV_COLUMNSで定義した順）
    cols_available = [c for c in CSV_COLUMNS if c in df.columns]
    extra_cols     = [c for c in df.columns if c not in CSV_COLUMNS]
    df = df[cols_available + extra_cols]

    # 既存CSVがあれば追記・重複排除
    if output_path.exists():
        existing = pd.read_csv(output_path)
        df = pd.concat([existing, df], ignore_index=True)
        df.drop_duplicates(subset=["date", "point"], keep="last", inplace=True)
        df.sort_values(["date", "point"], inplace=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"CSV保存: {output_path} ({len(df)} 行)")
    return output_path


def load_csv(csv_path: Path) -> pd.DataFrame:
    """CSVファイルを読み込む"""
    if not csv_path.exists():
        return pd.DataFrame(columns=CSV_COLUMNS)
    return pd.read_csv(csv_path, encoding="utf-8-sig")


# =============================================================================
# サマリー生成
# =============================================================================

def generate_monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    月次サマリーを生成（地点別・月別の平均値）

    Args:
        df: load_csv() で読み込んだDataFrame

    Returns:
        月次サマリーDataFrame
    """
    df = df.copy()
    df["date"]  = pd.to_datetime(df["date"])
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    summary = df.groupby(["year", "month", "point"]).agg(
        avg_speed_ms =("speed_ms",  "mean"),
        avg_speed_kn =("speed_kn",  "mean"),
        avg_direction=("direction", "mean"),
        avg_temp_c   =("temp_c",    "mean"),
        avg_salinity =("salinity",  "mean"),
        max_speed_kn =("speed_kn",  "max"),
        count        =("speed_ms",  "count"),
    ).round(3).reset_index()

    return summary


def print_summary(df: pd.DataFrame, last_n_days: int = 7):
    """最近N日間のデータサマリーを表示"""
    if df.empty:
        print("データがありません")
        return

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    recent = df.sort_values("date").tail(last_n_days * len(MEASUREMENT_POINTS))

    print(f"\n=== 直近データ（最新{last_n_days}日） ===")
    print(recent[["date", "point", "speed_kn", "direction", "temp_c"]].to_string(index=False))
    print()
