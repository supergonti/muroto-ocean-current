"""
室戸沖 海流データ収集システム - ダウンロードモジュール

対応ソース:
  1. JCOPE2M OPeNDAP (JAMSTECへの申請後)
  2. JCOPE2M FTP ダウンロード
  3. CMEMS (Copernicus Marine Service) 代替ソース
  4. ローカルNetCDFファイル（既ダウンロード済みファイルの利用）
"""

import os
import sys
import ftplib
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

from config import (
    JCOPE_FTP_HOST, JCOPE_FTP_USER, JCOPE_FTP_PASS, JCOPE_FTP_PATH,
    JCOPE2M_OPENDAP_BASE,
    CMEMS_USE, CMEMS_USERNAME, CMEMS_PASSWORD, CMEMS_DATASET,
    OUTPUT_DIR
)

logger = logging.getLogger(__name__)

# ローカルキャッシュディレクトリ
CACHE_DIR = Path(OUTPUT_DIR) / "nc_cache"


def ensure_cache_dir():
    """キャッシュディレクトリを作成"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# JCOPE2M OPeNDAP アクセス
# =============================================================================

def build_opendap_url(target_date: date) -> str:
    """
    JCOPE2M OPeNDAP URLを構築する
    例: http://synthesis.jamstec.go.jp/JCOPE2M/opendap/daily/2022/JCOPE2M_20220101.nc
    """
    year_str = target_date.strftime("%Y")
    date_str = target_date.strftime("%Y%m%d")
    url = f"{JCOPE2M_OPENDAP_BASE}/daily/{year_str}/JCOPE2M_{date_str}.nc"
    return url


def open_opendap(target_date: date):
    """
    OPeNDAPで直接NetCDFデータセットを開く（ダウンロード不要）

    Returns:
        xarray.Dataset or None
    """
    try:
        import xarray as xr
        url = build_opendap_url(target_date)
        logger.info(f"OPeNDAP接続: {url}")
        ds = xr.open_dataset(url, engine="netcdf4")
        logger.info(f"  → 接続成功")
        return ds
    except ImportError:
        logger.error("xarrayがインストールされていません: pip install xarray netCDF4")
        return None
    except Exception as e:
        logger.warning(f"  → OPeNDAP接続失敗: {e}")
        return None


# =============================================================================
# JCOPE2M FTP ダウンロード
# =============================================================================

def build_ftp_filename(target_date: date) -> str:
    """FTPのファイル名を構築"""
    date_str = target_date.strftime("%Y%m%d")
    return f"JCOPE2M_{date_str}.nc"


def download_ftp(target_date: date, overwrite: bool = False) -> Optional[Path]:
    """
    JCOPE2M FTPからNetCDFファイルをダウンロード

    Args:
        target_date: 対象日付
        overwrite: Trueの場合、既存ファイルを上書き

    Returns:
        ダウンロードしたファイルのパス（失敗時はNone）
    """
    ensure_cache_dir()
    filename = build_ftp_filename(target_date)
    local_path = CACHE_DIR / filename

    # キャッシュ確認
    if local_path.exists() and not overwrite:
        logger.info(f"  → キャッシュ使用: {local_path}")
        return local_path

    if not JCOPE_FTP_USER:
        logger.error("FTP認証情報が未設定です。config.py の JCOPE_FTP_USER/JCOPE_FTP_PASS を設定してください。")
        return None

    try:
        logger.info(f"FTPダウンロード開始: {filename}")
        year_path = f"{JCOPE_FTP_PATH}{target_date.year}/"

        with ftplib.FTP(JCOPE_FTP_HOST) as ftp:
            ftp.login(JCOPE_FTP_USER, JCOPE_FTP_PASS)
            ftp.cwd(year_path)

            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)

        logger.info(f"  → ダウンロード完了: {local_path} ({local_path.stat().st_size:,} bytes)")
        return local_path

    except ftplib.all_errors as e:
        logger.error(f"  → FTPエラー: {e}")
        if local_path.exists():
            local_path.unlink()  # 壊れたファイルを削除
        return None


# =============================================================================
# CMEMS (Copernicus Marine Service) アクセス
# =============================================================================

# CMEMSダウンロードのキャッシュ先（test_cmems.py と共有）
CMEMS_CACHE_DIR = Path(OUTPUT_DIR) / "cmems"

# データセットID（テストで動作確認済み）
_CMEMS_DATASET_MY      = "cmems_mod_glo_phy_my_0.083deg_P1D-m"          # GLORYS12: 1993〜2025年（全変数）
_CMEMS_DATASET_AFC_CUR = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"    # 予報: 流速（uo/vo）
_CMEMS_DATASET_AFC_TMP = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m" # 予報: 水温（thetao）
_CMEMS_DATASET_AFC_SAL = "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m"     # 予報: 塩分（so）


def _select_cmems_dataset(target_date: date) -> str:
    """日付に応じてCMEMSデータセットを選択（メインデータセット）"""
    if target_date <= date(2025, 12, 31):
        return _CMEMS_DATASET_MY
    return _CMEMS_DATASET_AFC_CUR


def _get_cmems_variables(dataset_id: str) -> list:
    """データセットに応じた変数リストを返す"""
    if dataset_id == _CMEMS_DATASET_AFC_CUR:
        return ["uo", "vo"]
    if dataset_id == _CMEMS_DATASET_AFC_TMP:
        return ["thetao"]
    if dataset_id == _CMEMS_DATASET_AFC_SAL:
        return ["so"]
    return ["uo", "vo", "thetao", "so"]  # GLORYS12


def _rename_cmems_vars(ds):
    """CMEMSの変数名をJCOPE互換にリネーム（uo→u, vo→v, thetao→temp, so→salt）"""
    rename_map = {}
    for cmems_name, jcope_name in [("uo","u"),("vo","v"),("thetao","temp"),("so","salt")]:
        if cmems_name in ds:
            rename_map[cmems_name] = jcope_name
    if "longitude" in ds.coords:
        ds = ds.rename({"longitude": "lon"})
    if "latitude" in ds.coords:
        ds = ds.rename({"latitude": "lat"})
    if rename_map:
        ds = ds.rename(rename_map)
    return ds


def download_cmems(
    target_date: date,
    lat_min: float = 33.0,
    lat_max: float = 33.6,
    lon_min: float = 134.0,
    lon_max: float = 134.6,
    overwrite: bool = False
) -> Optional[Path]:
    """
    CMEMSから室戸沖エリアのデータをダウンロード

    事前準備:
        pip install copernicusmarine h5py
        python -c "import copernicusmarine; copernicusmarine.login()"

    Args:
        target_date: 対象日付
        lat_min/max: 緯度範囲
        lon_min/max: 経度範囲
        overwrite:   既存ファイルを上書きするか

    Returns:
        ダウンロードしたファイルのパス（失敗時はNone）
    """
    CMEMS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    date_str   = target_date.strftime("%Y%m%d")
    filename   = f"cmems_muroto_{date_str}.nc"
    local_path = CMEMS_CACHE_DIR / filename

    # キャッシュ確認
    if local_path.exists() and not overwrite:
        logger.info(f"  → CMEMSキャッシュ使用: {local_path.name}")
        return local_path

    try:
        import copernicusmarine as cm
    except ImportError:
        logger.error("coperniusmarineがインストールされていません: pip install copernicusmarine")
        return None

    dataset_id = _select_cmems_dataset(target_date)
    start_str  = f"{target_date.isoformat()}T00:00:00"
    end_str    = f"{target_date.isoformat()}T23:59:59"

    # 2026年以降: 3データセットを個別DL → 合体して保存
    if target_date > date(2025, 12, 31):
        return _download_cmems_forecast_merged(
            cm, target_date, start_str, end_str,
            lat_min, lat_max, lon_min, lon_max
        )

    # 〜2025年: GLORYS12（全変数を1回でDL）
    variables = _get_cmems_variables(dataset_id)
    logger.info(f"  → CMEMSダウンロード: {target_date} ({dataset_id})")
    try:
        cm.subset(
            dataset_id        = dataset_id,
            variables         = variables,
            start_datetime    = start_str,
            end_datetime      = end_str,
            minimum_longitude = lon_min,
            maximum_longitude = lon_max,
            minimum_latitude  = lat_min,
            maximum_latitude  = lat_max,
            minimum_depth     = 0.0,
            maximum_depth     = 1.0,
            output_filename   = filename,
            output_directory  = str(CMEMS_CACHE_DIR),
        )
    except Exception as e:
        logger.error(f"  → CMEMSエラー: {e}")
        return None

    if local_path.exists():
        logger.info(f"  → ダウンロード完了: {local_path.name} ({local_path.stat().st_size:,} bytes)")
        return local_path

    logger.error("  → CMEMSダウンロード失敗（ファイルが作成されませんでした）")
    return None


def _download_cmems_forecast_merged(
    cm, target_date: date, start_str: str, end_str: str,
    lat_min: float, lat_max: float, lon_min: float, lon_max: float
) -> Optional[Path]:
    """
    2026年以降: 流速・水温・塩分を3つのデータセットから個別にDLして1ファイルに合体
    """
    import xarray as xr

    date_str    = target_date.strftime("%Y%m%d")
    merged_path = CMEMS_CACHE_DIR / f"cmems_muroto_{date_str}.nc"

    datasets_to_fetch = [
        (_CMEMS_DATASET_AFC_CUR, ["uo", "vo"],    f"cmems_muroto_{date_str}_cur.nc"),
        (_CMEMS_DATASET_AFC_TMP, ["thetao"],       f"cmems_muroto_{date_str}_tmp.nc"),
        (_CMEMS_DATASET_AFC_SAL, ["so"],           f"cmems_muroto_{date_str}_sal.nc"),
    ]

    loaded = []
    for ds_id, vars_list, tmp_filename in datasets_to_fetch:
        tmp_path = CMEMS_CACHE_DIR / tmp_filename
        logger.info(f"  → CMEMS予報DL: {vars_list} ({ds_id})")
        try:
            cm.subset(
                dataset_id        = ds_id,
                variables         = vars_list,
                start_datetime    = start_str,
                end_datetime      = end_str,
                minimum_longitude = lon_min,
                maximum_longitude = lon_max,
                minimum_latitude  = lat_min,
                maximum_latitude  = lat_max,
                minimum_depth     = 0.0,
                maximum_depth     = 1.0,
                output_filename   = tmp_filename,
                output_directory  = str(CMEMS_CACHE_DIR),
            )
            if tmp_path.exists():
                loaded.append(xr.open_dataset(tmp_path))
            else:
                logger.warning(f"  → {tmp_filename} が作成されませんでした（スキップ）")
        except Exception as e:
            logger.warning(f"  → {vars_list} のDL失敗（スキップ）: {e}")

    if not loaded:
        logger.error("  → 予報データの全DLに失敗")
        return None

    # データセットを合体して保存
    merged = xr.merge(loaded, compat="override")
    merged.to_netcdf(merged_path)
    for ds in loaded:
        ds.close()
    # 一時ファイル削除
    for _, _, tmp_filename in datasets_to_fetch:
        tmp_path = CMEMS_CACHE_DIR / tmp_filename
        if tmp_path.exists():
            tmp_path.unlink()

    logger.info(f"  → 予報データ合体完了: {merged_path.name} ({merged_path.stat().st_size:,} bytes)")
    return merged_path


def open_cmems(target_date: date, overwrite: bool = False):
    """CMEMSデータをダウンロードしてxarray.Datasetとして返す"""
    try:
        import xarray as xr
    except ImportError:
        return None

    local_path = download_cmems(target_date, overwrite=overwrite)
    if local_path is None:
        return None
    try:
        ds = xr.open_dataset(local_path)
        return _rename_cmems_vars(ds)
    except Exception as e:
        logger.error(f"  → CMEMSファイル読み込み失敗: {e}")
        return None


# =============================================================================
# 統合ダウンロード関数
# =============================================================================

def get_dataset(target_date: date, use_local_dir: Optional[str] = None):
    """
    指定日付のデータセットを取得する（複数ソースを試みる）

    優先順位:
      1. ローカルキャッシュ（既存ファイル）
      2. ユーザー指定のローカルディレクトリ
      3. OPeNDAP（JCOPE2M）
      4. FTPダウンロード（JCOPE2M）
      5. CMEMS（設定されている場合）

    Args:
        target_date: 対象日付
        use_local_dir: ローカルNetCDFファイルのディレクトリ（任意）

    Returns:
        (xarray.Dataset, str) : データセットとソース名のタプル
        失敗時は (None, None)
    """
    try:
        import xarray as xr
    except ImportError:
        logger.error("xarrayがインストールされていません: pip install xarray netCDF4")
        return None, None

    ensure_cache_dir()

    # --- 1. ローカルキャッシュ確認 ---
    jcope_cache = CACHE_DIR / build_ftp_filename(target_date)
    if jcope_cache.exists():
        logger.info(f"[{target_date}] キャッシュから読み込み: {jcope_cache.name}")
        try:
            ds = xr.open_dataset(jcope_cache)
            return ds, "jcope_cache"
        except Exception as e:
            logger.warning(f"  → キャッシュ読み込み失敗: {e}")

    # --- 2. ユーザー指定ローカルディレクトリ ---
    if use_local_dir:
        local_file = _find_local_file(target_date, use_local_dir)
        if local_file:
            logger.info(f"[{target_date}] ローカルファイルから読み込み: {local_file.name}")
            try:
                ds = xr.open_dataset(local_file)
                return ds, "local"
            except Exception as e:
                logger.warning(f"  → ローカルファイル読み込み失敗: {e}")

    # --- 3. CMEMSキャッシュ確認（既にtest_cmems.pyでDL済みのファイル） ---
    cmems_cache = CMEMS_CACHE_DIR / f"cmems_muroto_{target_date.strftime('%Y%m%d')}.nc"
    if cmems_cache.exists():
        logger.info(f"[{target_date}] CMEMSキャッシュから読み込み: {cmems_cache.name}")
        try:
            ds = xr.open_dataset(cmems_cache)
            return _rename_cmems_vars(ds), "cmems_cache"
        except Exception as e:
            logger.warning(f"  → CMEMSキャッシュ読み込み失敗: {e}")

    # --- 4. CMEMS ダウンロード（認証情報が保存されていれば自動使用） ---
    logger.info(f"[{target_date}] CMEMSダウンロード試行...")
    ds = open_cmems(target_date)
    if ds is not None:
        return ds, "cmems"

    # --- 5. OPeNDAP (JCOPE2M) ---
    logger.info(f"[{target_date}] JCOPE OPeNDAPアクセス試行...")
    ds = open_opendap(target_date)
    if ds is not None:
        return ds, "jcope_opendap"

    # --- 6. FTPダウンロード (JCOPE2M) ---
    if JCOPE_FTP_USER:
        logger.info(f"[{target_date}] JCOPE FTPダウンロード試行...")
        local_path = download_ftp(target_date)
        if local_path:
            try:
                ds = xr.open_dataset(local_path)
                return ds, "jcope_ftp"
            except Exception as e:
                logger.warning(f"  → FTPファイル読み込み失敗: {e}")

    logger.error(f"[{target_date}] すべてのデータソースから取得失敗")
    return None, None


def _find_local_file(target_date: date, directory: str) -> Optional[Path]:
    """ローカルディレクトリから対象日付のNetCDFファイルを検索"""
    date_str = target_date.strftime("%Y%m%d")
    dir_path = Path(directory)

    patterns = [
        f"*{date_str}*.nc",
        f"*{date_str}*.NC",
        f"JCOPE2M_{date_str}.nc",
    ]
    for pattern in patterns:
        matches = list(dir_path.glob(pattern))
        if matches:
            return matches[0]

    return None


def check_available_sources() -> dict:
    """
    利用可能なデータソースを確認して報告

    Returns:
        {ソース名: 利用可否} の辞書
    """
    status = {}

    # OPeNDAP疎通確認
    try:
        import requests
        resp = requests.head(JCOPE2M_OPENDAP_BASE, timeout=5)
        status["JCOPE OPeNDAP"] = resp.status_code < 500
    except Exception:
        status["JCOPE OPeNDAP"] = False

    # FTP認証情報
    status["JCOPE FTP"] = bool(JCOPE_FTP_USER and JCOPE_FTP_PASS)

    # CMEMS
    status["CMEMS"] = CMEMS_USE and bool(CMEMS_USERNAME and CMEMS_PASSWORD)

    # copernicusmarine ライブラリ
    try:
        import copernicusmarine
        status["copernicusmarine ライブラリ"] = True
    except ImportError:
        status["copernicusmarine ライブラリ"] = False

    # xarray ライブラリ
    try:
        import xarray
        status["xarray ライブラリ"] = True
    except ImportError:
        status["xarray ライブラリ"] = False

    return status


def print_source_status():
    """データソースの利用状況を表示"""
    status = check_available_sources()
    print("\n=== データソース状況 ===")
    for name, available in status.items():
        mark = "✅" if available else "❌"
        print(f"  {mark} {name}")
    print()
