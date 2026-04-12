"""
室戸沖潮流基盤データ V2.0 - メイン実行スクリプト
Muroto Offshore Current Data v2.0 - Main Script

【使い方】
  # 本日のデータを取得
  python main.py

  # 日付指定（単日）
  python main.py --date 2024-01-15

  # 期間指定
  python main.py --start 2023-01-01 --end 2023-12-31

  # 2022/1/1から今日まで全データ取得
  python main.py --all

  # ローカルNetCDFファイルから処理
  python main.py --local-dir ./nc_files --start 2023-01-01 --end 2023-01-31

  # データソース状況確認
  python main.py --check

  # 月次サマリー表示
  python main.py --summary
"""

import sys
import logging
import argparse
from datetime import date, timedelta
from pathlib import Path

# --- パッケージインポート ---
try:
    import numpy as np
    import pandas as pd
    import xarray as xr
except ImportError as e:
    print(f"❌ 必要なライブラリがインストールされていません: {e}")
    print("   pip install -r requirements.txt を実行してください")
    sys.exit(1)

from config import START_DATE, END_DATE, OUTPUT_DIR, OUTPUT_PREFIX
from downloader import print_source_status, get_dataset
from processor import extract_daily_data, save_to_csv, load_csv, generate_monthly_summary, print_summary

# =============================================================================
# ログ設定
# =============================================================================
def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(OUTPUT_DIR) / "run.log", encoding="utf-8"),
        ]
    )

logger = logging.getLogger(__name__)


# =============================================================================
# メイン処理
# =============================================================================

def collect_range(
    start: date,
    end: date,
    local_dir: str = None,
    skip_existing: bool = True,
) -> Path:
    """
    指定期間のデータを収集してCSVに保存

    Args:
        start: 開始日
        end: 終了日
        local_dir: ローカルNetCDFディレクトリ（任意）
        skip_existing: 既に取得済みの日付をスキップする

    Returns:
        保存したCSVファイルのパス
    """
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 常に同じファイルに追記・上書きする（muroto_current_all.csv）
    output_path = output_dir / f"{OUTPUT_PREFIX}_all.csv"
    existing_dates = set()

    if skip_existing and output_path.exists():
        existing_df = load_csv(output_path)
        if not existing_df.empty:
            existing_dates = set(existing_df["date"].unique())
            logger.info(f"既存データ: {len(existing_dates)} 日分を確認")

    # 日付イテレーション
    all_records = []
    current = start
    total_days = (end - start).days + 1
    processed = 0
    skipped   = 0
    failed    = 0

    print(f"\n{'='*60}")
    print(f"  室戸沖潮流基盤データ V2.0 — データ収集")
    print(f"  Muroto Offshore Current Data v2.0")
    print(f"  期間: {start} ～ {end}  ({total_days}日間)")
    print(f"{'='*60}\n")

    while current <= end:
        date_str = current.isoformat()

        # スキップ判定
        if skip_existing and date_str in existing_dates:
            logger.debug(f"[{date_str}] スキップ（既取得）")
            current += timedelta(days=1)
            skipped += 1
            continue

        # データ取得
        logger.info(f"[{date_str}] データ取得中... ({processed+1}/{total_days-skipped})")
        ds, source = get_dataset(current, use_local_dir=local_dir)

        if ds is None:
            logger.warning(f"[{date_str}] データ取得失敗 → スキップ")
            failed += 1
            current += timedelta(days=1)
            continue

        # 5地点データ抽出
        try:
            records = extract_daily_data(ds, current)
            all_records.extend(records)
            processed += 1
            ds.close()
        except Exception as e:
            logger.error(f"[{date_str}] 処理エラー: {e}")
            failed += 1
        finally:
            if ds is not None:
                try:
                    ds.close()
                except Exception:
                    pass

        current += timedelta(days=1)

        # 10日ごとに中間保存
        if len(all_records) > 0 and processed % 10 == 0:
            save_to_csv(all_records, output_path)
            logger.info(f"  → 中間保存: {len(all_records)} レコード")

    # 最終保存
    if all_records:
        output_path = save_to_csv(all_records, output_path)
    elif not output_path.exists():
        output_path = None

    # 結果表示
    print(f"\n{'='*60}")
    print(f"  完了サマリー")
    print(f"  取得成功: {processed} 日")
    print(f"  スキップ: {skipped} 日（既存データ）")
    print(f"  失敗:     {failed} 日")
    if output_path:
        print(f"  出力先:   {output_path}")
    print(f"{'='*60}\n")

    return output_path


def collect_single_day(target_date: date, local_dir: str = None) -> Path:
    """1日分のデータを収集"""
    return collect_range(target_date, target_date, local_dir=local_dir)


def show_summary(csv_path: Path = None):
    """サマリー表示"""
    if csv_path is None:
        # outputフォルダ内の最新CSVを使用
        output_dir = Path(OUTPUT_DIR)
        csvs = sorted(output_dir.glob(f"{OUTPUT_PREFIX}*.csv"))
        if not csvs:
            print("CSVファイルが見つかりません。先にデータを収集してください。")
            return
        csv_path = csvs[-1]

    df = load_csv(csv_path)
    if df.empty:
        print("データが空です")
        return

    print_summary(df)

    monthly = generate_monthly_summary(df)
    summary_path = csv_path.parent / f"{OUTPUT_PREFIX}_monthly_summary.csv"
    monthly.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"月次サマリー保存: {summary_path}")
    print(monthly.to_string(index=False))


# =============================================================================
# コマンドライン引数解析
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="室戸沖潮流基盤データ V2.0 / Muroto Offshore Current Data v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # モード選択
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--all",     action="store_true", help="2022/1/1から今日まで全データ取得")
    mode_group.add_argument("--check",   action="store_true", help="データソース接続確認")
    mode_group.add_argument("--summary", action="store_true", help="既存データのサマリー表示")

    # 期間指定
    parser.add_argument("--date",  type=str, help="単日取得 (例: 2024-01-15)")
    parser.add_argument("--start", type=str, help="開始日 (例: 2023-01-01)")
    parser.add_argument("--end",   type=str, help="終了日 (例: 2023-12-31)")

    # オプション
    parser.add_argument("--local-dir", type=str, help="ローカルNetCDFファイルのディレクトリ")
    parser.add_argument("--no-skip",   action="store_true", help="既存データを上書き取得")
    parser.add_argument("--verbose",   action="store_true", help="詳細ログ出力")

    return parser.parse_args()


def main():
    args = parse_args()

    # 出力ディレクトリ作成
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # ログ設定
    setup_logging(args.verbose)

    # --- データソース確認モード ---
    if args.check:
        print_source_status()
        return

    # --- サマリーモード ---
    if args.summary:
        show_summary()
        return

    # --- 日付設定 ---
    if args.all:
        start = START_DATE
        end   = END_DATE
    elif args.date:
        start = end = date.fromisoformat(args.date)
    elif args.start or args.end:
        start = date.fromisoformat(args.start) if args.start else START_DATE
        end   = date.fromisoformat(args.end)   if args.end   else date.today()
    else:
        # デフォルト: 今日
        start = end = date.today()

    # --- データ収集実行 ---
    csv_path = collect_range(
        start=start,
        end=end,
        local_dir=args.local_dir,
        skip_existing=not args.no_skip,
    )

    if csv_path:
        print(f"\n✅ 完了！CSVファイル: {csv_path}")
    else:
        print("\n⚠️  データを取得できませんでした。")
        print("    --check オプションでデータソースの状態を確認してください。")
        sys.exit(1)


if __name__ == "__main__":
    main()
