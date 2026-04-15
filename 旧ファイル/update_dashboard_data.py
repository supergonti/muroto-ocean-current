#!/usr/bin/env python3
"""
muroto_dashboard_data.js を生成するスクリプト
============================================
HTMLダッシュボードが自動でCSVデータを読み込めるよう、
CSVの内容をJavaScript変数として書き出します。

【使い方】
  python update_dashboard_data.py

【自動化】
  main.py の末尾に以下を追加すると毎朝6時に自動更新されます:
      import subprocess
      subprocess.run(["python", str(Path(__file__).parent.parent / "update_dashboard_data.py")])
"""

import os
import sys
from pathlib import Path

# パス設定
script_dir = Path(__file__).parent
csv_path   = script_dir / "jcope_muroto" / "output" / "muroto_current_all.csv"
js_path    = script_dir / "muroto_dashboard_data.js"

def main():
    if not csv_path.exists():
        print(f"❌ CSVが見つかりません: {csv_path}")
        sys.exit(1)

    with open(csv_path, "r", encoding="utf-8") as f:
        csv_text = f.read()

    row_count = len(csv_text.strip().splitlines()) - 1  # ヘッダー除く

    # バッククォート・バックスラッシュ・テンプレートリテラルのエスケープ
    escaped = csv_text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")

    js_content = (
        "// 自動生成ファイル — update_dashboard_data.py により作成\n"
        "// このファイルを直接編集しないでください\n"
        f"// 生成日時: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"window.MUROTO_CSV_TEXT = `{escaped}`;\n"
    )

    with open(js_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"✅ muroto_dashboard_data.js を更新しました（{row_count:,} 行）")
    print(f"   保存先: {js_path}")

if __name__ == "__main__":
    main()
