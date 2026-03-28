import pandas as pd
import yfinance as yf
import time
from datetime import datetime
import os

# ================================================
# 外部ファイルの設定
# ================================================
INPUT_FILE = "_topix_list.xlsx"  # 読み込み元ファイル名（TOPIXリスト）
OUTPUT_FILE = "stock_data_results.xlsx" # 書き出し先ファイル名
# ================================================

# データ取得設定
HIST_PERIOD = "1mo"
AVG_DAYS = 20
SLEEP_TIME = 0.2 
UNIT_OKU = 100_000_000

# カラム名定義
COL_INPUT_CODE = "ティッカーコード"
COL_DATE = "年月日"
COL_INDUSTRY = "インダストリー"
COL_MARKET_CAP = "時価総額" 
COL_TRADE_VALUE = "売買代金"
COL_DIVIDEND = "1株配当"
COL_EX_DATE = "配当落ち日"
COL_PER = "PER"
COL_FORWARD_PER = "PER（予想）"
COL_PBR = "PBR"
COL_PRICE = "株価"
COL_DIV_YIELD = "配当利回り"

def fetch_data_efficiently(codes):
    """
    yfinanceを使用して株価データを一括・個別で取得する
    """
    today = datetime.now().strftime('%Y-%m-%d')
    ticker_symbols = [str(c).strip() for c in codes]
    
    print(f"--- 株価データ一括取得中 ({len(ticker_symbols)}件) ---")
    # 複数銘柄のヒストリカルデータを一括ダウンロード
    all_hist = yf.download(ticker_symbols, period=HIST_PERIOD, group_by='ticker', progress=False) 
    
    results = []
    print("--- 銘柄詳細データ取得中 ---")
    for symbol in ticker_symbols:
        code_str = symbol
        try:
            avg_trading_value = None
            # 一括取得データから該当銘柄を抽出
            ticker_hist = all_hist[symbol] if len(ticker_symbols) > 1 else all_hist
            
            if not ticker_hist.empty and len(ticker_hist) >= AVG_DAYS:
                last_n = ticker_hist.tail(AVG_DAYS)
                # 売買代金の20日間平均（億円単位）
                avg_trading_value = (last_n['Close'] * last_n['Volume']).mean() / UNIT_OKU
            
            # 個別銘柄情報の取得
            ticker_obj = yf.Ticker(symbol)
            info = ticker_obj.info            
            
            ex_date_raw = info.get("exDividendDate")
            ex_date = datetime.fromtimestamp(ex_date_raw).strftime('%Y-%m-%d') if ex_date_raw else None
            
            current_price = info.get("currentPrice")
            dividend_rate = info.get("dividendRate")
            div_yield = (dividend_rate / current_price) if (dividend_rate and current_price) else None
            
            results.append({
                COL_DATE: today, 
                COL_INPUT_CODE: code_str,
                COL_INDUSTRY: info.get("industry"),
                COL_MARKET_CAP: info.get("marketCap") / UNIT_OKU if info.get("marketCap") else None,
                COL_TRADE_VALUE: avg_trading_value,
                COL_DIVIDEND: dividend_rate, 
                COL_EX_DATE: ex_date,
                COL_PER: info.get("trailingPE"), 
                COL_FORWARD_PER: info.get("forwardPE"),
                COL_PBR: info.get("priceToBook"),
                COL_PRICE: current_price, 
                COL_DIV_YIELD: div_yield * 100 if div_yield else None
            })
            print(f"成功: {symbol}")
        except Exception as e:
            print(f"失敗: {symbol} - 理由: {e}")
            results.append({COL_DATE: today, COL_INPUT_CODE: code_str, COL_INDUSTRY: "取得失敗"})
        
        time.sleep(SLEEP_TIME)    
    return pd.DataFrame(results)

def main():
    # 1. 入力ファイルの読み込み確認
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: 入力ファイル '{INPUT_FILE}' が見つかりません。")
        return

    print(f"--- リスト読み込み開始: {INPUT_FILE} ---")
    df_input = pd.read_excel(INPUT_FILE)
    
    if COL_INPUT_CODE not in df_input.columns:
        print(f"エラー: '{INPUT_FILE}' 内に '{COL_INPUT_CODE}' カラムが見つかりません。")
        return

    codes = df_input[COL_INPUT_CODE].tolist()
    
    # 2. データ取得実行
    df_final = fetch_data_efficiently(codes)
    
    # 3. 取得失敗銘柄の再試行
    failed_codes = df_final[df_final[COL_INDUSTRY] == "取得失敗"][COL_INPUT_CODE].tolist()
    if failed_codes:
        print(f"\n--- {len(failed_codes)}件を再試行します ---")
        time.sleep(3)
        df_retry = fetch_data_efficiently(failed_codes)
        
        # 成功分と再試行分を結合
        df_success_only = df_final[df_final[COL_INDUSTRY] != "取得失敗"]
        df_final = pd.concat([df_success_only, df_retry], ignore_index=True)

    # 4. カラム順序の整理と数値の丸め処理
    cols = [COL_DATE, COL_INPUT_CODE, COL_INDUSTRY, COL_MARKET_CAP, COL_TRADE_VALUE, 
            COL_DIVIDEND, COL_EX_DATE, COL_PER, COL_FORWARD_PER, COL_PBR, COL_PRICE, COL_DIV_YIELD]
    
    df_final = df_final.reindex(columns=cols).round({
        COL_MARKET_CAP: 2, 
        COL_TRADE_VALUE: 2, 
        COL_DIV_YIELD: 4
    })
    
    # 5. Excelファイルへ書き出し
    try:
        df_final.to_excel(OUTPUT_FILE, index=False)
        print("-" * 30)
        print(f"処理完了！")
        print(f"出力ファイル: {OUTPUT_FILE}")
        print("-" * 30)
    except PermissionError:
        print(f"エラー: {OUTPUT_FILE} が開かれているため保存できません。ファイルを閉じて再試行してください。")

if __name__ == "__main__":
    main()