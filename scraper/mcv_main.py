import os
import json
import time
import glob
import csv
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 環境変数 (Secretsから取得) ---
USER_ID = os.environ["USER_ID"]
PASSWORD = os.environ["USER_PASS"]
json_creds = json.loads(os.environ["GCP_JSON"])
TARGET_URL = os.environ["TARGET_URL"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
PARTNER_NAME = os.environ["PARTNER_NAME"]

# --- コマンドライン引数で期間モードを切り替え ---
# "today" or "yesterday"
MODE = sys.argv[1] if len(sys.argv) > 1 else "today"

SHEET_MAP = {
    "today": "mcv_today",
    "yesterday": "mcv_yesterday"
}
SHEET_NAME = SHEET_MAP.get(MODE, "mcv_today")

def get_google_service(service_name, version):
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
    return build(service_name, version, credentials=creds)

def update_google_sheet(csv_path):
    print(f"スプレッドシートへの転記を開始: {SHEET_NAME}")
    service = get_google_service('sheets', 'v4')

    csv_data = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            csv_data = list(reader)
    except UnicodeDecodeError:
        print("UTF-8失敗。Shift_JIS(CP932)で再試行します。")
        with open(csv_path, 'r', encoding='cp932') as f:
            reader = csv.reader(f)
            csv_data = list(reader)

    if not csv_data:
        print("CSVデータが空のため転記をスキップします。")
        return

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME
        ).execute()
        print("既存データをクリアしました。")
    except Exception as e:
        print(f"シートクリアエラー: {e}")

    body = {'values': csv_data}
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        print(f"スプレッドシート更新完了: {result.get('updatedCells')} セル更新")
    except Exception as e:
        print(f"書き込みエラー: {e}")

def get_date_jst(offset_days=0):
    JST = timezone(timedelta(hours=+9), 'JST')
    target = datetime.now(JST) + timedelta(days=offset_days)
    return target.strftime("%Y年%m月%d日")

def main():
    print(f"=== MCV取得処理開始 (モード: {MODE}) ===")

    if MODE == "today":
        date_str = get_date_jst(0)
    elif MODE == "yesterday":
        date_str = get_date_jst(-1)
    else:
        print(f"不明なモード: {MODE}")
        return

    date_range_str = f"{date_str} - {date_str}"
    print(f"取得期間: {date_range_str}")

    download_dir = os.path.join(os.getcwd(), "downloads_mcv")
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    try:
        # 1. ログイン
        safe_user = quote(USER_ID, safe='')
        safe_pass = quote(PASSWORD, safe='')
        url_body = TARGET_URL.replace("https://", "").replace("http://", "")
        auth_url = f"https://{safe_user}:{safe_pass}@{url_body}"

        print(f"アクセス中: {TARGET_URL}")
        driver.get(auth_url)
        time.sleep(3)
        driver.get(auth_url)
        time.sleep(5)

        # 2. 検索メニューを開く
        print("検索メニューを開きます...")
        try:
            filter_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), '絞り込み検索')]")))
            filter_btn.click()
            time.sleep(3)
        except:
            pass

        # 3. 日付入力
        try:
            print(f"日付範囲を指定します: {date_range_str}")
            date_label = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'クリック日時')]")))
            date_input = date_label.find_element(By.XPATH, "./following::input[1]")
            driver.execute_script("arguments[0].click();", date_input)
            driver.execute_script("arguments[0].value = '';", date_input)
            time.sleep(0.5)
            date_input.send_keys(date_range_str)
            date_input.send_keys(Keys.ENTER)
            print("日付を入力しました")
            time.sleep(2)
        except Exception as e:
            print(f"日付入力エラー: {e}")

        # 4. パートナー入力
        print("パートナーを入力します...")
        try:
            partner_label = driver.find_element(By.XPATH, "//div[contains(text(), 'パートナー')] | //label[contains(text(), 'パートナー')]")
            partner_target = partner_label.find_element(By.XPATH, "./following::input[contains(@placeholder, '選択')][1]")
            partner_target.click()
            time.sleep(1)
            active_elem = driver.switch_to.active_element
            active_elem.send_keys(PARTNER_NAME)
            time.sleep(3)
            active_elem.send_keys(Keys.ENTER)
            print(f"パートナー({PARTNER_NAME})を選択しました")
            time.sleep(2)
        except Exception as e:
            print(f"パートナー入力エラー: {e}")

        # 5. 検索実行
        print("検索ボタンを押します...")
        try:
            search_btns = driver.find_elements(By.XPATH, "//input[@value='検索'] | //button[contains(text(), '検索')]")
            target_search_btn = None
            for btn in search_btns:
                if btn.is_displayed():
                    target_search_btn = btn
            if target_search_btn:
                driver.execute_script("arguments[0].click();", target_search_btn)
                print("検索ボタンをクリックしました")
            else:
                webdriver.ActionChains(driver).send_keys(Keys.ENTER).perform()
        except Exception as e:
            print(f"検索ボタン操作エラー: {e}")
            webdriver.ActionChains(driver).send_keys(Keys.ENTER).perform()

        print("検索結果を待機中...")
        time.sleep(15)

        # 6. CSVダウンロード
        print("CSV作成ボタンを押します...")
        try:
            csv_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'CSV') or @value='CSV作成' or @value='CSV生成']")))
            driver.execute_script("arguments[0].click();", csv_btn)
            print("CSVボタンをクリックしました")
        except Exception as e:
            print(f"CSVボタンエラー: {e}")
            return

        time.sleep(5)
        for i in range(20):
            files = glob.glob(os.path.join(download_dir, "*.csv"))
            if files:
                break
            time.sleep(3)

        files = glob.glob(os.path.join(download_dir, "*.csv"))
        if not files:
            print("【エラー】CSVファイルが見つかりません。")
            return

        csv_file_path = files[0]
        print(f"ダウンロード成功: {csv_file_path}")

        # 7. スプレッドシートへ転記
        update_google_sheet(csv_file_path)

    except Exception as e:
        print(f"【エラー発生】: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
