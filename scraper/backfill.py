import os
import json
import time
import glob
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from google.oauth2.service_account import Credentials
from google.cloud import storage, bigquery
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 環境変数 ---
USER_ID = os.environ["USER_ID"]
PASSWORD = os.environ["USER_PASS"]
json_creds = json.loads(os.environ["GCP_JSON"])
TARGET_URL = os.environ["TARGET_URL"]
PARTNER_NAME = os.environ["PARTNER_NAME"]

# --- GCP設定 ---
PROJECT_ID = 'gen-lang-client-0904675126'
DATASET_ID = 'meta_ads_v2'
BUCKET_NAME = 'gen-lang-client-0904675126-backfill'

# --- コマンドライン引数 ---
DATA_TYPE = sys.argv[1]  # "mcv" or "cv"
START_DATE = sys.argv[2]
END_DATE = sys.argv[3]

TABLE_MAP = {
    "mcv": "mcv_fixed",
    "cv": "cv_fixed"
}
TABLE_ID = TABLE_MAP.get(DATA_TYPE)

def get_gcp_credentials():
    return Credentials.from_service_account_info(json_creds)

def upload_to_gcs_and_load_bq(csv_path):
    """CSVをGCSにアップロードしてBQにロード"""
    creds = get_gcp_credentials()
    
    # 1. GCSにアップロード
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob_name = f"backfill/{DATA_TYPE}_{timestamp}.csv"
    
    # Shift-JISをUTF-8に変換
    utf8_path = csv_path + ".utf8.csv"
    try:
        with open(csv_path, 'r', encoding='cp932') as f_in:
            with open(utf8_path, 'w', encoding='utf-8', newline='') as f_out:
                f_out.write(f_in.read())
    except UnicodeDecodeError:
        utf8_path = csv_path  # 既にUTF-8の場合そのまま使う
    
    print(f"GCSにアップロード中: gs://{BUCKET_NAME}/{blob_name}")
    storage_client = storage.Client(project=PROJECT_ID, credentials=creds)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(utf8_path)
    print("GCSアップロード完了")
    
    # 2. BQにロード
    print(f"BigQueryにロード中: {DATASET_ID}.{TABLE_ID}")
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=10,
    )
    
    uri = f"gs://{BUCKET_NAME}/{blob_name}"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # 完了まで待機
    
    print(f"BQロード完了: {load_job.output_rows} 行")

def run_mcv_backfill():
    """MCVバックフィル"""
    date_range_str = f"{START_DATE} - {END_DATE}"
    print(f"=== MCVバックフィル開始 (期間: {date_range_str}) ===")

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
        for i in range(60):
            files = glob.glob(os.path.join(download_dir, "*.csv"))
            if files:
                break
            time.sleep(5)

        files = glob.glob(os.path.join(download_dir, "*.csv"))
        if not files:
            print("【エラー】CSVファイルが見つかりません。")
            return

        csv_file_path = files[0]
        print(f"ダウンロード成功: {csv_file_path}")

        # 7. GCS→BQ直接ロード
        upload_to_gcs_and_load_bq(csv_file_path)

    except Exception as e:
        print(f"【エラー発生】: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

def run_cv_backfill():
    """CVバックフィル"""
    print(f"=== CVバックフィル開始 (期間: {START_DATE} - {END_DATE}) ===")

    download_dir = os.path.join(os.getcwd(), "downloads_cv")
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

        # 3. 検索条件入力
        for label_text in ["登録日時", "承認日時"]:
            try:
                full_date_str = f"{START_DATE} - {END_DATE}"
                print(f"「{label_text}」に日付を入力します: {full_date_str}")
                label_elem = wait.until(EC.presence_of_element_located((By.XPATH, f"//*[contains(text(), '{label_text}')]")))
                input_elem = label_elem.find_element(By.XPATH, "./following::input[1]")
                driver.execute_script("arguments[0].click();", input_elem)
                driver.execute_script("arguments[0].value = '';", input_elem)
                time.sleep(0.5)
                input_elem.send_keys(full_date_str)
                input_elem.send_keys(Keys.ENTER)
                time.sleep(1)
            except Exception as e:
                print(f"日付入力エラー({label_text}): {e}")

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

        # 4. 詳細項目 > クリック時リファラ
        print("詳細項目を設定します...")
        try:
            detail_btn = wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '詳細項目')]")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_btn)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", detail_btn)
            print("「詳細項目」をクリックしました")
            time.sleep(3)

            target_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='clickReferrer']")))
            target_div = target_input.find_element(By.XPATH, "./..")
            current_status = target_div.get_attribute("aria-checked")

            if current_status == "false":
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_div)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", target_div)
                print("「クリック時リファラ」をONにしました")
            else:
                print("「クリック時リファラ」は既にONのため操作しません")
            time.sleep(1)
        except Exception as e:
            print(f"詳細項目の設定でエラー: {e}")

        # 5. 検索実行
        print("検索ボタンを押します...")
        try:
            search_btns = driver.find_elements(By.XPATH, "//input[@value='検索'] | //button[contains(text(), '検索')]")
            target_search_btn = None
            for btn in search_btns:
                if btn.is_displayed():
                    target_search_btn = btn
            if target_search_btn:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_search_btn)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", target_search_btn)
                print("検索ボタンをクリックしました")
            else:
                webdriver.ActionChains(driver).send_keys(Keys.ENTER).perform()
        except Exception as e:
            print(f"検索ボタン操作エラー: {e}")
            webdriver.ActionChains(driver).send_keys(Keys.ENTER).perform()

        print("検索結果を待機中...")
        time.sleep(15)

        # 6. CSV生成
        print("CSV生成ボタンを押します...")
        try:
            csv_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@value='CSV生成' or contains(text(), 'CSV生成')]")))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", csv_btn)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", csv_btn)
            print("CSV生成ボタンをクリックしました")
        except Exception as e:
            print(f"CSVボタンエラー: {e}")
            return

        print("ダウンロード待機中...")
        time.sleep(8)
        for i in range(60):
            files = glob.glob(os.path.join(download_dir, "*.csv"))
            if files:
                break
            time.sleep(5)

        files = glob.glob(os.path.join(download_dir, "*.csv"))
        if not files:
            print("【エラー】CSVファイルが見つかりません。")
            return

        csv_file_path = files[0]
        print(f"ダウンロード成功: {csv_file_path}")

        # 7. GCS→BQ直接ロード
        upload_to_gcs_and_load_bq(csv_file_path)

    except Exception as e:
        print(f"【エラー発生】: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

def main():
    if DATA_TYPE == "mcv":
        run_mcv_backfill()
    elif DATA_TYPE == "cv":
        run_cv_backfill()
    else:
        print(f"不明なタイプ: {DATA_TYPE}")

if __name__ == "__main__":
    main()
