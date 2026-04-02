#!/usr/bin/env python3
"""
Notion抽出条件 → BigQuery → 好調クリエイティブDB 自動投稿スクリプト
"""
import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ===== 設定 =====
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
EXTRACTION_DB_ID = "335f6a186ac7805987b1f481313f5d92"
CREATIVE_DB_ID = "335f6a186ac780daa57ed0ea0bba9be4"
GCP_PROJECT = "gen-lang-client-0904675126"
BQ_VIEW = f"{GCP_PROJECT}.meta_ads_v2.v_integrated_report"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def get_bq_client():
    """GCP認証してBQクライアントを返す"""
    gcp_json = os.environ.get("GCP_JSON")
    if gcp_json:
        info = json.loads(gcp_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=GCP_PROJECT, credentials=creds, location="asia-northeast1")
    return bigquery.Client(project=GCP_PROJECT, location="asia-northeast1")

def fetch_extraction_conditions():
    """Notionの抽出条件DBからステータス=実行中のレコードを取得"""
    url = f"https://api.notion.com/v1/databases/{EXTRACTION_DB_ID}/query"
    payload = {
        "filter": {
            "property": "ステータス",
            "select": {"equals": "待機中"}
        }
    }
    resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
    resp.raise_for_status()
    results = []
    for page in resp.json().get("results", []):
        p = page["properties"]
        title_arr = p.get("Name", {}).get("title", [])
        name = title_arr[0]["plain_text"] if title_arr else ""
        
        date_start = p.get("期間_開始", {}).get("date")
        date_end = p.get("期間_終了", {}).get("date")
        account_sel = p.get("アカウント名", {}).get("select")
        
        results.append({
            "page_id": page["id"],
            "name": name,
            "cpa_limit": p.get("CPA上限", {}).get("number"),
            "cost_min": p.get("コスト下限", {}).get("number"),
            "date_start": date_start["start"] if date_start else None,
            "date_end": date_end["start"] if date_end else None,
            "account_name": account_sel["name"] if account_sel else None,
        })
    return results

def query_bq(condition):
    """BQから条件に合う広告を取得"""
    client = get_bq_client()
    
    where_clauses = ["spend > 0", "approved_count > 0"]
    if condition["date_start"] and condition["date_end"]:
        where_clauses.append(f"date_start BETWEEN '{condition['date_start']}' AND '{condition['date_end']}'")
    if condition["account_name"]:
        where_clauses.append(f"account_name = '{condition['account_name']}'")
    
    having_clauses = []
    if condition["cost_min"]:
        having_clauses.append(f"SUM(spend) >= {condition['cost_min']}")
    if condition["cpa_limit"]:
        having_clauses.append(f"SAFE_DIVIDE(SUM(spend), SUM(approved_count)) <= {condition['cpa_limit']}")
    
    where_sql = " AND ".join(where_clauses)
    having_sql = f"HAVING {' AND '.join(having_clauses)}" if having_clauses else ""
    
    query = f"""
    SELECT
      ad_name,
      url_key,
      COALESCE(ANY_VALUE(account_name), '') AS account_name,
      COALESCE(ANY_VALUE(campaign_name), '') AS campaign_name,
      COALESCE(ANY_VALUE(adset_name), '') AS adset_name,
      COALESCE(ANY_VALUE(image_url), '') AS image_url,
      COALESCE(ANY_VALUE(landing_url), '') AS landing_url,
      SUM(impressions) AS total_imp,
      SUM(spend) AS total_spend,
      SUM(meta_clicks) AS total_clicks,
      SUM(mcv_clicks) AS total_mcv,
      SUM(approved_count) AS total_cv,
      SAFE_DIVIDE(SUM(spend), SUM(approved_count)) AS cpa,
      SAFE_DIVIDE(SUM(mcv_clicks), SUM(meta_clicks)) AS mcvr,
      SAFE_DIVIDE(SUM(approved_count), SUM(mcv_clicks)) AS cvr,
      SAFE_DIVIDE(SUM(meta_clicks), SUM(impressions)) AS ctr
    FROM `{BQ_VIEW}`
    WHERE {where_sql}
    GROUP BY ad_name, url_key
    {having_sql}
    ORDER BY total_spend DESC
    LIMIT 50
    """
    print(f"  BQクエリ実行中...")
    rows = list(client.query(query).result())
    print(f"  {len(rows)}件の広告が条件に合致")
    return rows

def post_to_creative_db(rows, condition_name):
    """好調クリエイティブDBに投稿"""
    url = "https://api.notion.com/v1/pages"
    count = 0
    for row in rows:
        payload = {
            "parent": {"database_id": CREATIVE_DB_ID},
            "properties": {
                "Name": {
                    "title": [{"text": {"content": str(row.ad_name or "")}}]
                },
                "アカウント名": {
                    "rich_text": [{"text": {"content": str(row.account_name or "")}}]
                },
                "キャンペーン": {
                    "rich_text": [{"text": {"content": str(row.campaign_name or "")}}]
                },
                "広告セット": {
                    "rich_text": [{"text": {"content": str(row.adset_name or "")}}]
                },
                "IMP": {"number": int(row.total_imp or 0)},
                "コスト": {"number": round(float(row.total_spend or 0), 0)},
                "クリック": {"number": int(row.total_clicks or 0)},
                "MCV": {"number": int(row.total_mcv or 0)},
                "CV": {"number": int(row.total_cv or 0)},
                "CPA": {"number": round(float(row.cpa or 0), 0)},
                "MCVR(%)": {"number": round(float(row.mcvr or 0), 4)},
                "CVR(%)": {"number": round(float(row.cvr or 0), 4)},
                "CTR(%)": {"number": round(float(row.ctr or 0), 4)},
            }
        }
        # 画像URLがあれば追加
        if row.image_url:
            payload["properties"]["画像URL"] = {"url": str(row.image_url)}
        # LP URLがあれば追加
        if row.landing_url:
            payload["properties"]["LP URL"] = {"url": str(row.landing_url)}
        
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if resp.status_code == 200:
            count += 1
        else:
            print(f"  投稿エラー: {resp.status_code} - {resp.text[:200]}")
    print(f"  {count}件を好調クリエイティブDBに投稿完了")
    return count

def update_status(page_id, status_name):
    """抽出条件のステータスを更新"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "ステータス": {
                "select": {"name": status_name}
            }
        }
    }
    resp = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    resp.raise_for_status()
    print(f"  ステータスを「{status_name}」に更新")

def main():
    print("=== Notion抽出条件 → BQ → 好調クリエイティブ 自動投稿 ===")
    
    conditions = fetch_extraction_conditions()
    print(f"実行中の抽出条件: {len(conditions)}件")
    
    if not conditions:
        print("実行対象なし。終了。")
        return
    
    for cond in conditions:
        print(f"\n--- 処理中: {cond['name']} ---")
        print(f"  期間: {cond['date_start']} ～ {cond['date_end']}")
        print(f"  CPA上限: {cond['cpa_limit']}, コスト下限: {cond['cost_min']}")
        print(f"  アカウント: {cond['account_name'] or '全アカウント'}")
        
        try:
            update_status(cond["page_id"], "実行中")
            rows = query_bq(cond)
            if rows:
                posted = post_to_creative_db(rows, cond["name"])
                update_status(cond["page_id"], "完了")
                print(f"  完了: {posted}件投稿")
            else:
                update_status(cond["page_id"], "完了")
                print(f"  条件に合う広告なし（0件で完了）")
        except Exception as e:
            print(f"  エラー: {e}")
            update_status(cond["page_id"], "エラー")
    
    print("\n=== 全処理完了 ===")

if __name__ == "__main__":
    main()
