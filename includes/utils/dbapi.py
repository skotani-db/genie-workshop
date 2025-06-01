"""
Databricks API通信モジュール
Genieとの対話に必要なAPI呼び出しを管理します
"""

from typing import Any, Dict, Optional
import os
import asyncio
import httpx

# Databricks接続に必要な設定値
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_GENIE_SPACE_ID = os.environ.get("DATABRICKS_GENIE_SPACE_ID", "")

# APIエンドポイントの定義
GENIE_START_CONVERSATION_API = "/api/2.0/genie/spaces/{space_id}/start-conversation"  # 会話開始用エンドポイント
GENIE_GET_MESSAGE_API = "/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"  # メッセージ取得用エンドポイント
STATEMENT_API = "/api/2.0/sql/statements/{statement_id}"  # SQLステートメント実行結果取得用エンドポイント

async def make_databricks_request(
    method: str,
    endpoint: str,
    json_data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    DatabricksのAPIにリクエストを送信する汎用関数
    
    Args:
        method (str): HTTPメソッド('get'または'post')
        endpoint (str): APIエンドポイントのパス
        json_data (Optional[Dict[str, Any]]): POSTリクエスト用のJSONデータ
        params (Optional[Dict[str, Any]]): GETリクエスト用のクエリパラメータ
    
    Returns:
        Dict[str, Any]: APIレスポンスのJSONデータ
    """
    url = f"{DATABRICKS_HOST}{endpoint}"
    print(url)

    # 認証ヘッダーの設定
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            # HTTPメソッドに応じてリクエストを実行
            if method.lower() == "get":
                response = await client.get(url, headers=headers, params=params, timeout=30.0)
            elif method.lower() == "post":
                response = await client.post(url, headers=headers, json=json_data, timeout=30.0)
            else:
                raise ValueError(f"サポートされていないHTTPメソッド: {method}")
            
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            # HTTPエラーの詳細情報を取得して例外を発生
            error_message = f"HTTPエラー: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_message += f" - {error_detail.get('message', '')}"
            except Exception:
                pass
            raise Exception(error_message)
        except Exception as e:
            raise Exception(f"Databricks APIリクエスト中にエラーが発生: {str(e)}")

async def genie_conversation(content: str, space_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Genieとの会話を実行し、結果が得られるまで待機する
    
    Args:
        content (str): Genieに送信するメッセージ内容
        space_id (Optional[str]): Genie Space ID(未指定時は環境変数から取得)
    
    Returns:
        Dict[str, Any]: 会話の実行結果
    """
    if not space_id:
        space_id = DATABRICKS_GENIE_SPACE_ID
    
    if not space_id:
        raise ValueError("Genie Space IDが必要です。環境変数DATABRICKS_GENIE_SPACE_IDを設定するか、パラメータとして指定してください。")
    
    # 会話開始リクエストの作成
    statement_data = {
        "content": content
    }
    
    # 会話を開始し、conversation_idとmessage_idを取得
    endpoint_url = GENIE_START_CONVERSATION_API.format(space_id=space_id)
    response = await make_databricks_request("post", endpoint_url, json_data=statement_data)
    message = response.get("message")
    conversation_id = message["conversation_id"]
    message_id = message["id"]

    if not conversation_id:
        raise Exception("レスポンスからconversation_IDの取得に失敗しました")
    
    # 会話生成完了をポーリング
    max_retries = 60  # 最大リトライ回数（10秒間隔で10分）
    retry_count = 0

    while retry_count < max_retries:
        # メッセージのステータスを確認
        message_status = await make_databricks_request(
            "get", 
            GENIE_GET_MESSAGE_API.format(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
        )

        status = message_status["status"]
        
        if status == "COMPLETED":
            # 完了した場合、クエリ結果を取得
            query_result_statement_id = message_status["attachments"][0]["query"]["statement_id"]

            statement_status = await make_databricks_request(
                "get", 
                STATEMENT_API.format(statement_id=query_result_statement_id)
            )
            
            return statement_status
        elif status in ["FAILED", "CANCELED"]:
            error_message = message_status["status"]
            raise Exception(f"メッセージの取得に失敗: {error_message}")
        
        # 次のポーリングまで待機
        await asyncio.sleep(10)
        retry_count += 1
    
    return message["content"]
