# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Genie Space にて生成 AI によるデータ分析の実績 (標準時間：60分)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 本ノートブックの目的：AI/BI Genie Space を用いた自然言語によるデータ分析の方法論を理解する
# MAGIC
# MAGIC Q1. Genie スペース を作成<br>
# MAGIC Q2. General Instructions 修正による Genie スペース の改善<br>
# MAGIC Q3. Example SQL Queries 追加による Genie スペース の改善 <br>
# MAGIC Q4. Trusted Assets 追加による Genie スペース の改善

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備 (標準時間：10分)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 本ノートブックで利用するスキーマを作成
schema_name = f"01_genie_workshop_for_{user_name}"
print(f"schema_name: `{schema_name}`")
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    """
)

# COMMAND ----------

# 本ノートブックで利用するテーブルの作成とデータの挿入（5 分程度で完了）
init_notebooks = [
    "./includes/01_genie_workshop/01_create_tables",
    "./includes/01_genie_workshop/02_add_constraint",
    "./includes/01_genie_workshop/03_write_data",
]
notebook_parameters = {
    "catalog_name": catalog_name,
    "schema_name": schema_name,
}
for init_n in init_notebooks:
    dbutils.notebook.run(
        init_n,
        0,
        notebook_parameters,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Genie スペース を作成 (標準時間：20分)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. 現在のノートブックの左型タブにある`Workspace (Ctrl + Alt + E)`を選択し、現在のディレクトリ（`genie-workshop`)を表示
# MAGIC 1. ケバブメニュー（`︙`）を選択し、`作成` -> `Genieスペース`を選択 *1
# MAGIC 1. `データの接続`ウィンドウにて、チームのカタログを選択し、スキーマ`.01_genie_workshop_for_<username>`のテーブル`account`、`lead`、`opportunity`、`order`、`pricebook_entry`、`product2`を追加する。
# MAGIC 1. `精度向上のためこれらの表のサンプル値を使用`のチェックボックスをオンにする。
# MAGIC 1. チャットウィンドウにて、`データセットに含まれるテーブルについて説明して`というサンプル質問が表示されるので、実行して回答が来ることを確認する。
# MAGIC 1. Genieスペースの画面右ペインの`設定`タブにて下記セルの出力結果を設定して`保存`を選択する。
# MAGIC
# MAGIC *1 Genie スペース
# MAGIC 参考リンク
# MAGIC
# MAGIC - [AI/BI Genie スペースとは](https://learn.microsoft.com/ja-jp/azure/databricks/genie/)
# MAGIC - [Use trusted assets in AI/BI Genie spaces](https://learn.microsoft.com/ja-jp/azure/databricks/genie/trusted-assets)
# MAGIC - [効果的な Genie スペースをキュレーションする](https://learn.microsoft.com/ja-jp/azure/databricks/genie/best-practices)

# COMMAND ----------

print("-- Title")
print(f" SFA Analytics by {user_name}")
print("")

print("-- Description")
print("""
# Sales Force Automation 
## 概要
このシステムは、営業活動における重要データに対して日常的な言葉で質問できるAI搭載の検索・分析ツールです。

## 対象データ
リード: 見込み客の基本情報、獲得経路、スコア、対応履歴
商談機会: 案件の進捗状況、金額、確度、担当者、予想クローズ日
注文データ: 受注情報、商品詳細、売上実績、顧客別購買履歴

## 主な機能
「今月の新規リード数は？」「確度80%以上の案件一覧を表示して」といった自然な日本語での質問が可能
複雑な条件指定も会話形式で実現（「先月比で売上が増加した顧客を教えて」など）
グラフやチャートでの視覚的なデータ表示
過去のクエリ履歴の保存と再実行

## 重要な注意事項
⚠️ システムの限界について

AI分析のため、時として不正確な回答や解釈ミスが発生する可能性があります
重要な意思決定には必ず元データの確認をお願いします
誤った回答や改善点があれば、システム内のフィードバック機能をご利用ください
継続的な学習により回答精度の向上を図っています

## 推奨利用方法
営業チームの日常的なデータ確認や傾向分析の出発点として活用し、詳細な検証は従来の管理画面と併用することをお勧めします。

""")
print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. General Instruction の追加 (標準時間：5分)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Genieスペースの画面右ペインの`コンテキスト`タブの`指示`を選択
# MAGIC 2. `General Instructions`を下記のように書き換えて`Save`を選択
# MAGIC 4. `+ New chat`を選択して`データセットに含まれるテーブルについて説明して`という質問の回答が来ることを確認
# MAGIC 5. クエリの要望通り、前回との出力結果を比較して回答が適当・簡潔になっていることを確認。
# MAGIC

# COMMAND ----------

print("-- General Instruction")
print("""
# 基本的なふるまい
- 質問には日本語で回答しなさい。

# データセットについて
- Ringo Computer Company という法人向け PC、タブレット、スマートフォンを販売している会社の Sales Force Automation に関するデータセット
- lead -> opportunity -> order という順に営業活動が進みます

# テーブル名の概要

| テーブル        | 日本語テーブル名 | 概要                                                         |
| --------------- | ---------------- | ------------------------------------------------------------ |
| lead            | リード           | 潜在顧客の情報を管理するためのオブジェクト。                 |
| opportunity     | 商談             | 商談や販売機会の情報を管理するためのオブジェクト。           |
| order           | 注文             | 顧客からの注文情報を管理するためのオブジェクト。             |
| account         | 取引先           | 取引先情報を管理するためのオブジェクト。顧客やパートナー企業などの情報を保持。 |

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. SQL クエリー追加による Genie スペース の改善 (標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. `注文日ごとの受注金額を教えてください。`という質問。
# MAGIC 1. 適切な回答がこないことを確認し、フィードバック機能で何が不足しているか指摘する。またレビューをリクエストする。
# MAGIC 1. 右上メニューのモニタリングを選択して、該当のクエリを確認する。
# MAGIC 1. 右上メニューの`設定する`を選択し、`コンテキスト`タブの`SQLクエリー`を選択する
# MAGIC 1. `追加`を選択し、下記セルの出力結果を貼り付けて、`プレビュー`で結果を確認、設定を`保存`する
# MAGIC 1. `+ 新しいチャット`を選択して`注文日ごとの受注金額を教えてください`という質問の回答が来ることを確認
# MAGIC 1. 結果に満足したら、フィードバックをすると共に、`モニタリング`タブの該当クエリを`レビュー済みとしてマーク`する

# COMMAND ----------

sql = f"""
SELECT
  CAST(ord.ActivatedDate AS DATE) AS order_date -- 注文日
  ,SUM(opp.TotalOpportunityQuantity * pbe.UnitPrice) AS total_ammount -- 受注金額

FROM
  {catalog_name}.{schema_name}.`order` ord

LEFT JOIN {catalog_name}.{schema_name}.opportunity opp
ON 
  ord.OpportunityId__c = opp.Id

LEFT JOIN {catalog_name}.{schema_name}.product2 prd
ON 
  opp.Product2Id__c = prd.Id

LEFT JOIN {catalog_name}.{schema_name}.pricebook_entry pbe
ON 
  prd.Id = pbe.Product2Id

WHERE
  StatusCode = 'Completed'
GROUP BY ALL
""".strip()
print("-- このクエリはどのような質問に答えていますか?")
print("注文日ごとの受注金額を教えてください。")
print("")
print("-- クエリ")
print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. SQL 関数追加による Genie スペース の改善 (標準時間：10分)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. `東京都と大阪府におけるパイプラインを教えてください`という質問。この質問における`pipeline`は、データエンジニアリング（Delta live Tables）のパイプラインではなく、営業活動に関連する見込み商談を指します。
# MAGIC 1. 適切な回答がこないことを確認し、フィードバック機能で何が不足しているか指摘する。またレビューをリクエストする。
# MAGIC 1. 右上メニューのモニタリングを選択して、該当のクエリを確認する。
# MAGIC 1. 右上メニューの`設定する`を選択し、`コンテキスト`タブの`SQLクエリー`を選択する
# MAGIC 1. `+ 追加`右のプルダウンから`関数`を選択し、下記セルの出力結果を貼り付けて、`プレビュー`で結果を確認、設定を`保存`する
# MAGIC 1. `+ 新しいチャット`を選択して`東京都と大阪府におけるパイプラインを教えてください`という質問の回答が来ることを確認
# MAGIC 1. 結果に満足したら、フィードバックをすると共に、`モニタリング`タブの該当クエリを`レビュー済みとしてマーク`する
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Use trusted assets in AI/BI Genie spaces](https://learn.microsoft.com/ja-jp/azure/databricks/genie/trusted-assets)
# MAGIC - [パイプライン管理とは？効果的に運用するための4ステップを解説](https://www.salesforce.com/jp/blog/jp-pipeline-management/)

# COMMAND ----------

function_name = "open_opps_in_states"
sql = f"""
CREATE
OR REPLACE FUNCTION {catalog_name}.{schema_name}.{function_name} (
  states ARRAY < STRING >
  COMMENT 'List of states.  Example: ["東京都", "大阪府", ]' DEFAULT NULL
) RETURNS TABLE
COMMENT '指定された地域におけるパイプライン（営業案件）に関する質問に対応し、該当地域のオープンな商談機会をすべてリスト表示します。地域が指定されていない場合は、全てのオープンな商談機会を返します。
質問例:

「東京駅と大阪府のパイプラインはどうですか？」
「APACのオープンな商談機会を教えて」"' RETURN
SELECT
  o.id AS `OppId`,
  a.BillingState AS `State`,
  o.name AS `Opportunity Name`,
  o.forecastcategory AS `Forecast Category`,
  o.stagename,
  o.closedate AS `Close Date`,
  o.amount AS `Opp Amount`
FROM
  {catalog_name}.{schema_name}.opportunity o
  JOIN {catalog_name}.{schema_name}.account a ON o.accountid = a.id
WHERE
  o.forecastcategory = 'Pipeline'
  AND o.stagename NOT LIKE '%closed%'
  AND (
    isnull({function_name}.states)
    OR array_contains({function_name}.states, BillingState)
  );
"""
spark.sql(sql)

print("-- Catalog")
print(catalog_name)
print()

print("-- Schema")
print(schema_name)
print()

print("-- Function")
print(function_name)
print()

print("-- データが存在する states")
states_sql = f"""
SELECT DISTINCT
  a.BillingState AS `State`
FROM
  {catalog_name}.{schema_name}.opportunity o
  JOIN {catalog_name}.{schema_name}.account a ON o.accountid = a.id
WHERE
  o.forecastcategory = 'Pipeline'
  AND o.stagename NOT LIKE '%closed%'
"""
spark.sql(states_sql).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5. Metric Views による一貫したメトリクスの活用 (標準時間：20分)

# COMMAND ----------

# MAGIC %md
# MAGIC Q4までは「受注金額」、「パイプライン」といった用語に対して、関数やクエリ例を与えることで、GenieがSales Force Automationにおけるビジネス・コンテキストを理解して回答を生成することができることを確かめました。 <br>
# MAGIC 組織内ではビジネス・メトリクスは一貫しているべきですが、このようなアプローチではGenie Roomごとに異なる手法によって「同じ」メトリクスを計算してしまう恐れがあります。<br>
# MAGIC また、テーブルに対して様々な軸で分析したい場合に、精度改善の過程で同じテーブルから異なるビューを作成することになるケースもありました。<br>
# MAGIC ![スクリーンショット 2025-06-01 14.38.23.png](static/metrics_view_1.png)
# MAGIC
# MAGIC ここで活躍するのが、Metric Views です。<br>
# MAGIC Metric Views は一般的なセマンティック・レイヤーに該当し、複数の軸（ディメンション）でビジネス・メトリクス（メジャー）を分析したい場合に非常に強力な機能になります。<br>
# MAGIC ![スクリーンショット 2025-06-01 14.41.31.png](static/metrics_view_2.png)<br>
# MAGIC このセクションでは、 Metric Views を活用した組織内の一貫したビジネス・メトリクス管理手法と Genie への応用を学びます。<br>
# MAGIC 以下のステップで、メジャー（受注金額やパイプライン）を様々な軸（日付、地域、顧客）で分析する Metrics View を作成します。
# MAGIC
# MAGIC 参考URL：[Databricksのメトリクス・ビュー](https://qiita.com/taka_yayoi/items/0b57f38c05b2c4720ed1)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 左ペインの`カタログ`から自身のカタログとスキーマに移動して、作成から`メトリクスビュー`を選択します
# MAGIC 1. ビューの名前に`sfa_metrics_view`と入力します
# MAGIC 1. メトリクスビューの作成エディタに、以下のセルの出力を貼り付けて、保存します
# MAGIC 1. カタログ上のメトリクスビューの右上メニューの`作成`からGenieスペースを選択して、`sfa_metrics_view`を追加します。
# MAGIC 1. `2024年の受注件数は？`、`2024年6月の受注件数は？`、`2024年5月1日の受注件数は？`といった受注件数に関する集計粒度の異なる質問を投げかけてみましょう
# MAGIC 1. 結果とCodeを実際に確認してみて、集計粒度が自在に制御されていることを確認します。

# COMMAND ----------

metric_view = f"""
version: 0.1

# ベーステーブルとして注文テーブルを指定
source: {catalog_name}.{schema_name}.`order`

# JOINを使用してスター・スキーマを構築
joins:
  - name: opportunity
    source: {catalog_name}.{schema_name}.opportunity
    on: source.OpportunityId__c = opportunity.Id

# ディメンション定義
dimensions:
  # 注文日
  - name: 注文日
    expr: CAST(source.ActivatedDate AS DATE)
  
  # 注文月
  - name: 注文月
    expr: DATE_TRUNC('MONTH', CAST(source.ActivatedDate AS DATE))
  
  # 注文年
  - name: 注文年
    expr: DATE_TRUNC('YEAR', CAST(source.ActivatedDate AS DATE))
  
# メジャー定義
measures:
 
  # 受注件数
  - name: 受注件数
    expr: COUNT(DISTINCT source.Id)
"""

print(metric_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge. 受注金額についての Metric Views を作成する
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。<br>
# MAGIC 受注金額を年月日および商品名で集計するメトリクスビューを作成します
# MAGIC
# MAGIC 1. カタログ上で先ほど作成したメトリクスビューを選択して、`編集`を選択してエディタを起動します
# MAGIC 1. ディメンションとして商品名を追加します
# MAGIC 1. メジャーとして受注金額を追加します
# MAGIC 1. Genieスペースで新しいチャットを起動して、商品別、年月日別の受注金額に関する質問を投げかけてみましょう

# COMMAND ----------

metric_view = f"""
version: 0.1

# ベーステーブル
source: SELECT * FROM {catalog_name}.{schema_name}.order ord
  LEFT JOIN {catalog_name}.{schema_name}.opportunity opp
  ON ord.OpportunityId__c = opp.Id
  LEFT JOIN {catalog_name}.{schema_name}.product2 prd
  ON opp.Product2Id__c = prd.Id
  LEFT JOIN {catalog_name}.{schema_name}.pricebook_entry pbe
  ON prd.Id = pbe.Product2Id

# ディメンション定義
dimensions:
  # 注文日
  - name: 注文日
    expr: CAST(ord.ActivatedDate AS DATE)
  
  # 注文月
  - name: 注文月
    expr: DATE_TRUNC('MONTH', CAST(ord.ActivatedDate AS DATE))
  
  # 注文年
  - name: 注文年
    expr: DATE_TRUNC('YEAR', CAST(ord.ActivatedDate AS DATE))

  # 商品名
  <Todo>
  
# メジャー定義
measures:
 
  # 受注件数
  - name: 受注件数
    expr: COUNT(DISTINCT ord.Id)

  # 受注金額
  <Todo>
"""

print(metric_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6. Genie Conversation APIs (標準時間：5分)

# COMMAND ----------

# MAGIC %md
# MAGIC Genie APIを使用すると、開発者は自然言語データのクエリをアプリケーション、チャットボット、およびAIエージェントフレームワークに統合できます。<br>
# MAGIC ステートフルな会話をサポートしているため、ユーザーはフォローアップの質問をしたり、時間の経過とともにデータをより自然に探索したりできます。<br>
# MAGIC Genie APIを使用すると、自然言語クエリをツールやワークフローに統合し、組織全体でデータにアクセスしやすくすることができます。<br>
# MAGIC 1. 下記セルで環境変数を設定します。右上の`設定`、`開発者`、`アクセストークン`から個人用アクセストークンを発行します。
# MAGIC 1. あらかじめ用意してある関数を使って、クエリを行います。

# COMMAND ----------

# MAGIC %pip install httpx

# COMMAND ----------

import os
os.environ["DATABRICKS_HOST"] = "https://adb-xxxxxxxxxxxxxxxxxxxx.azuredatabricks.net/"
os.environ["DATABRICKS_TOKEN"] = "dapixxxxxxxxxxxxxxxxxxxxx" # Personal Access Token
os.environ["DATABRICKS_GENIE_SPACE_ID"] = "01f03e8d5d8714d4893f98168416c9c4" # https://adb-xxxxxxxxxxxxxxxxxxxx.azuredatabricks.net//genie/rooms/<DATABRICKS_GENIE_SPACE_ID>

# COMMAND ----------

from includes.utils.databricks_formatter import format_query_results
from includes.utils.dbapi import genie_conversation
result = await genie_conversation("注文日ごとの受注金額を教えてください")
print(format_query_results(result))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge. Genie Conversation APIs でフォローアップ質問を行う
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。<br>
# MAGIC 以下のドキュメントを参考に、`includes/utils`内のモジュールを修正、または追加の関数`follow_genie_conversation`を実装、フォローアップ質問が送れるようにしてください。<br>
# MAGIC https://docs.databricks.com/aws/ja/genie/conversation-api

# COMMAND ----------

# MAGIC %md
# MAGIC ## 参考
# MAGIC
# MAGIC [Azure AI Foundry の Azure Databricks Native Connector の発表](https://www.databricks.com/blog/announcing-azure-databricks-native-connector-azure-ai-foundry)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
