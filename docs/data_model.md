# 資料模型、品質規則與指標定義

## 1. 來源資料（Landing，每日增量 CSV）

路徑：`data/landing/<YYYY-MM-DD>/<table>.csv`

| 檔案 | 欄位 | 主鍵 | 每日內容 |
|---|---|---|---|
| orders.csv | order_id, customer_id, order_ts, status, updated_at | order_id | 新訂單 + 舊訂單狀態變更 |
| order_items.csv | order_id, product_id, quantity, unit_price, discount_amount | (order_id, product_id) | 新訂單的明細 |
| customers.csv | customer_id, customer_name, email, city, segment, updated_at | customer_id | 新客戶 + 屬性變更 |
| products.csv | product_id, product_name, category, unit_cost, list_price, updated_at | product_id | 新商品 + 價格/成本變更 |

> 與原始規劃的差異：orders 使用 `order_ts`（下單時間）而不是只有 `order_date`，
> 這樣才能和 SCD2 維度做「下單當下」的 point-in-time join；`order_date` 在 Silver 由 `order_ts` 推導。
> 另外加上 `updated_at` 讓訂單狀態變更可以被增量處理。

訂單狀態流程：`placed → paid → shipped → delivered → refunded`，`placed → cancelled`。

## 2. 分層

| 層 | 路徑 | 寫入方式 | 冪等機制 |
|---|---|---|---|
| Bronze | lake/bronze/<table> | 原始字串 + `_batch_date, _batch_id, _run_id, _source_file, _ingestion_ts, _row_hash`，依 `_batch_date` 分區 | `replaceWhere _batch_date = ds` |
| Quarantine | lake/quarantine/<table> | 無效資料的原始值 + `_dq_reason` | `replaceWhere _batch_date = ds` |
| Silver products / customers | lake/silver/... | 清洗後的變更紀錄，依 `_batch_date` 分區（SCD2 的輸入） | `replaceWhere _batch_date = ds` |
| Silver orders | lake/silver/orders | 目前狀態表，MERGE on order_id | 只接受 `updated_at` 較新（或相同）的版本 |
| Silver order_items | lake/silver/order_items | MERGE on (order_id, product_id) | MERGE |
| Gold dim_customer / dim_product | lake/gold/... | SCD2 + SCD1 | row hash 比對 + 舊資料不回寫 + 決定性 surrogate key |
| Gold fact_sales | lake/gold/fact_sales | MERGE on (order_id, product_id)，只處理本批次受影響訂單 | MERGE |
| Gold agg_daily_sales | lake/gold/agg_daily_sales | 只重算受影響日期 | `replaceWhere order_date IN (...)` |
| Gold agg_customer_orders | lake/gold/agg_customer_orders | 只重算受影響客戶 | MERGE on customer_id |
| Ops | lake/ops/pipeline_metrics, dq_results, manifests | append（保留每次執行紀錄） | 以 run_id 區分 |

## 3. Silver 資料品質規則

一筆資料可同時違反多條規則，`_dq_reason` 會以 `;` 串接。

| 表 | 規則 | 說明 |
|---|---|---|
| orders | order_id_missing, customer_id_missing | 必填 |
| | order_ts_invalid / updated_at_invalid | 無法解析為 `yyyy-MM-dd HH:mm:ss` |
| | order_ts_in_future / updated_at_in_future | 晚於批次日結束時間 |
| | status_invalid | 不在允許狀態中 |
| | updated_before_order_ts | updated_at 早於下單時間 |
| order_items | quantity_invalid / quantity_not_positive | 非整數 / ≤ 0 |
| | unit_price_invalid, discount_invalid, discount_exceeds_gross | 金額檢查 |
| | orphan_order_id, unknown_product_id | 參照完整性（訂單不存在或被隔離時，明細也一起隔離） |
| customers | customer_id_missing, city_missing, segment_invalid, email_invalid, updated_at_* | |
| products | product_id_missing, category_missing, unit_cost_invalid, list_price_invalid, updated_at_* | |

**重複資料**：相同主鍵保留 `updated_at` 最新的一筆（同時間再依 `_ingestion_ts`、`_row_hash` 排序，結果具決定性）。重複筆數記錄於 metrics 的 `duplicate_rows`，不進入 quarantine。

**品質門檻**：每張表無效比例超過門檻（orders / order_items 5%、customers / products 10%，可用 `ECOM_DQ_MAX_INVALID_RATIO` 覆寫）時，該批次失敗；quarantine 會先寫入以便排查，但 Silver 不會被更新。

**標準化**：字串 trim、空字串轉 NULL；city 轉 Title Case 並合併多餘空白；segment 轉小寫底線（`Small Business → small_business`）；email 轉小寫。

## 4. Gold 指標定義（統計口徑）

| 指標 | 定義 |
|---|---|
| gross_amount | quantity × unit_price |
| net_amount | gross_amount − discount_amount |
| **營收認列狀態** | `paid`, `shipped`, `delivered` |
| recognized_revenue | 認列狀態：net_amount；其他狀態（placed / cancelled / refunded）：0 |
| cost_amount | quantity × unit_cost（**下單時點**的成本，來自 dim_product SCD2） |
| gross_profit | 認列狀態：recognized_revenue − cost_amount；其他：0 |
| refunded_amount | status = refunded 時的 net_amount |
| order_count（agg） | 該日、該類別中，含有該類別商品的認列訂單數（distinct） |
| avg_order_value（agg） | net_revenue / order_count（類別歸屬 AOV；全站 AOV 請由 fact_sales 計算，見 analytics/sql/01） |
| is_repeat_customer | 認列訂單數 ≥ 2 |

**退款處理**：訂單轉為 refunded 時，該訂單在原下單日的營收會被沖回（重算該日 agg），並記入 refunded_amount。
**取消處理**：cancelled 訂單不認列營收，計入 cancelled_order_count。

## 5. 維度歷史（SCD）

| 維度 | SCD2 欄位（保留歷史） | SCD1 欄位（直接覆寫） |
|---|---|---|
| dim_customer | city, segment | customer_name, email |
| dim_product | category, unit_cost, list_price | product_name |

- `effective_from` = 來源 `updated_at`；`effective_to` = 下一版本的 effective_from，目前版本為 `9999-12-31 00:00:00`。
- 版本區間為左閉右開 `[effective_from, effective_to)`。
- `fact_sales` 以 `order_ts` 做 point-in-time join；若找不到對應版本（維度晚到），退回目前版本；完全找不到時 sk = −1。
- 同一批次內同一客戶有多次變更時，只保留最後一次（日批次粒度）。
- 回補舊日期時，比目前版本更舊的變更會被忽略，不會讓維度倒退。

## 6. 輸出驗證（validate_outputs）

| 檢查 | 嚴重度 |
|---|---|
| fact_sales、agg_daily_sales 主鍵唯一 | error |
| SCD2：每個 key 恰好一個 current、區間有效、版本連續 | error |
| fact 認列營收總和 = agg net_revenue 總和 | error |
| 無負營收 / 非正數量；非認列狀態營收為 0 | error |
| Bronze 筆數 = Silver 有效 + 隔離 + 重複；quarantine 筆數 = metrics | error |
| 批次有訂單、未知維度比例 ≤ 1% | warn |
