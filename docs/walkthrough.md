# 操作手冊：一步一步跑這個專案（Docker + Airflow）

照順序操作，每一步都會說明「要做什麼、應該看到什麼、代表什麼」。
所有 `make` 指令都在專案根目錄（`Ecommerce-Project/`）執行；實際工作都在 Docker 容器裡進行，
本機只需要 Docker Desktop。

| 階段 | 步驟 | 預估時間 |
|---|---|---|
| A. 啟動環境 | 1 – 3 | 10 分鐘（第一次含建置映像） |
| B. 用 Airflow 跑 pipeline | 4 – 8 | 15 分鐘 |
| C. 驗證四個核心功能 | 9 – 12 | 15 分鐘 |
| D. Airflow 網頁操作與關閉 | 13 – 15 | 5 分鐘 |
| E. Trino + Superset 儀表板 | 16 – 19 | 15 分鐘（第一次含下載映像） |

---

## A. 啟動環境

### 步驟 1：確認 Docker

```bash
docker --version
docker info > /dev/null && echo "Docker 正在執行"
```

打開 Docker Desktop → Settings → Resources，記憶體建議至少 **6 GB**（Spark 和 Airflow 同時執行）。

### 步驟 2：啟動 Airflow

```bash
make up
```

第一次會建置映像檔（下載 Airflow、安裝 Java 和 PySpark），約 5–10 分鐘；之後啟動只要幾十秒。
確認容器狀態：

```bash
docker compose ps -a
```

| 容器 | 正常狀態 |
|---|---|
| `postgres` | `Up (healthy)` |
| `airflow-webserver` | `Up` |
| `airflow-scheduler` | `Up` |
| `airflow-init` | `Exited (0)`：一次性的初始化容器，結束是正常的 |

瀏覽器打開 http://localhost:8080，帳號 `airflow`、密碼 `airflow`（webserver 剛啟動時可能要等 30 秒）。
DAG 列表應該出現 `ecommerce_daily_batch`。

### 步驟 3：產生模擬資料

```bash
make data DAYS=7
```

應該看到：`Generated 7 day(s) in /opt/airflow/data/landing: 2024-01-01 .. 2024-01-07`

`/opt/airflow/data` 是容器內的路徑，對應到你本機的 `data/` 資料夾，所以在本機就能打開：

```bash
ls data/landing/                          # 7 個日期資料夾
head -5 data/landing/2024-01-02/orders.csv
grep -- "-900" data/landing/2024-01-02/orders.csv    # 故意放進去的壞資料
```

`-9001` 到 `-9004` 這 4 筆訂單是故意製造的壞資料：缺客戶、日期 `2024-13-45`、狀態 `lost`、未來時間。
後面步驟會看到它們被擋下來。資料產生器用固定的亂數種子，每次產生的內容都一樣。

---

## B. 用 Airflow 跑 pipeline

### 步驟 4：只跑第一天

```bash
make backfill START=2024-01-01 END=2024-01-01
```

約 1–2 分鐘。終端機會顯示 5 個 task 依序完成。同時打開網頁：

1. 點 `ecommerce_daily_batch`
2. **Grid** 頁籤：2024-01-01 那一欄的 5 個格子依序變綠
3. 點 `transform_silver` 的格子 → **Logs**，可以看到：

```
silver.orders: in=... valid=... invalid=4 dup=...     ← invalid 就是被隔離的壞資料
```

`validate_outputs` 的日誌最後一行 `23 checks, 0 failed (error)` 代表這一天全部通過。

日誌也存在本機：`logs/dag_id=ecommerce_daily_batch/` 底下，依日期和 task 分資料夾。

### 步驟 5：看看產出了什麼

```bash
make inspect
```

會印出：

- **Row counts**：各層每張表的筆數
- **agg_daily_sales**：每天的營收、毛利、退款
- **Quarantine reasons**：被隔離的資料和原因，步驟 3 看到的 `-900x` 訂單會出現在這裡
- **Latest pipeline metrics**：每個步驟處理了幾筆、花了幾秒

資料存放在本機的 `data/lake/`：

```
data/lake/
├── bronze/      原始資料（每天一個分區）
├── silver/      清洗後
├── gold/        分析用的表
├── quarantine/  被隔離的壞資料
└── ops/         執行紀錄、檢查結果、來源檔清單
```

### 步驟 6：自己查詢任何一張表

進入容器並開啟 Python：

```bash
make shell
python
```

```python
from ecommerce.spark_session import get_spark
from ecommerce.config import load_settings

spark = get_spark()
s = load_settings()

fact = spark.read.format("delta").load(s.gold("fact_sales"))
fact.show(5)
fact.groupBy("order_status").sum("recognized_revenue").show()   # cancelled/placed 的營收是 0

spark.read.format("delta").load(s.quarantine("orders")).select("order_id", "_dq_reason").show(truncate=False)
```

離開：按 `Ctrl+D` 離開 Python，再輸入 `exit` 離開容器。

### 步驟 7：跑完剩下 6 天

```bash
make backfill START=2024-01-02 END=2024-01-07
```

約 6–10 分鐘。在 Grid 頁面可以看到 Airflow 一天接著一天執行，同一時間只跑一個日期。

### 步驟 8：再看一次結果

```bash
make inspect
```

注意 **agg_daily_sales** 裡 2024-01-01 的數字，和步驟 5 比已經變了。
因為後面幾天有 1/1 的訂單被付款、出貨或退款，1/1 的營收被重新計算。這就是「增量處理」，步驟 10 會詳細說明。

---

## C. 驗證四個核心功能

### 步驟 9：冪等性（重跑不會重複計算）

先記下目前的數字：

```bash
make inspect > /tmp/before.txt
```

重跑 1/3，再回頭重跑 1/2：

```bash
make rerun DATE=2024-01-03
make rerun DATE=2024-01-02
make inspect > /tmp/after.txt
```

比較筆數和每日營收：

```bash
diff <(sed -n '/Row counts/,/Repeat customers/p' /tmp/before.txt) \
     <(sed -n '/Row counts/,/Repeat customers/p' /tmp/after.txt) && echo "完全相同"
```

應該印出 `完全相同`。重跑多少次，筆數和金額都不會增加。

想看每張 Delta 表被寫了幾次，用步驟 6 的方式進入 Python 後執行：

```python
spark.sql(f"DESCRIBE HISTORY delta.`{s.gold('fact_sales')}`").select("version", "operation", "timestamp").show()
```

每次執行都會多一個版本（MERGE），但資料內容不變。

### 步驟 10：增量處理

看 1/7 那天 Gold 實際處理了什麼：

```bash
grep -h "fact_sales: upserted\|affected_dates" logs/dag_id=ecommerce_daily_batch/run_id=*2024-01-07*/task_id=build_gold/*.log | tail -2
```

- `fact_sales: upserted N rows`：只處理 1/7 新增或變更的訂單，不是全部訂單
- `affected_dates: [...]`：被重算的日期，包含更早的日期（因為舊訂單在 1/7 改了狀態）

### 步驟 11：SCD2 客戶歷史

`make inspect` 的 **SCD2 example** 區塊會顯示像這樣的資料：

```
C00018  Taichung  consumer  2024-01-01 00:55:35  2024-01-04 09:14:16  false   ← 舊版本
C00018  Hsinchu   consumer  2024-01-04 09:14:16  9999-12-31 00:00:00  true    ← 目前版本
```

代表這位客戶 1/4 從台中搬到新竹。在 1/4 之前下的訂單，fact_sales 裡記的城市是 Taichung。
用步驟 6 的方式驗證：

```python
fact.filter("customer_id = 'C00018'").select("order_id", "order_ts", "customer_city").orderBy("order_ts").show()
```

### 步驟 12：資料品質門檻（壞資料會被擋下）

先產生第 8 天：

```bash
make data DAYS=8
```

故意把第 8 天一半訂單的 customer_id 清空（只用到 Python 標準函式庫，Mac 內建的 `python3` 就能執行）：

```bash
python3 - <<'EOF'
import csv
p = "data/landing/2024-01-08/orders.csv"
rows = list(csv.DictReader(open(p)))
for r in rows[::2]:
    r["customer_id"] = ""
w = csv.DictWriter(open(p, "w", newline=""), fieldnames=rows[0].keys())
w.writeheader(); w.writerows(rows)
print("已破壞", len(rows[::2]), "筆")
EOF
```

執行第 8 天：

```bash
make rerun DATE=2024-01-08
```

應該失敗：Grid 頁面 2024-01-08 的 `transform_silver` 變紅，後面的 task 不會執行。
點紅色格子 → **Logs**，可以看到：

```
DataQualityError: [orders] invalid ratio 50.xx% ... exceeds threshold 5.00%; batch rejected
```

確認壞資料沒有進入 Silver 和 Gold：

```bash
make inspect    # Quarantine reasons 出現大量 customer_id_missing；Latest pipeline metrics 有一筆 status=failed
```

修正來源檔（重新產生），再重跑：

```bash
make data DAYS=8
make rerun DATE=2024-01-08    # 這次全部變綠
```

---

## D. Airflow 網頁操作與關閉

### 步驟 13：在網頁上重跑

- **重跑單一 task**：點某天的 `build_gold` 格子，選 **Clear**。它和下游的 `validate_outputs` 會重新執行，結果不變（冪等）。
- **只重跑部分資料表**：右上角 ▶ 選 **Trigger DAG w/ config**，填入
  `{"silver_tables": "orders,order_items", "gold_tables": "fact_sales,agg_daily_sales"}`

### 步驟 14：開啟每日排程

```bash
make unpause
```

也可以在網頁上把 DAG 左側的開關打開。開啟後 Airflow 會在每天 UTC 午夜處理前一天的資料
（需要 `data/landing/<日期>/` 已經有那天的 CSV，否則 `check_source_files` 會重試約 1 小時後失敗）。

### 步驟 15：關閉

```bash
make down           # 停止所有容器（包含 E 階段的 Trino、Superset），執行紀錄會保留
```

想繼續做 E 階段（儀表板），先不要關閉，或之後再 `make up` 啟動即可。

想清除 Airflow 的所有執行紀錄、從頭開始：

```bash
make reset          # 停止容器並刪除 Postgres 資料
make clean          # 刪除處理後的資料（data/lake），保留 CSV
```

---

## E. Trino + Superset 儀表板

這一段讓分析人員用 SQL 和儀表板查看 Gold 表。需要先完成 B 階段（Gold 表已經建好），Airflow 不必開著。

### 步驟 16：啟動分析層

```bash
make analytics
```

第一次會下載 Trino、Superset 映像並建置（約 5–10 分鐘）。最後應該看到：

```
added  gold.fact_sales
...
chart   9  Quarantined rows per batch (data quality)
dashboard ready: http://localhost:8088/superset/dashboard/ecommerce-sales/
```

`added` 代表 Trino 已經註冊這張 Delta 表；再次執行時會顯示 `exists`。

### 步驟 17：看儀表板

打開 http://localhost:8088/superset/dashboard/ecommerce-sales/ ，帳號 `admin`、密碼 `admin`。

| 區塊 | 對應前面的哪個功能 |
|---|---|
| Net revenue、Daily revenue | 只計入 paid、shipped、delivered 訂單的營收 |
| Revenue by customer segment (at order time) | SCD2：用下單當時的客戶分群，不是現在的分群 |
| Quarantined rows per batch | 資料品質：每天被隔離的壞資料筆數，1/1 最多（含 2 筆無效客戶、1 筆無效商品） |

Top 10 products 右上角的黃色 ⚠ 是「已達列數上限（10 筆）」的提示，不是錯誤。

### 步驟 18：用 SQL 查詢

Superset 上方選單 **SQL → SQL Lab**，資料庫選 `Trino Lakehouse`，貼上 `analytics/sql/` 裡任一個查詢執行，例如 `04_scd2_history.sql`：
比較「下單當時的分群」和「現在的分群」，就能看出 SCD2 的差別。

也可以直接用 Trino 的命令列：

```bash
docker compose exec trino trino --catalog delta
```

```sql
SELECT order_date, SUM(net_revenue) FROM gold.agg_daily_sales GROUP BY 1 ORDER BY 1;
```

輸入 `quit` 離開。

### 步驟 19：處理新資料後更新儀表板

例如在步驟 12 處理了第 8 天，或用 Airflow 補跑了更多日期之後：

```bash
make analytics-refresh
```

Trino 讀的是 Delta 表的最新版本，所以已註冊的表會自動看到新資料；這個指令只是註冊新出現的表、並重建儀表板。
重新整理儀表板頁面就會看到新的日期。

結束時：`make analytics-down` 只停止 Trino 和 Superset；`make down` 停止全部。

---

## 常用指令速查

| 想做的事 | 指令 |
|---|---|
| 啟動 / 停止 | `make up` / `make down` |
| 產生資料 | `make data DAYS=7` |
| 處理一段日期 | `make backfill START=2024-01-01 END=2024-01-07` |
| 重跑某一天 | `make rerun DATE=2024-01-03` |
| 看結果 | `make inspect` |
| 單元測試（約 1 分鐘） | `make test-fast` |
| 全部測試（約 6 分鐘） | `make test` |
| 啟動 Trino + Superset 並建立儀表板 | `make analytics` |
| 處理新資料後更新儀表板 | `make analytics-refresh` |
| 查看 scheduler 日誌 | `make logs` |
| 進入容器 | `make shell` |
| 只執行某個步驟 | `docker compose exec airflow-scheduler python -m ecommerce.jobs.build_gold --batch-date 2024-01-03` |
| 全部重來 | `make reset`、`rm -rf data` 後從步驟 2 開始 |

## 常見問題

| 狀況 | 處理方式 |
|---|---|
| `service "airflow-scheduler" is not running` | 先執行 `make up` |
| 網頁打不開 | webserver 啟動需要約 30 秒；用 `docker compose ps -a` 確認狀態 |
| 容器一直 `Restarting` | `docker compose logs airflow-init` 查看初始化錯誤 |
| task 失敗、日誌出現 `Killed` 或 exit code 137 | Docker 記憶體不足，到 Docker Desktop 調高到 6 GB 以上 |
| `Source check failed ... missing` | 那一天的 CSV 不存在，先執行 `make data DAYS=<天數>` |
| `DataQualityError` | 壞資料比例超過門檻，查看 `make inspect` 的 Quarantine reasons |
| Superset 顯示 `Table not found` | Gold 表還沒建好，先跑 B 階段，再執行 `make analytics-refresh` |
| 執行 `make clean` 後儀表板沒有資料 | 重新處理資料後執行 `make analytics-refresh` |
| 8080 port 被占用 | 關掉占用 8080 的程式，或執行 `make down` 關掉舊的 Airflow |
