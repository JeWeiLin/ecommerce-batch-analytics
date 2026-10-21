"""
### 電商每日批次處理流程（ecommerce_daily_batch）

每天將當日的訂單、訂單明細、客戶、商品 CSV，經過 Bronze → Silver → Gold
三層轉換成可分析的 Delta Lake 資料表。Airflow 只負責排程與協調，
實際的資料處理由各 task 啟動的 PySpark job 完成（`python -m ecommerce.jobs.<job>`）。

#### 任務流程

`check_source_files >> ingest_bronze >> transform_silver >> build_gold >> validate_outputs`

| 順序 | Task | 負責內容 | 重試設定 |
|---|---|---|---|
| 1 | check_source_files | 確認當天 4 份 CSV 都存在、欄位齊全，並記錄筆數與 MD5 | 6 次，每 10 分鐘（等待晚到的檔案） |
| 2 | ingest_bronze | 原樣載入 CSV，加上批次日期、來源檔名等血緣欄位 | 2 次 |
| 3 | transform_silver | 轉型別、標準化、驗證、去重；無效資料送到 quarantine；無效比例超過門檻就讓批次失敗 | 不重試（資料品質問題不會自己修好） |
| 4 | build_gold | 建立 dim_customer / dim_product（SCD2）、fact_sales、每日營收與客戶回購聚合 | 2 次，且必須等前一天成功 |
| 5 | validate_outputs | 檢查主鍵唯一、SCD2 版本正確、營收對帳、筆數吻合，結果寫入 ops/dq_results | 不重試 |

#### 設計重點

* **處理日期**：使用 Airflow 的邏輯日期 `{{ ds }}`，不使用系統時間。
  排程執行、手動重跑、回補歷史日期，處理的都是同一天的資料。
* **可重跑（冪等）**：每個 Spark job 都用 replaceWhere 或 MERGE 寫入，
  清除（Clear）任一 task 或重跑整天，都不會重複計算。
* **依時間順序**：`max_active_runs=1` 加上 `build_gold` 的 `depends_on_past=True`，
  確保回補時一天接一天執行，客戶與商品的歷史版本（SCD2）才不會錯亂。
* **不自動補跑**：`catchup=False`，歷史日期請用 backfill 指令處理。

#### 常用操作

回補一段日期（在專案資料夾執行 `make backfill START=2024-01-01 END=2024-01-07`，等同於在 scheduler 容器內執行）：

    airflow dags backfill ecommerce_daily_batch -s 2024-01-01 -e 2024-01-07 --reset-dagruns -y

只重跑部分資料表：選 Trigger DAG w/ config，填入參數（留空代表全部）：

    {"silver_tables": "orders,order_items", "gold_tables": "fact_sales,agg_daily_sales"}
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Paths inside the Airflow container (project folders are mounted under /opt/airflow,
# see docker-compose.yml). Override with environment variables if needed.
PROJECT_HOME = os.getenv("ECOM_PROJECT_HOME", "/opt/airflow")
PYTHON_BIN = os.getenv("ECOM_PYTHON_BIN", "python")


def spark_job(module: str, tables_param: str = "") -> str:
    cmd = (
        f"cd {PROJECT_HOME} && {PYTHON_BIN} -m ecommerce.jobs.{module} "
        '--batch-date "{{ ds }}" --run-id "{{ run_id }}"'
    )
    if tables_param:
        cmd += ' --tables "{{ params.%s }}"' % tables_param
    return cmd


def _check_source_files(ds: str, **_) -> None:
    from ecommerce.config import load_settings
    from ecommerce.jobs.check_source_files import check_source_files

    check_source_files(load_settings(), ds)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="ecommerce_daily_batch",
    description="Orders/products/customers -> Bronze/Silver/Gold Delta Lake",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,  # use `airflow dags backfill` for historical dates
    max_active_runs=1,
    default_args=default_args,
    params={
        "silver_tables": Param("", type="string", description="Comma list; empty = all silver tables"),
        "gold_tables": Param("", type="string", description="Comma list; empty = all gold tables"),
    },
    tags=["ecommerce", "spark", "delta-lake"],
    doc_md=__doc__,
) as dag:
    check_source_files = PythonOperator(
        task_id="check_source_files",
        python_callable=_check_source_files,
        # source files may land late: keep retrying for ~1 hour
        retries=6,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=False,
    )

    ingest_bronze = BashOperator(task_id="ingest_bronze", bash_command=spark_job("ingest_bronze"))

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=spark_job("transform_silver", "silver_tables"),
        # a data-quality rejection will not fix itself; fail fast
        retries=0,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=spark_job("build_gold", "gold_tables"),
        depends_on_past=True,
    )

    validate_outputs = BashOperator(task_id="validate_outputs", bash_command=spark_job("validate_outputs"), retries=0)

    check_source_files >> ingest_bronze >> transform_silver >> build_gold >> validate_outputs
