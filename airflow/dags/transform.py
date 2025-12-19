from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="distributed")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@task(execution_timeout=timedelta(minutes=10))
def compute_analytics():
    client = Client("dask-scheduler:8786")
    try:
        # === 1. Загрузка ===
        hook = PostgresHook(postgres_conn_id="sales_db_conn")
        df = hook.get_pandas_df(sql="""
            SELECT
                "brand",
                "primaryCategories",
                "prices.merchant" AS merchant,
                "prices.condition" AS condition,
                "prices.amountMin" AS amount_min,
                "prices.amountMax" AS amount_max,
                "prices.dateSeen" AS date_seen
            FROM sales_raw
            WHERE "prices.amountMin" IS NOT NULL
        """)
        print(f"✅ Загружено {len(df)} строк")

        # === 2. Обработка даты (в pandas) ===
        df["sale_date"] = pd.to_datetime(
            df["date_seen"].astype(str)
               .str.replace(r'[\[\]"\']', '', regex=True)
               .str.split(',').str[0]
               .str.strip(),
            errors="coerce"
        ).dt.date
        df = df.dropna(subset=["sale_date"])

        # === 3. sales_by_brand (в pandas) ===
        sales_by_brand = df.groupby("brand").agg({
            "amount_min": ["min", "mean"],
            "amount_max": ["max", "mean"],
            "brand": "size",
        })
        sales_by_brand.columns = ["min_price", "avg_price", "max_price", "avg_max_price", "product_count"]
        sales_by_brand = sales_by_brand.reset_index()

        # top_condition (в pandas — без Dask!)
        cond_df = df[["brand", "condition"]].dropna(subset=["condition"])
        top_condition_series = (
            cond_df.groupby("brand")["condition"]
            .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else None)
            .rename("top_condition")
        )
        sales_by_brand = sales_by_brand.merge(
            top_condition_series,
            left_on="brand",
            right_index=True,
            how="left"
        )

        # === 4. price_trends_daily ===
        price_trends = df.groupby("sale_date").agg({
            "amount_min": ["min", "mean", "size"],
            "amount_max": "max"
        })
        price_trends.columns = ["min_price", "avg_price", "count", "max_price"]
        price_trends = price_trends.reset_index()

        # === 5. merchant_competitiveness ===
        merchant_stats = df["merchant"].dropna().value_counts().reset_index()
        merchant_stats.columns = ["merchant", "product_count"]

        # === 6. product_condition_stats ===
        condition_stats = df[["primaryCategories", "condition"]].dropna()
        condition_stats = condition_stats.groupby(["primaryCategories", "condition"]).size().reset_index()
        condition_stats.columns = ["primaryCategories", "condition", "count"]

        # === 7. Сохранение ===
        engine = hook.get_sqlalchemy_engine()
        print("📤 Сохраняем...")
        sales_by_brand.to_sql("sales_by_brand", engine, if_exists="replace", index=False)
        price_trends.to_sql("price_trends_daily", engine, if_exists="replace", index=False)
        merchant_stats.to_sql("merchant_competitiveness", engine, if_exists="replace", index=False)
        condition_stats.to_sql("product_condition_stats", engine, if_exists="replace", index=False)

        print("✅ Успех! Все 4 таблицы созданы.")

    finally:
        client.close()

# DAG
with DAG(
    dag_id="transform_sales_analytics",
    default_args=default_args,
    description="Analytics → 4 tables (pandas-only for reliability)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics"],
) as dag:
    compute_analytics()