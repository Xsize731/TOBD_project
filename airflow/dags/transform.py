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

@task(execution_timeout=timedelta(minutes=15))
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
        ddf = dd.from_pandas(df, npartitions=4)
        print(f"✅ Загружено {len(df)} строк")

        # === 2. Обработка даты ===
        ddf = ddf.assign(
            sale_date=dd.to_datetime(
                ddf.date_seen.astype(str)
                       .str.replace(r'[\[\]"\']', '', regex=True)
                       .str.split(',').str[0]
                       .str.strip(),
                errors="coerce"
            ).dt.date
        ).dropna(subset=["sale_date"])

        # === 3. sales_by_brand ===
        sales_by_brand = ddf.groupby("brand").agg({
            "amount_min": ["min", "mean"],
            "amount_max": ["max", "mean"],
            "brand": "size",
        })
        sales_by_brand.columns = ["min_price", "avg_price", "max_price", "avg_max_price", "product_count"]
        sales_by_brand = sales_by_brand.reset_index()

        # top_condition (совместимо с pandas 1.3.5)
        cond_counts = ddf[["brand", "condition"]].dropna(subset=["condition"])
        top_condition = (
            cond_counts.groupby(["brand", "condition"])
            .size()
            .rename("cnt")
            .reset_index()
            .sort_values(["brand", "cnt"], ascending=[True, False])
            .drop_duplicates("brand")
            [["brand", "condition"]]
            .set_index("brand")["condition"]
        )
        sales_by_brand = sales_by_brand.set_index("brand").join(top_condition.rename("top_condition")).reset_index()

        # === 4. price_trends_daily ===
        price_trends = ddf.groupby("sale_date").agg({
            "amount_min": ["min", "mean", "size"],
            "amount_max": "max"
        })
        price_trends.columns = ["min_price", "avg_price", "count", "max_price"]
        price_trends = price_trends.reset_index()

        # === 5. merchant_competitiveness ===
        merchant_stats = ddf[["merchant"]].dropna(subset=["merchant"])
        merchant_stats = (
            merchant_stats.groupby("merchant")
            .size()
            .rename("product_count")
            .reset_index()
        )

        # === 6. product_condition_stats ===
        condition_stats = ddf[["primaryCategories", "condition"]].dropna(subset=["condition", "primaryCategories"])
        condition_stats = (
            condition_stats.groupby(["primaryCategories", "condition"])
            .size()
            .rename("count")
            .reset_index()
        )

        # === 7. Сохранение ===
        engine = hook.get_sqlalchemy_engine()
        print("📤 Сохраняем 4 таблицы...")
        sales_by_brand.compute().to_sql("sales_by_brand", engine, if_exists="replace", index=False)
        price_trends.compute().to_sql("price_trends_daily", engine, if_exists="replace", index=False)
        merchant_stats.compute().to_sql("merchant_competitiveness", engine, if_exists="replace", index=False)
        condition_stats.compute().to_sql("product_condition_stats", engine, if_exists="replace", index=False)

        print("✅ Успех! Все таблицы созданы.")

    finally:
        client.close()

# DAG
with DAG(
    dag_id="transform_sales_analytics",
    default_args=default_args,
    description="Dask analytics → 4 tables",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "dask"],
) as dag:
    compute_analytics()
