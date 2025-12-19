# ./dashboards/app.py
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import sqlalchemy
import os

# === 1. Подключение к БД (ваш postgres контейнер) ===
# Измените credentials, если используете другую БД (например, sales_db)
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "sales_db",   # ← поменяйте на "sales_db", если нужно
    "user": "airflow",
    "password": "airflow",
}

engine = sqlalchemy.create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# === 2. Функция загрузки данных ===
def load_table(table_name):
    try:
        df = pd.read_sql_table(table_name, engine)
        print(f"✅ Загружена таблица {table_name}, {len(df)} строк")
        return df
    except Exception as e:
        print(f"⚠️ Ошибка загрузки {table_name}: {e}")
        return pd.DataFrame()

# Предварительная загрузка (можно сделать on-demand, но для простоты — сразу)
df_brand = load_table("sales_by_brand")
df_trends = load_table("price_trends_daily")
df_merch = load_table("merchant_competitiveness")
df_cond = load_table("product_condition_stats")

# === 3. Инициализация Dash ===
app = dash.Dash(__name__, title="Sales Analytics Dashboard")
server = app.server  # для gunicorn (если понадобится)

# === 4. Макет дэшборда ===
app.layout = html.Div([
    html.H1("📊 Sales Analytics Dashboard", style={"textAlign": "center", "marginBottom": 30}),

    # 1. ТОП брендов по количеству товаров
    html.Div([
        html.H2("🏆 ТОП-10 брендов по количеству товаров"),
        dcc.Graph(id="brand-count-chart")
    ], style={"marginBottom": 40}),

    # 2. Динамика средней цены
    html.Div([
        html.H2("📈 Динамика средней минимальной цены"),
        dcc.Graph(id="price-trend-chart")
    ], style={"marginBottom": 40}),

    # 3. ТОП мерчантов
    html.Div([
        html.H2("🏪 ТОП-10 мерчантов по количеству товаров"),
        dcc.Graph(id="merchant-chart")
    ], style={"marginBottom": 40}),

    # 4. Распределение состояний по категориям
    html.Div([
        html.H2("📦 Состояние товаров по категориям"),
        dcc.Graph(id="condition-chart")
    ]),

    # Автообновление (опционально)
    dcc.Interval(id="interval", interval=30*1000, n_intervals=0),  # каждые 5 мин
])

# === 5. Callbacks ===
@app.callback(
    Output("brand-count-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_brand_chart(n):
    if df_brand.empty:
        return px.bar(title="Нет данных")
    top10 = df_brand.nlargest(10, "product_count").reset_index()
    fig = px.bar(
        top10, x="brand", y="product_count",
        color="avg_price",
        color_continuous_scale="Blues",
        labels={"product_count": "Количество товаров", "brand": "Бренд", "avg_price": "Средняя цена"},
        title="ТОП-10 брендов"
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig

@app.callback(
    Output("price-trend-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_trend_chart(n):
    if df_trends.empty:
        return px.line(title="Нет данных")
    df = df_trends.reset_index()
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = df.sort_values("sale_date")
    fig = px.line(
        df, x="sale_date", y="avg_price",
        title="Средняя минимальная цена по дням",
        markers=True
    )
    fig.update_layout(xaxis_title="Дата", yaxis_title="Средняя цена")
    return fig

@app.callback(
    Output("merchant-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_merchant_chart(n):
    if df_merch.empty:
        return px.bar(title="Нет данных")
    top10 = df_merch.nlargest(10, "product_count")
    fig = px.bar(
        top10, x="merchant", y="product_count",
        color="product_count",
        color_continuous_scale="Viridis",
        labels={"product_count": "Количество товаров", "merchant": "Мерчант"},
        title="ТОП-10 мерчантов"
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig

@app.callback(
    Output("condition-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_condition_chart(n):
    if df_cond.empty:
        return px.bar(title="Нет данных")
    # Группируем по категориям, отображаем top condition в каждой
    df = df_cond.sort_values("count", ascending=False)
    fig = px.bar(
        df.head(15),  # первые 15 комбинаций
        x="primaryCategories",
        y="count",
        color="conditions",
        barmode="group",
        labels={"count": "Количество", "primaryCategories": "Категория", "conditions": "Состояние"},
        title="Распределение состояний (топ-15)"
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig

# === 6. Запуск ===
if __name__ == "__main__":
    # Для запуска внутри контейнера (0.0.0.0)
    app.run(host="0.0.0.0", port=8050, debug=False)