import streamlit as st
import plotly.express as px
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False
    

def get_last_fraud():
    DB_HOST = os.getenv("DB_HOST", "db")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "ml_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "ml_pass")
    DB_NAME = os.getenv("DB_NAME", "ml_scores")

    conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
    cur = conn.cursor()

    query_for_last_transactions = f"""
            SELECT *
            FROM transaction_scores
            WHERE predicted_label = '1'
            ORDER BY created_at DESC
            LIMIT 10;
        """
    
    query_for_plot = f"""
            SELECT created_at, score
            FROM transaction_scores
            ORDER BY created_at DESC
            LIMIT 100
    """

    transactions_df = pd.read_sql(query_for_last_transactions, conn)
    scores_df = pd.read_sql(query_for_plot, conn)
    conn.close()
    return transactions_df, scores_df

def execute_sql(query):

    if not query.strip().upper().startswith("SELECT"):
        msg = "Поддерживаются только SELECT-запросы"
        return msg, pd.DataFrame()

    DB_HOST = os.getenv("DB_HOST", "db")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "ml_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "ml_pass")
    DB_NAME = os.getenv("DB_NAME", "ml_scores")

    with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        ) as conn:
            df = pd.read_sql(query, conn)
    msg = "Запрос выполнен успешно"

    return msg, df

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.subheader("Результаты скоринга")
if st.button("Посмотреть результаты"):
    try:
        with st.spinner("Загрузка данных из базы"):
            transactions_df, scores_df = get_last_fraud()
    except Exception as e:
        st.error(f"Ошибка при получении данных: {e}")
        st.stop()

    if transactions_df.empty and scores_df.empty:
        st.warning("Нет данных, сначала загрузите транзакции и дождитесь обработки.")

    else:
        st.subheader("Последние мошеннические транзакции")
        transactions_df = transactions_df.sort_values(by='created_at', ascending=False)
        transactions_df["created_at"] = transactions_df["created_at"].dt.tz_localize("UTC").dt.tz_convert("Europe/Moscow")
        st.dataframe(transactions_df[['transaction_id', 'score', 'created_at']])

        st.subheader("Распределение скоров последних транзакций")
        fig = px.histogram(
        scores_df,
        x='score',
        nbins=20,
        )
        fig.update_layout(
            xaxis_title="Score",
            yaxis_title="Количество транзакций",
            bargap=0.1,
        )
        st.plotly_chart(fig, use_container_width=True)


st.subheader("SQL консоль")
query = st.text_area("Введите SQL-запрос", height=150, placeholder="Поддерживаются только SELECT-запросы")
if st.button("Выполнить запрос"):
    try:
        msg, df = execute_sql(query)
        if msg == "Запрос выполнен успешно":
            st.success(msg)
            df["created_at"] = df["created_at"].dt.tz_localize("UTC").dt.tz_convert("Europe/Moscow")
            st.dataframe(df)
        else:
            st.error(msg)
    except Exception as e:
        st.error(f"Ошибка при выполнении: {e}")
