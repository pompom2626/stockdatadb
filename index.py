import MySQLdb
import yfinance as yf
import numpy as np

# MySQL 데이터베이스 연결 설정
conn = MySQLdb.connect(
    user="",
    passwd="",
    host="localhost",
    db="crawl_data",
    charset="utf8"
)

# 커서 생성
cursor = conn.cursor()

# 실행할 때마다 다른 값이 나오지 않게 테이블을 제거해두기
cursor.execute("DROP TABLE IF EXISTS us_stock_daily")

# 테이블 생성하기
cursor.execute(
    """
    CREATE TABLE us_stock_daily (
        date DATE,
        open DECIMAL(20, 10),
        high DECIMAL(20, 10),
        low DECIMAL(20, 10),
        close DECIMAL(20, 10),
        adj_close DECIMAL(20, 10),
        volume BIGINT
    )
    """
)

# Yahoo Finance에서 주식 데이터 가져오기
ticker = "AAPL"  # 예: Apple Inc.
data = yf.download(ticker, start="2023-01-01", end="2023-12-31")

# NaN 값을 None으로 변환하고 데이터 타입 변환
data = data.replace({np.nan: None})
data = data.astype({
    'Open': 'float',
    'High': 'float',
    'Low': 'float',
    'Close': 'float',
    'Adj Close': 'float',
    'Volume': 'int'
})

# 데이터 확인하기
print(data.head())

# 데이터 저장하기
insert_query = (
    "INSERT INTO us_stock_daily "
    "(date, open, high, low, close, adj_close, volume) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)

for index, row in data.iterrows():
    try:
        # 각 데이터 타입 확인 및 변환
        date = index.date()
        open_price = (
            float(row['Open'])
            if row['Open'] is not None
            else None
        )
        high_price = (
            float(row['High'])
            if row['High'] is not None
            else None
        )
        low_price = (
            float(row['Low'])
            if row['Low'] is not None
            else None
        )
        close_price = (
            float(row['Close'])
            if row['Close'] is not None
            else None
        )
        adj_close = (
            float(row['Adj Close'])
            if row['Adj Close'] is not None
            else None
        )
        volume = (
            int(row['Volume'])
            if row['Volume'] is not None
            else None
        )

        print(
            f"Inserting row: {date}, {open_price}, {high_price}, "
            f"{low_price}, {close_price}, {adj_close}, {volume}"
        )

        cursor.execute(
            insert_query,
            (
                date, open_price, high_price, low_price,
                close_price, adj_close, volume
            )
        )
    except Exception as e:
        print(f"Error inserting row: {e}")

# 커밋하기
conn.commit()
# 연결 종료하기
conn.close()
