import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine


def updateCompnaines():
    db_params = {
        'dbname': 'finance',
        'user': 'postgres',
        'password': '12345',
        'host': '127.0.0.1',
        'port': '5432'
    }

    # Create a connection string
    conn_str = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

    # Create a SQLAlchemy engine
    engine = create_engine(conn_str)
    # List of company symbols
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'JPM', 'UNH',
        'V', 'MA', 'HD', 'DIS', 'KO', 'PFE', 'NFLX', 'PEP', 'INTC', 'CSCO'
    ]

    # Fetch company names and symbols
    data = []

    for symbol in symbols:
        stock = yf.Ticker(symbol)
        info = stock.info
        data.append({
            'symbol': symbol,
            'name': info.get('shortName', 'N/A')  # Using 'shortName' for company name
        })

    # Create DataFrame
    df = pd.DataFrame(data)

    # Display DataFrame
    # print(df)
    df.to_sql('companiesNames', engine, if_exists='replace', index=False)

    print("Data successfully inserted into PostgreSQL database.")
