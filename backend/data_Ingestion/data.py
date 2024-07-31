import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

def updateDataFunc():
    print("############################################################################################################################")
# List of company tickers and names
    companies_data = [
        {"symbol": "AAPL", "name": "Apple Inc."},
        {"symbol": "MSFT", "name": "Microsoft Corporation"},
        {"symbol": "GOOGL", "name": "Alphabet Inc."},
        {"symbol": "AMZN", "name": "Amazon.com, Inc."},
        {"symbol": "TSLA", "name": "Tesla, Inc."},
        {"symbol": "META", "name": "Meta Platforms, Inc."},
        {"symbol": "NVDA", "name": "NVIDIA Corporation"},
        {"symbol": "BRK-B", "name": "Berkshire Hathaway Inc. New"},
        {"symbol": "JPM", "name": "JP Morgan Chase & Co."},
        {"symbol": "UNH", "name": "UnitedHealth Group Incorporated"},
        {"symbol": "V", "name": "Visa Inc."},
        {"symbol": "MA", "name": "Mastercard Incorporated"},
        {"symbol": "HD", "name": "Home Depot, Inc. (The)"},
        {"symbol": "DIS", "name": "Walt Disney Company (The)"},
        {"symbol": "KO", "name": "Coca-Cola Company (The)"},
        {"symbol": "PFE", "name": "Pfizer, Inc."},
        {"symbol": "NFLX", "name": "Netflix, Inc."},
        {"symbol": "PEP", "name": "Pepsico, Inc."},
        {"symbol": "INTC", "name": "Intel Corporation"},
        {"symbol": "CSCO", "name": "Cisco Systems, Inc."},
        {"symbol": "TCS.NS", "name": "Tata Consultancy Services"},
        {"symbol": "RELIANCE.NS", "name": "Reliance Industries"},
        {"symbol": "INFY.NS", "name": "Infosys"},
        {"symbol": "HDFCBANK.NS", "name": "HDFC Bank"},
        {"symbol": "RR.L", "name": "Rolls Royce"}
    ]

    # Function to get the last available trading day data
    def get_last_available_data(symbol, start_date, end_date):
        data = yf.download(symbol, start=start_date, end=end_date, interval="1d")
        if not data.empty:
            return data.iloc[-1]  # Get the most recent entry
        return None

    # List to store the data
    data_list = []

    for company in companies_data:
        symbol = company["symbol"]
        name = company["name"]

        # Fetch stock data
        stock = yf.Ticker(symbol)
        data = stock.history(period='1d')
        
        today = data.index[-1].date()  # Extract date object
        one_month_ago = today - timedelta(days=30)
        one_year_ago = today - timedelta(days=365)

        # Fetch data for the specific dates
        today_data = get_last_available_data(symbol, today - timedelta(days=1), today + timedelta(days=1))
        month_ago_data = get_last_available_data(symbol, one_month_ago - timedelta(days=3), one_month_ago + timedelta(days=3))
        year_ago_data = get_last_available_data(symbol, one_year_ago - timedelta(days=3), one_year_ago + timedelta(days=3))

        # Ensure data is not empty
        if today_data is not None and month_ago_data is not None and year_ago_data is not None:
            # Use the last available data if exact date is not available
            today_close = today_data['Close']
            today_open = today_data['Open']
            month_ago_close = month_ago_data['Close']
            year_ago_close = year_ago_data['Close']

            # Calculate percentage changes
            change_24h = ((today_close - today_open) / today_open) * 100
            change_1m = ((today_close - month_ago_close) / month_ago_close) * 100
            change_1y = ((today_close - year_ago_close) / year_ago_close) * 100

            # Append to data list
            data_list.append([
                symbol, name, today_close, today_open,
                change_24h, change_1m, change_1y,
                month_ago_data.name.date(), year_ago_data.name.date()
            ])
        else:
            print(f"No data available for {symbol} on {today}, {one_month_ago}, or {one_year_ago}")

    # Create a DataFrame from the list
    columns = ["symbol", "company_name", "close", "open", "today_Change(%)",
            "one_month_change(%)", "one_year_change(%)", "one_month_back_date", "One_year_back_date"]
    df = pd.DataFrame(data_list, columns=columns)

    # print(type(df))

    # Database connection parameters
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

    # Insert DataFrame into PostgreSQL table
    df.to_sql('stock_data', engine, if_exists='replace', index=False)

    print("Data successfully inserted into PostgreSQL database.")