import time
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import schedule

def updateHistoricalData():
    
    # Define the stock symbols
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'JPM', 'UNH',
        'V', 'MA', 'HD', 'DIS', 'KO', 'PFE', 'NFLX', 'PEP', 'INTC', 'CSCO'
    ]

    # Define the period for data retrieval
    years = 1  # Number of years of data to retrieve
    end_date = datetime.today()
    start_date = end_date - timedelta(days=years*365)

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
    engine = create_engine(conn_str)

    # Function to get stock data
    def get_stock_data(symbol, start_date, end_date):
        stock = yf.Ticker(symbol)
        data = stock.history(start=start_date, end=end_date, interval='1d')
        return data

    # Function to save data to the database
    def save_to_database(symbol, data, engine):
        # Reset index to make 'Date' a column
        data.reset_index(inplace=True)
        
        # Print data for debugging
        print(f"Data for {symbol}:")
        print(data.head())  # Print first few rows of the DataFrame
        
        # Table name based on symbol
        table_name = symbol
        
        # Save data to the database
        try:
            data.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"{symbol} saved to table {table_name}")
        except Exception as e:
            print(f"Failed to save data for {symbol}: {e}")

    # Loop through each symbol to get and save data
    for symbol in symbols:
        try:
            print(f"Fetching data for {symbol}")
            data = get_stock_data(symbol, start_date, end_date)
            
            if not data.empty:
                # Save data to the database
                print(f"Saving data for {symbol} to the database")
                save_to_database(symbol, data, engine)
            else:
                print(f"No data retrieved for {symbol}")

        except Exception as e:
            print(f"Failed to fetch or save data for {symbol}: {e}")

    print("Data retrieval and saving complete.")