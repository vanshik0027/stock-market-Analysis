from sqlalchemy import create_engine, text

# Corrected database URL with the correct database name 'finance'
DATABASE_URL = 'postgresql+psycopg2://postgres:12345@127.0.0.1:5432/finance'

def test_connection():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            # Wrap the query in the text() function
            result = connection.execute(text("SELECT 1"))
            if result:
                print("Database connection successful!")
    except Exception as e:
        print(f"Database connection failed: {e}")

if __name__ == '__main__':
    test_connection()
