import requests
import pandas as pd

url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data"
querystring = {"symbol":"AMRN","region":"india"}

headers = {
    "x-rapidapi-key": "##################################3",
    "x-rapidapi-host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()
print(data)
historical_data = data.get('prices', [])

# Convert historical data to DataFrame
df = pd.DataFrame(historical_data)
print(df)

