from flask import Flask, render_template
import requests
import pandas as pd
import json
import plotly.express as px

app = Flask(__name__)

@app.route('/')
def home():
    url = "http://34.66.152.215:5000/transactions"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        transactions = []
        for transaction in data:
            transactions.append((transaction[17], transaction[0]))
        df = pd.DataFrame(transactions, columns=["trans_date_trans_time", "amt"])
        fig = px.line(df, x="trans_date_trans_time", y="amt")
        fig.show()
        return render_template('dashboard.html')
    else:
        print('Error in getting response from the url')
        return render_template('dashboard.html')

if __name__ == '__main__':
    app.run()
