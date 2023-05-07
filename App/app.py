from flask import Flask, render_template, request
import requests
import matplotlib.pyplot as plt
import pandas as pd
import json
import plotly
import plotly.express as px
import plotly.graph_objs as go
import os

project_root = os.path.dirname(__file__)
template_path = os.path.join(project_root, 'templates')
app = Flask(__name__, template_folder=template_path)

def get_transactions():
    url = "http://34.66.152.215:5000/transactions"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.content)
        
        transactions = []
        for transaction in data['transactions']:
            if '' in transaction:
                del transaction['']
            transactions.append((transaction["category"], transaction["zip"]))
        df = pd.DataFrame(transactions, columns=["category", "zip"])
        fig = px.line(df, x="category", y="zip")
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON
    else:
        print('Error in getting response from the url')
        return None

def get_fraud_chart():
    urlCount = "http://34.66.152.215:5000/transactions/metrics"
    responseCount = requests.get(urlCount)

    fraud_count = 0
    not_fraud_count = 0
    total_transactions = 0
    if responseCount.status_code == 200:
        dataCount = json.loads(responseCount.content)
        # print(dataCount)
        fraud_count = dataCount['fraudCount']
        not_fraud_count = dataCount['notFraud']
        total_transactions = dataCount['totalTransactions']

        # Create the donut chart
        labels = ['Fraud', 'Not Fraud']
        values = [fraud_count, not_fraud_count]

        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.5)])
        fig.update_layout(
            title_text=f"Fraud Transactions: {fraud_count}/{total_transactions} ({fraud_count/total_transactions*100:.2f}%)",
            annotations=[dict(text='Fraud', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        graphCount = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphCount
    else:
        print('Error in getting response from the url')
        return None

def get_category_chart():
    urlCategory = "http://34.66.152.215:5000/transactions/category/metrics"
    responseCategory = requests.get(urlCategory)

    categories = []
    fraud_counts = []
    non_fraud_counts = []
    if responseCategory.status_code == 200:
        dataCat = json.loads(responseCategory.content)
        for result in dataCat["results"]:
            category = result["_id"]
            fraud_count = result["fraudCount"]
            non_fraud_count = result["total"]
            total_count = fraud_count + non_fraud_count
            
            # Calculate the percentage of fraud and non-fraud records for the category
            fraud_percentage = fraud_count / total_count * 100
           
            
            # Add the category and percentage to the categories list
            categories.append(result["_id"])
            
            # Add the counts to the respective lists
            fraud_counts.append(fraud_count)
           

        # Create the traces for the histogram
        fraud_trace = go.Bar(
            x=categories,
            y=fraud_counts,
            name='Fraud',
            marker_color='green',
           
        )

        # Create the layout for the histogram
        layout = go.Layout(
            barmode='stack',
            title='Fraud and Non-Fraud Counts by Category',
            xaxis=dict(title='Category'),
            yaxis=dict(title='Count'),
        )

        # Combine the traces and layout into a figure
        fig = go.Figure(data=[fraud_trace], layout=layout)
        graphCat = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphCat

def get_job():
    url = "http://34.66.152.215:5000/transactions/job/metrics"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.content)
        
        profession = []
        fraudCount = []
        totalCount = []
        for result in data['results']:
            if '' in result:
                del result['']
            profession.append(result["_id"])
            fraudCount.append(result["fraudCount"])
            totalCount.append(result["totalcount"])
            
        df = pd.DataFrame({"_id": profession, "fraudCount": fraudCount})
        fig = px.line(df, x="_id", y="fraudCount")
        fig.update_layout(
            # title_text='Fraud Transactions by State',
            # geo_scope='usa',
            # autosize=True,
            xaxis_title="Profession",
            yaxis_title="fraudCount"
            
        )
        graphProfession = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphProfession
    else:
        print('Error in getting response from the url')
        return None
def get_time():
    url = "http://34.66.152.215:5000/transactions/time/metrics"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.content)
        
        fraudPercent = []
        hour = []
        for result in data['results']:
            if '' in result:
                del result['']
            fraudPercent.append(result["fraud_percentage"])
            hour.append(result["hour"])
            
        df = pd.DataFrame({"fraud_percentage": fraudPercent, "hour": hour})
        fig = px.bar(df, x="hour", y="fraud_percentage") # corrected the x and y variables
        graphTime = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphTime
    else:
        print('Error in getting response from the url')
        return None

def get_state():
    url = "http://34.66.152.215:5000/transactions"
    response = requests.get(url)
    if response.status_code == 200:
        data = json.loads(response.content)        
        transactions = []
        for transaction in data['transactions']:
            if '' in transaction:
                del transaction['']
            transactions.append((transaction["state"], int(transaction["is_fraud"])))
        df = pd.DataFrame(transactions, columns=['state', 'is_fraud'])
        fraud_df = df.groupby('state')['is_fraud'].sum().reset_index()
        # get state codes to use in the choropleth map
        state_codes = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_us_ag_exports.csv')['code']
        final_df = pd.merge(fraud_df, state_codes, left_on='state', right_on='code')
        fig = px.choropleth(final_df, 
                            locations='code', 
                            locationmode="USA-states", 
                            color='is_fraud',
                            color_continuous_scale="Viridis_r", 
                            scope="usa")
        fig.update_layout(
            # title_text='Fraud Transactions by State',
            # geo_scope='usa',
            # autosize=True,
            margin= {
                "l": 0,"r":0,"b":0,"t":0
            }
        )
        graphState = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphState
    else:
        print('Error in getting response from the url')
        return None
def get_fraud_gender():
    url = "http://34.66.152.215:5000/transactions/gender/metrics"
    response = requests.get(url)

    if response.status_code == 200:
        data = json.loads(response.content)
        
        gender = []
        for result in data['results']:
            if '' in result:
                del result['']
            gender.append((result["gender"], result["fraud_percentage"]))
        df = pd.DataFrame(gender, columns=["gender","fraud_percentage"])
        print(df)
        fig = px.bar(df, x="gender", y="fraud_percentage")
        graphGender = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphGender

@app.route('/')
def route():
    graphJSON=get_transactions()
    graphCount = get_fraud_chart()
    graphCat = get_category_chart()
    graphProfession = get_job()
    graphTime = get_time()
    graphState = get_state()
    graphGender = get_fraud_gender()
    return render_template('dashboard.html',
                            transactionsJSON=graphJSON, 
                            count=graphCount, cat=graphCat
                            ,prof=graphProfession
                            ,time=graphTime
                            ,state = graphState
                            ,gender = graphGender
                            )

if __name__ == '__main__':
     app.run(port=8080, host='0.0.0.0')

# kill $(lsof -t -i:8080) ---------to kill process at 5000
