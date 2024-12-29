from flask import Flask, render_template, request
import mysql.connector
import locale
import pandas as pd
import ta  # Technical Analysis Library

app = Flask(__name__)

# Set the locale to Macedonian (Macedonia) to format numbers correctly
locale.setlocale(locale.LC_ALL, 'mk_MK.UTF-8')

# Define the function to format numbers
def format_macedonian_number(value):
    if value is not None:
        return locale.format_string('%.2f', value, grouping=True).replace(",", "X").replace(".", ",").replace("X", ".")
    return "0,00"

app.jinja_env.filters['macedonian_format'] = format_macedonian_number

# Database connection setup
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="lajnagomna123",
        database="stock_data"
    )

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Fetch all issuer codes for the dropdown
    cursor.execute("SELECT DISTINCT issuer_code FROM stock_data")
    issuer_codes = [row["issuer_code"] for row in cursor.fetchall()]

    conn.close()

    return render_template('index.html', issuer_codes=issuer_codes)

@app.route('/data', methods=['POST'])
def display_data():
    issuer_code = request.form['issuer_code']
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Fetch the data for the selected issuer code
    cursor.execute("SELECT * FROM stock_data WHERE issuer_code = %s", (issuer_code,))
    stock_data = cursor.fetchall()

    # Convert data to pandas DataFrame for analysis
    df = pd.DataFrame(stock_data)

    # Calculate Technical Indicators using the 'ta' library
    df['SMA_20'] = ta.trend.sma_indicator(df['avg_price'], window=20)
    df['EMA_20'] = ta.trend.ema_indicator(df['avg_price'], window=20)
    df['RSI'] = ta.momentum.rsi(df['avg_price'], window=14)
    df['MACD'] = ta.trend.macd(df['avg_price'])
    df['Stochastic'] = ta.momentum.stoch(df['max_price'], df['min_price'], df['avg_price'])

    # Add buy/sell/hold signals based on the indicators
    df['SMA_Signal'] = [
        'Buy' if row['SMA_20'] > row['avg_price'] else 'Sell'
        for _, row in df.iterrows()
    ]
    df['EMA_Signal'] = df.apply(lambda row: 'Buy' if row['EMA_20'] > row['avg_price'] else 'Sell', axis=1)
    df['RSI_Signal'] = df['RSI'].apply(lambda x: 'Buy' if x < 30 else 'Sell' if x > 70 else 'Hold')
    df['MACD_Signal'] = df['MACD'].apply(lambda x: 'Buy' if x > 0 else 'Sell')
    df['Stochastic_Signal'] = df['Stochastic'].apply(lambda x: 'Buy' if x > 80 else 'Sell' if x < 20 else 'Hold')

    # Fetch all issuer codes to repopulate the dropdown
    cursor.execute("SELECT DISTINCT issuer_code FROM stock_data")
    issuer_codes = cursor.fetchall()

    conn.close()

    return render_template(
        'data_table.html',
        stock_data=df.to_dict(orient='records'),
        issuer_code=issuer_code,
        issuer_codes=[code['issuer_code'] for code in issuer_codes]
    )


if __name__ == '__main__':
    app.run(debug=True)
