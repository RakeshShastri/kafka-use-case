import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta

# Database connection configuration
db = mysql.connector.connect(
    host="localhost",
    user="python-conn",
    password="pythonconn",
    database="stock_db"
)

# Create a cursor object
cursor = db.cursor(dictionary=True)

# 1. Yearly Performance Comparison Across All Indexes
def yearly_performance():
    query = """
    SELECT idx, YEAR(date) AS year, MIN(close) AS min_close, MAX(close) AS max_close,
           (MAX(close) - MIN(close)) / MIN(close) * 100 AS performance
    FROM stock_data
    GROUP BY idx, YEAR(date)
    ORDER BY idx, YEAR(date);
    """
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    
    # Print the text output to the console
    print("\nYearly Performance Comparison Across All Indexes:")
    print(df.to_string(index=False))
    
    # Plot Yearly Performance
    plt.figure(figsize=(10, 6))
    for index in df['idx'].unique():
        index_data = df[df['idx'] == index]
        plt.plot(index_data['year'], index_data['performance'], marker='o', label=index)
    
    plt.title('Yearly Performance Comparison Across All Indexes')
    plt.xlabel('Year')
    plt.ylabel('Performance (%)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('yearly_performance.png')
    plt.close()  # Close the figure after saving to free memory

    return df

# 2. Volatility Measures for Individual Stocks (using 14-day Average True Range)
def stock_volatility(stock_symbol):
    query = f"""
    SELECT date, tr
    FROM stock_data
    WHERE idx = %s
    ORDER BY date DESC
    LIMIT 14;
    """
    cursor.execute(query, (stock_symbol,))
    result = cursor.fetchall()

    if len(result) == 14:
        df = pd.DataFrame(result)
        atr = df['tr'].mean()
        
        # Print the text output to the console
        print(f"\nVolatility (14-day TR) for {stock_symbol}:")
        print(df.to_string(index=False))
        print(f"\nAverage True Range (14-day ATR) for {stock_symbol}: {atr:.2f}")
        
        # Plot the True Range values
        plt.figure(figsize=(10, 6))
        plt.plot(df['date'], df['tr'], marker='o', label=f'TR of {stock_symbol}')
        plt.title(f'14-day True Range for {stock_symbol}')
        plt.xlabel('Date')
        plt.ylabel('True Range')
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()

        # Save the chart
        plt.savefig(f'true_range_{stock_symbol}.png')
        plt.close()  # Close the figure after saving to free memory
    else:
        print(f"Not enough data for {stock_symbol}")

# 3. Seasonal Trends in Index Performance (Monthly Performance)
def seasonal_trends():
    query = """
    SELECT idx, MONTH(date) AS month, AVG(close) AS avg_close
    FROM stock_data
    GROUP BY idx, MONTH(date)
    ORDER BY idx, MONTH(date);
    """
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result)

    # Print the text output to the console
    print("\nSeasonal Trends in Index Performance:")
    print(df.to_string(index=False))
    
    # Plot Monthly Average Closing Prices
    plt.figure(figsize=(10, 6))
    for index in df['idx'].unique():
        index_data = df[df['idx'] == index]
        plt.plot(index_data['month'], index_data['avg_close'], marker='o', label=index)
    
    plt.title('Seasonal Trends in Index Performance')
    plt.xlabel('Month')
    plt.ylabel('Average Closing Price')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Save the chart
    plt.savefig('seasonal_trends.png')
    plt.close()  # Close the figure after saving to free memory

    return df

# Call functions to run the analyses
yearly_performance_df = yearly_performance()

# Analyze volatility for a specific stock (change 'HSI' to other symbols as needed)
stock_symbol = 'HSI'
stock_volatility(stock_symbol)

seasonal_trends_df = seasonal_trends()

# Close the cursor and connection
cursor.close()
db.close()
