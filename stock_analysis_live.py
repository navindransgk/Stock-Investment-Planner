import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import date, timedelta, datetime, timezone
import sqlite3
import json
import os
import re
import numpy as np
import plotly.graph_objects as go
import numpy_financial as npf
import traceback
import time

st.set_page_config(layout="wide")

# Blue: 0000FF, Red: FF0000

st.markdown("""
<style>
    .stTabs [data-baseweb="tab-highlight"] {
        background-color:#1E90FF;
        font-size: 16px;
        font-weight: bold;
    }
            
	.stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
		font-size: 16px;
        font-weight: bold;
    }

	.stTabs [data-baseweb="tab"] {
		height: 30px;
        width: 180px;
        white-space: pre-wrap;
		background-color: #FFFFFF;
        color: #1E90FF;
		border-radius: 4px 4px 0px 0px;
		padding-top: 5px;
		padding-bottom: 5px;
        font-size: 16px;
        font-weight: bold;
        border-bottom: #1E90FF;
    }

	.stTabs [aria-selected="true"] {
  		background-color: #1E90FF;
        color: #FFFFFF;
        border-bottom: 2px solid #s;
        font-size: 16px;
        font-weight: bold;
	}
            
    span[data-baseweb="tag"] {
        background-color: #1E90FF !important;
        color: black !important; /* Change text color for contrast */
    }

    /* Targets the main value in the metric */
    [data-testid="stMetricValue"] {
        font-size: 1.5rem; /* Adjust the size as needed, e.g., to 1.5rem or 20px */
    }
    
    /* Targets the label/header in the metric */
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem; /* Adjust the size as needed */
    }

/* Targets the optional delta value */
[data-testid="stMetricDelta"] {
    font-size: 0.8rem; /* Adjust the size as needed */
}
</style>""", unsafe_allow_html=True)

# --- 1. SQLite Database Setup and Management ---
DB_NAME = "historical_live_data.db"
FILE_PATH = "ticker_list.json"

 # Create SQLite DB Connection
sqlite_conn = sqlite3.connect(DB_NAME)
sqlite_cursor = sqlite_conn.cursor()

# Empty lists to store data
tickerDataList = []
stockInfoList = []
pivotPointList = []
valuationRatiosList = []
profitabilityMetricsList = []
financialHealthLiquidityList = []
riskVolatilityList = []
companyValuesList = []
splitsDividendsDetailList = []
sharesList = []
bidAskDetailsList = []
targetPricePredictionList = []
incomeStatementList = []

# Columns for dataframes
stockInfoColumns = ['Ticker', 'Company Name', 'Sector', 'Industry', 'Market Type', 'Series', 'Exchange', 'Full Exchange Name', 'Quote Source', 'Market Capitalization', 'Analyst Recommendations', 'Analyst Rating', 'Total Opinions', 'Stock Currency', 'Data Valid For', 'Data Refreshed Frequency', 'Price Information Delay', 'Country', "Exchange Timezone", "Exchange Timezone Code"]
valuationRatiosColumns =['Ticker','Trailing PE Ratio', 'Forward PE Ratio', 'Price To Book', 'PE Growth Ratio']
profitabilityMetricsColumns = ['Ticker', 'Trailing EPS', 'Forward EPS', 'Earnings Growth', 'Revenue Growth', 'Operating Margins', 'Net Profit Margins', 'Gross Margins']
financialHealthLiquidityColumns = ['Ticker', 'Debt To Equity Ratio', 'Free Cash Flow', 'Current Ratio']
riskVolatilityColumns = ['Ticker', 'Beta', 'Stock Volatility Risk']
companyValuesColumns = ['Ticker', 'Total Market Value', 'Total Cash', 'Total Debt', 'Total Revenue', 'Total Employees']
splitsDividendsDetailColumns = ['Ticker', 'Last Split Factor', 'Last Split Date', 'Latest Dividend Value', 'Latest Dividend Date', 'Dividend Rate', 'Dividend Yield', 'Ex Dividend Date', 'Payout Ratio', 'Payout Ratio Percent', 'Dividend Sustainability']
sharesColumns = ['Ticker', 'Shares Outstanding', 'Implied Shares Outstanding', 'Potential Conversion Shares', 'Shares Remaining Ratio', 'Float Shares', 'Insider Ownership Gap']
bidAskDetailsColumns = ['Ticker', 'Bid', 'Ask', 'Bid Ask Spread', 'Ticker Liquidity', 'Bid Size', 'Ask Size', 'Price Movement', 'Bid Ask Percent']
targetPricePredictionColumns = ['Ticker', 'Regular Market Time', 'Regular Market Price', 'Target High Price', 'Target Low Price', 'Target Growth', 'Target Growth Percent', 'Target Fall', 'Target Fall Percent']
incomeStatementColumns = ['Ticker', 'Total Revenue', 'Net Income', 'Total Expenses', 'Operating Income', 'Operating Expense', 'Operating Revenue']

# Define the end date for downloading historical trade data
end_date = date.today().strftime("%Y-%m-%d") # Use today's date as the end date

# "MAR",  "CSCO", "SPY", "AMD", "CMCSA", "WBD", "PFE", "MSTR", "NKE", "PYPL", "LYG", "PEP", "BCS", "JPM", "ABNB", "VZ", "DIS", "KO", "TRV", "CAT", "GS", "AMGN", "MCD", "AXP", "HON", "PLTR", "BA", "BAC", "DELL", "DE", "EBAY", "GIS", "GLW", "KMB", "KHC", "MRK", "LLY", "VRTX", "MNST", "CTAS", "ADBE", "QCOM", "SBUX", "ACN", "MSI", "BIDU", "MU", "IRWD", "BRK-A", "BRK-B", "DPZ", "PG", "MS", "WFC", "AZN", "HSBC", "NVS", "PM", "CRM", "TMO", "SHOP", "SHEL", "ABT", "HDB", "TXN", "NOW", "SONY", "IBN", "BN", "SNOW", "EQIX", "INFY", "FDX", "TRI", "EA", "CTSH", "MT", "HPE", "VOD", "WIT", "FOX", "RDY", "JNJ", "UL", "UBER", "IMO", "SU", "SLF", "NWG", "CL", "WHR", "FTS", "LEVI", "HOG", "ZM", "LOGI", "CRWD", "REYN", "TRMB", "VRSN", "MMYT", "HWKN", "UPWK", "COUR", "BB", "FVRR", "DOMO"

# Required functions
# Save the list stored in session into json file for subsequent sessions
def saveTickerListToFile(tickerList):
    with open(FILE_PATH, 'w') as f:
        json.dump(tickerList, f)

# Load list from the saved json file for use in current session
def loadTickerListFromFile():
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, 'r') as f:
            tickerList = json.load(f)
    else:
        tickerList = []
    return tickerList

if 'ticker_list' not in st.session_state:
    st.session_state.ticker_list = loadTickerListFromFile()

def add_ticker_to_list():
    print("inside call back function")
    new_ticker = st.session_state.newTickers.strip().upper()
    newTickersList = [ticker.strip().upper() for ticker in new_ticker.split(',') if ticker.strip()]
    updatedTickerslist = st.session_state['ticker_list'] + newTickersList
    st.session_state['ticker_list'] = sorted(list(set(updatedTickerslist)))
    st.success(f"Ticker '{new_ticker}' added.")
    saveTickerListToFile(st.session_state['ticker_list'])
    st.session_state.newTickers = ""

def recordsForTickerExists(ticker, conn):
    # print(f"Checking records for ticker: {ticker}")
    query = """
        SELECT Trade_Date, Adjusted_Close, Close, High, Low, Open, Volume, Ticker
        FROM historical_live_trade_data
        WHERE Ticker = ?;
    """
    dbStockDataDF = pd.read_sql_query(query, conn, params=(ticker,))
    # print()
    # print(dbStockDataDF)
    # print()
    totalRecords = len(dbStockDataDF)
    # print(f"Total Records for {ticker}: {totalRecords}")

    if totalRecords > 0:
        recordsExists = True
    else:
        recordsExists = False 

    # print()
    # print(f"Records Exists for {ticker}: {recordsExists}")
    return recordsExists

def getMaxDateForExistingTickers(ticker, conn):
    query = """
        SELECT Ticker, MAX(Trade_Date) AS Last_Trade_Date
        FROM historical_live_trade_data
        WHERE Ticker = ?
        GROUP BY Ticker;
    """
    latestTradeDataDF = pd.read_sql_query(query, conn, params=(ticker,))
    # print(latestTradeDataDF)
    return latestTradeDataDF

def setInvestmentEndDate(investmentStartDate, preferredInvestmentDuration):
    if investmentStartDate:
        st.session_state.investmentStartDate = st.session_state.investmentStartDateKey
        daysToAdd = 365 * preferredInvestmentDuration
        st.session_state.investmentEndDate = st.session_state.investmentStartDateKey + timedelta(days=int(daysToAdd))

def setPillSelectionLimit(numberOfStocks):
    currentSelection = st.session_state['selectedPills']
    
    if len(currentSelection) > numberOfStocks:
        st.session_state["selectedPills"] = currentSelection[:numberOfStocks]
        st.warning(f"You can only select up to {numberOfStocks} options.")

def setStatusColor(shareTrend):
    if isinstance(shareTrend, (str)):
        if shareTrend == 'Advancing':
            color = "#238823"
        elif shareTrend == 'Declining':
            color = "#FF0000"    
        else:
            color = "#FFD200"
        return f"color: {color};"
    return ""

def formatBigNumbers(marketCap):
    # Formats a large number into millions (M) or billions (B) or Trillions (B).
    if abs(marketCap) >= 1e12:
        return f'{marketCap / 1e12:.2f} Trillions'
    elif abs(marketCap) >= 1e9:
        return f'{marketCap / 1e9:.2f} Billions'
    elif abs(marketCap) >= 1e6:
        return f'{marketCap / 1e6:.2f} Millions'
    elif abs(marketCap) >= 1e3:
        return f'{marketCap / 1e3:.2f} Thousands'
    else:
        # Keep non-large numbers in their original format with some rounding
        return f'{marketCap:,.2f}'

def flattenDataframeRenamingColumns(tickerDataDF, ticker):
    tickerDataDF['Ticker'] = ticker.replace('.NS', '') # remove extension if any non US stocks
    # Double Header
    # joining dataframe columns, replacing the ticker name with empty string and replace again with sapce
    tickerDataDF.columns = [','.join(col).strip().replace(ticker, '').replace(',', '') for col in tickerDataDF.columns.values]
    
    # When the ticker name is 'The Alphabet Portfolio: Single-Letter Tickers', it returns wrong column names since the above line replaces ticker name with empty text, the below code handles dynamic renaming without raising and errors
    tickerColumnsRenameMapping = {'dj Close': 'Adj Close', 'ow':'Low', 'pen':'Open', 'icker':'Ticker', 'olume':'Volume', 'lose': 'Close', 'Adj lose': 'Adj Close', 'igh': 'High'}
    tickerDataDF = tickerDataDF.rename(columns=tickerColumnsRenameMapping)    
    tickerDataDF.reset_index(inplace=True) 
    return tickerDataDF

def flattenDataframeLengthCondition(tickerDataDF, ticker):
    tickerExchangeList = ticker.split('.')
    # tickerName = ticker.replace('.NS', '') # remove extension if any non US stocks
    
    columnLength = len(tickerExchangeList[0])
    checkTickerLength = columnLength == 1

    if checkTickerLength:
        tickerDataDF.columns = [','.join(col).strip().replace(',', '') for col in tickerDataDF.columns.values]
    else:
        tickerDataDF.columns = [','.join(col).strip().replace(ticker, '').replace(',', '') for col in tickerDataDF.columns.values]

    return tickerDataDF

def flattenDataframeStack(tickerTradesDF):
    tickerTradesDF = tickerTradesDF.stack(level=1, future_stack=True) # Stack multi level into single level header or index
    tickerTradesDF.index.names = ['Date', 'Ticker'] # Rename index column appropriately
    tickerTradesDF = tickerTradesDF.reset_index(level=1) # Reset level 1 index
    tickerTradesDF = tickerTradesDF.rename_axis(columns={"Price": "Index"}) # Rename axis
    tickerTradesDF = tickerTradesDF.reset_index() # Reset level 2 index
    tickerTradesDF = tickerTradesDF.sort_values(by=['Ticker', 'Date'], ascending=[True, True]) # Sort datafarme by Ticker and Date
    resetColumnOrder = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'] # Reorder column list
    tickerTradesDF = tickerTradesDF[resetColumnOrder] # Reorder columns
    tickerTradesDF.reset_index(inplace=True) # Reset index again to set proper counter
    tickerTradesDF.drop('index', axis=1, inplace=True) # Drop new index column created

    # remove single extension for non US stocks in yfinance
    # removeExtension = '.NS'
    # tickerTradesDF['Ticker'] = tickerTradesDF['Ticker'].str.replace(removeExtension, '', regex=False)

    # remove multiple extensions using pattern for non US stocks in yfinance
    removeExtension = ['.AE', '.AQ', '.AS', '.AT', '.AX', '.BA', '.BD', '.BE', '.BK', '.BM', '.BO', '.BR', '.CA', '.CBT', '.CL', '.CME', '.CMX', '.CN', '.CO', '.CR', '.DE', '.DU', '.F', '.HA', '.HE', '.HK', '.HM', '.IC', '.IL', '.IR', '.IS', '.JK', '.JO', '.KL', '.KQ', '.KS', '.KW', '.L', '.LS', '.MC', '.MI', '.MU', '.MX', '.NE', '.NS', '.NX', '.NYB', '.NYM', '.NZ', '.OL', '.PA', '.PR', '.PS', '.QA', '.REGA', '.RG', '.RO', '.SA', '.SAU', '.SG', '.SI', '.SN', '.SS', '.ST', '.SW', '.SZ', '.T', '.TA', '.TI', '.TL', '.TO', '.TW', '.TWO', '.V', '.VI', '.VN', '.VS', '.WA', '.XA', '.XC', '.XD']
    extensionPattern = '|'.join(re.escape, removeExtension)
    tickerTradesDF['Ticker'] = tickerTradesDF['Ticker'].str.replace(extensionPattern, '', regex=True)

    # remove multiple extensions using 'for loop' for non US stocks in yfinance
    # for extension in removeExtension:
        # tickerTradesDF['Ticker'] = tickerTradesDF['Ticker'].str.replace(extension, '', regex=False)

    return tickerTradesDF

def yearToDaysMathematical(years):
    investmentDays = (years * 365) + (years // 4)
    return investmentDays

def yearToDaysDatetime(years):
    investmentStartDate = date(1, 1, 1)
    investmentEndDate = date(1 + years, 1, 1)
    daysDelta = investmentEndDate - investmentStartDate
    investmentDays = daysDelta.days
    return investmentDays

def setProfitLoss(profitLossAmount):
    if profitLossAmount > 0:
        color = "#4CBB17"
    else:
        color = "#FF0000"
    return f"color: {color};"

def onToggleChange():
    currentToggleState = st.session_state.hasToggled

    if currentToggleState:
        st.session_state.toggleLabel = 'Sectors'
    else:
        st.session_state.toggleLabel = "Industries"

st.title("Live Stock Data Analysis")

tradeData, investmentPlan, returnsCal, priceTrends, keyIndicators, companyMetrics, sectorIndustry = st.tabs(["Trade Data", "Investment Planner", "Returns on Investment", "Analyze Price Trends", "Key Technical Indicators", "Company Metrics", "Sector & Industry"])

with tradeData:
    st.header("Trade Data Download")
    # --- 2. Option to download stock data for new tickers ---
    # st.title("Add New Ticker Symbols to download historical & live trade data and include it in existing ticker list.")
    with st.container(horizontal=True):
        addNewTicker = st.button("Add Ticker")
        downloadTickerData = st.button("Download Ticker Data")

    if addNewTicker:
        try:
            newTickers = st.text_input("Enter Ticker Symbol", key='newTickers', on_change=add_ticker_to_list, placeholder="e.g., AAPL")
            appendTicker = st.button("Append Ticker to List")
            st.subheader("Current Tickers:")
            availableTickers = ', '.join(st.session_state['ticker_list'])
            st.write(availableTickers)        
        except Exception as e:
            st.error(f"Error downloading data: {e}")

    # --- 4. Download Ticker Data ---
    if downloadTickerData:
        try: 
            # drop_cursor = sqlite_conn.cursor()
            # drop_cursor.execute('''
            #    DROP TABLE IF EXISTS historical_live_stock_data
            # ''')
                
            # Create table to store live transactions for each ticker
            creatLiveStockTable = """
                CREATE TABLE IF NOT EXISTS historical_live_trade_data(
                    Trade_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Trade_Date TIMESTAMP NOT NULL,
                    Adjusted_Close REAL,
                    Close REAL,
                    High REAL,
                    Low REAL,
                    Open REAL,
                    Volume INTEGER,
                    Ticker TEXT NOT NULL
                );
            """
            sqlite_cursor.execute(creatLiveStockTable)
            
            if st.session_state['ticker_list']:
                for ticker in st.session_state['ticker_list']:
                    # st.write(ticker)
                    checkRecordsExists = recordsForTickerExists(ticker, sqlite_conn)
                    # st.write(checkRecordsExists)

                    if checkRecordsExists:
                        latestTradeDataDF = getMaxDateForExistingTickers(ticker, sqlite_conn)
                        latestTradeDataDF['Last_Trade_Date'] = pd.to_datetime(latestTradeDataDF['Last_Trade_Date'])
                        currentDate = pd.Timestamp.today().normalize()
                        dayDifference = (currentDate - latestTradeDataDF.Last_Trade_Date.iloc[0]).days
                        daysToDownload = int(dayDifference) - 1
                        start_date = (date.today() - timedelta(days=daysToDownload)).strftime("%Y-%m-%d")
                    else:
                        start_date = "1995-01-01" # 1995-01-01

                    print(f"Downloading data for {ticker} from {start_date} to {end_date}")
                    print()

                    tickerDataDF = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, multi_level_index=False)
                    tickerName = str(ticker)

                    if "." in tickerName:
                        tickerExchangeList = tickerName.split('.')
                        tickerDataDF['Ticker'] = tickerExchangeList[0]
                        tickerDataDF['Exchange Code'] = tickerExchangeList[1]
                    else:
                        tickerDataDF['Ticker'] = tickerName
                        tickerDataDF['Exchange Code'] = "NA"
                    
                    #.replace('.NS', '')
                    
                    # Double Header
                    tickerDataDF.columns = [','.join(col).strip().replace(ticker, '').replace(',', '') for col in tickerDataDF.columns.values]
                
                    # The Alphabet Portfolio: Single-Letter Tickers return wrong column names since the above line sreplace ticker name with empty text, the below code handles dynamic renaming without raising and errors
                    tickerColumnsRenameMapping = {'dj Close': 'Adj Close', 'ow':'Low', 'pen':'Open', 'icker':'Ticker', 'olume':'Volume', 'lose': 'Close', 'Adj lose': 'Adj Close', 'igh': 'High'}
                    tickerDataDF = tickerDataDF.rename(columns=tickerColumnsRenameMapping)
                
                    tickerDataDF.reset_index(inplace=True)            
                    tickerDataList.append(tickerDataDF)

                if 'historicalStockDataList' in st.session_state:
                    del st.session_state.historicalStockDataList

                if 'historicalStockDataList' not in st.session_state:
                    st.session_state.historicalStockDataList = tickerDataList 
            else:
                st.warning("Ticker list is empty. Please add tickers to download data.")
        
            # stockDataList = st.session_state.historicalStockDataList
            tickerHistoricalLiveDataDF = pd.concat(tickerDataList, ignore_index=False)
            tickerHistoricalLiveDataDF['Date'] = pd.to_datetime(tickerHistoricalLiveDataDF['Date'], format='%Y-%m-%d')
            tickerHistoricalLiveDataDF = tickerHistoricalLiveDataDF.rename_axis('Tradeindex')
            # st.write(tickerHistoricalLiveDataDF.dtypes)
            # st.write()# Insert the live trading records to the table
            
            if tickerHistoricalLiveDataDF is not None and not tickerHistoricalLiveDataDF.empty():
                insert_stock_transactions = "INSERT INTO historical_live_trade_data(Trade_Date, Adjusted_Close, Close, High, Low, Open, Volume, Ticker) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                        
                # Creating tuples for executemany
                # live_data_tuple = [tuple(row) for row in tickerHistoricalLiveDataDF.values]
                # st.write(live_data_tuple)

                # The below code is used for executemany
                # sqlite_cursor.executemany(insert_stock_transactions, live_data_tuple) # Does support timesatmp
                
                for index, row in tickerHistoricalLiveDataDF.iterrows():
                    # Convert pandas datetime to python datetime for SQLite3 database to accept Timestamp
                    row['Date'] = row['Date'].to_pydatetime()
                    sqlite_cursor.execute(insert_stock_transactions, (row['Date'], row['Adj Close'], row['Close'], row['High'], row['Low'], row['Open'], row['Volume'], row['Ticker']))
                
                # The below is syntax is used to create and load data into table without need ofto create table syntax or use insert statement
                # tickerHistoricalLiveDataDF.to_sql('historical_live_stock_data', sqlite_conn, if_exists='replace', index=False)
                
                sqlite_conn.commit()
                st.success("Data loaded successfully into 'historical_live_trade_data' table in 'historical_live_data.db'.")
                st.write()
        except sqlite3.Error as e:
            st.error("Error in downloading or loading data into SQLite")
            traceback.print_exc()
            st.exception(e)
    
    try:          
        st.header("Historical Stock Data")
        
        # tickerLists = st.selectbox("Select Tickers for Investment Plan", options=st.session_state['ticker_list'], \
        # help="Select the ticker symbols for which you want to create an investment plan.")
        st.write('Note: 30 years of trade data for multiple stocks is loaded into SQLite database. Filter using Tickers and Year.')
        with st.container(horizontal=False):
            tickerLists = st.multiselect("Select tickers", options=st.session_state['ticker_list'], default=st.session_state['ticker_list'], help="Select the ticker symbols for which you want to create an investment plan.")

            startDateCol, endDateCol = st.columns(2)
            fromDate = "1995-01-01"
            toDate =  date.today().strftime("%Y-%m-%d")
            maxFromDate = (date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            with startDateCol:
                tradeDateStart = st.date_input("Select trade start date:", value=fromDate, key="tradeStart", max_value=maxFromDate, help="Select the start date from which you need trade data.")
                
            with endDateCol:
                tradeDateEnd = st.date_input("Trade end date:", value=toDate, key="tradeEnd", disabled=True, help="Select the end date from which you need trade data.")

        placeholders = ', '.join(['?' for _ in tickerLists])
        tickerSymbolsList = [item.split('.', 1)[0] for item in tickerLists]
        allParams = (*tickerSymbolsList, tradeDateStart, tradeDateEnd)
        # st.write(tradeDateStart, tradeDateEnd)
        
        selectLiveTradesQuery = f"""
                SELECT Trade_Date, Adjusted_Close, Close, High, Low, Open, Volume, Ticker
                FROM historical_live_trade_data
                WHERE Ticker IN ({placeholders}) AND Trade_Date BETWEEN ? AND ?; 
        """
        
        tickerLiveDataDF = pd.read_sql_query(selectLiveTradesQuery, sqlite_conn, params=allParams)
        tickerLiveDataDF['Daily Return'] = tickerLiveDataDF['Close'].pct_change().fillna(0).map('{:.2%}'.format)
        tickerLiveDataDF['Trade Range'] = tickerLiveDataDF['High'] - tickerLiveDataDF['Low']
        tickerLiveDataDF['Trade Range'] = tickerLiveDataDF['Trade Range'].astype(float).round(2)
        tickerLiveDataDF.sort_values(by=['Ticker', 'Trade_Date'], ascending=[True, True], inplace=True)
        st.dataframe(tickerLiveDataDF, hide_index=True)

        if 'historicalLiveStockDataDF' in st.session_state:
            del st.session_state.historicalLiveStockDataDF

        if 'historicalLiveStockDataDF' not in st.session_state:
            st.session_state.historicalLiveStockDataDF = tickerLiveDataDF

        if 'selectedTickerLists' in st.session_state:
            del st.session_state.selectedTickerLists

        if 'selectedTickerLists' not in st.session_state:
            st.session_state.selectedTickerLists = tickerLists
                
        st.space("small") # "small", "medium", "large", or "stretch"
        st.divider()
    except sqlite3.Error as e:
        st.error("Error in fetching data from SQLite3 database")
        traceback.print_exc()
        st.exception(e)
    finally:
        # Close the connection
        if sqlite_conn:
            sqlite_conn.close()

with investmentPlan:
    try:    
        # Give option to get personal perference or system generate preference for investment
        st.header("Investment Planner for Live Stock Data")

        tickerLiveDataDF = st.session_state.historicalLiveStockDataDF

        availableStocks = tickerLiveDataDF['Ticker'].unique().tolist()
        # stocks = ', '.join(availableStocks)
        # st.subheader("Stocks available for investment:")
        # st.write(f"{stocks}")

        st.subheader("Stock Information")
        for ticker in availableStocks:
            tickerDetails = yf.Ticker(ticker).info
            company_long_name = tickerDetails.get('longName', 'N/A')
            company_short_name = tickerDetails.get('shortName', 'N/A') 

            if company_long_name != 'N/A':
                company_name = company_long_name
            elif company_short_name != 'N/A':
                company_name = company_short_name
            else:
                company_name = "Company name not available"

            sector = tickerDetails.get('sector', 'N/A')
            marketType = tickerDetails.get('market', 'N/A')
            series = tickerDetails.get('quoteType', 'N/A')
            exchange = tickerDetails.get('exchange', 'N/A')
            fullExchangeName = tickerDetails.get('fullExchangeName', 'N/A')
            quoteSourceName = tickerDetails.get('quoteSourceName', 'N/A')
            industry = tickerDetails.get('industry', 'N/A')
            stockMarketCap = tickerDetails.get('marketCap', 0)
            stockDataValidity = tickerDetails.get('maxAge', 0)
            stockSourceInterval = tickerDetails.get('sourceInterval', 0)
            exchangeDataDelayedBy = tickerDetails.get('exchangeDataDelayedBy', 0)
            country = tickerDetails.get('country', 'N/A')
            exchangeTimezone = tickerDetails.get('exchangeTimezoneName', 'N/A')
            exchangeTimezoneCode = tickerDetails.get('exchangeTimezoneShortName', 'N/A')
            
            # Recommendations Values
            recommendationKey = tickerDetails.get('recommendationKey', 'N/A')
            analystRating = tickerDetails.get('recommendationMean', 0)
            totalAnalystsOpinions = tickerDetails.get('numberOfAnalystOpinions', 0)

            # Currency in which all monetary values for that stock (such as price, market cap, dividends, etc.) are reported.
            tradeCurrency = tickerDetails.get('currency', 'N/A')
            
            # Valuation Ratios
            trailingPERatio = tickerDetails.get('trailingPE', 0)
            forwardPERatio = tickerDetails.get('forwardPE', 0)
            priceToBook = tickerDetails.get('priceToBook', 0)
            peGrowthRatio = tickerDetails.get('trailingPegRatio', 0)

            if peGrowthRatio is None:
                peGrowthRatio = 0
            else:
                peGrowthRatio = peGrowthRatio
            
            # Profitability Metrics 
            trailingEPS = tickerDetails.get('trailingEps', 0)
            forwardEPS = tickerDetails.get('forwardEps', 0)
            earningsGrowth = tickerDetails.get('earningsGrowth', 0)
            revenueGrowth = tickerDetails.get('revenueGrowth', 0)
            operatingMargins = tickerDetails.get('operatingMargins', 0)
            netProfitMargins = tickerDetails.get('profitMargins', 0)
            grossMargins = tickerDetails.get('grossMargins', 0)
            
            # Financial Health and Liquidity
            debtToEquityRatio = tickerDetails.get('debtToEquity', 0)
            freeCashFlow = tickerDetails.get('freeCashflow', 0)
            currentRatio = tickerDetails.get('currentRatio', 0)
            
            # Risk and Volatility
            tickerBeta = tickerDetails.get('beta', 0)
            
            if tickerBeta > 1:
                stockVolatilityRisk = 'More volatile than the market'
            elif tickerBeta == 1:
                stockVolatilityRisk = 'Same volatile as the market'
            elif tickerBeta > 0 and tickerBeta < 1:
                stockVolatilityRisk = 'Less volatile than the market'
            elif tickerBeta == 0:
                stockVolatilityRisk = 'Price movement uncorrelated to the market'
            elif tickerBeta < 0:
                stockVolatilityRisk = 'Price movement negatively uncorrelated to the market'

            # Company Values
            totalMarketValue = tickerDetails.get('enterpriseValue', 0)
            totalCash = tickerDetails.get('totalCash', 0)
            totalDebt = tickerDetails.get('totalDebt', 0)
            totalRevenue = tickerDetails.get('totalRevenue', 0)
            totalEmployees = tickerDetails.get('fullTimeEmployees', 0)

            # Split Details
            lastSplitFactor = tickerDetails.get('lastSplitFactor', 'N/A')
            lastSplitDateEpoch = tickerDetails.get('lastSplitDate', 'N/A')
            
            if lastSplitDateEpoch != 'N/A':
                lastSplitDate = datetime.fromtimestamp(lastSplitDateEpoch, tz=timezone.utc).date()

            # Dividend Details
            latestDividendValue = tickerDetails.get('lastDividendValue', 0)
            latestDividendDateEpoch = tickerDetails.get('lastDividendDate', 'N/A')
            
            if latestDividendDateEpoch != 'N/A':
                latestDividendDate = datetime.fromtimestamp(latestDividendDateEpoch, tz=timezone.utc).date()

            dividendRate = tickerDetails.get('dividendRate', 0)
            dividendYield = tickerDetails.get('dividendYield', 0)
            exDividendDateEpoch = tickerDetails.get('exDividendDate', 'N/A')
            
            if exDividendDateEpoch != 'N/A':
                exDividendDate = datetime.fromtimestamp(exDividendDateEpoch, tz=timezone.utc).date()    

            payoutRatio = tickerDetails.get('payoutRatio', 0)
            payoutRatioPercent = payoutRatio * 100

            if payoutRatioPercent > 100: 
                dividendSustainability = "Unsustainable Dividend"
            else:
                dividendSustainability = "Sustainable Dividend"

            # Shares
            sharesOutstanding = float(tickerDetails.get('sharesOutstanding', 0))
            impliedSharesOutstanding = float(tickerDetails.get('impliedSharesOutstanding', 0))
            potentialConversionShares = impliedSharesOutstanding - sharesOutstanding

            if sharesOutstanding != 0 and impliedSharesOutstanding != 0:
                sharesRemainingRatio = sharesOutstanding / impliedSharesOutstanding
            
            floatShares = float(tickerDetails.get('floatShares', 0))
            
            if sharesOutstanding != 0 and floatShares != 0:
                insiderOwnershipGap = sharesOutstanding - floatShares

            # Bid and Ask details
            stockBid =  float(tickerDetails.get('bid', 0))
            stockAsk =  float(tickerDetails.get('ask', 0))
            
            if stockBid > 0 and stockAsk > 0: 
                bidAskSpread = stockAsk - stockBid

            if bidAskSpread < 0.05:
                tickerLiquidity = 'High'
            elif 0.05 <= bidAskSpread < 0.5:
                tickerLiquidity = 'Medium'
            else:
                tickerLiquidity = 'Low'

            bidSize =  int(tickerDetails.get('bidSize', 0))
            askSize =  int(tickerDetails.get('askSize', 0))
            
            if bidSize - askSize > 1000:
                priceMovement = "Upward"
            elif askSize - bidSize > 1000:
                priceMovement = "Downward"
            else:
                priceMovement = "Stable"

            if bidAskSpread != 'N/A' and stockAsk != 0:
                bidAskPercent = (bidAskSpread / stockAsk) * 100

            regularMarketTimeEpoch = tickerDetails.get('regularMarketTime', 'N/A')
            
            if regularMarketTimeEpoch != 'N/A':
                regularMarketTime = datetime.fromtimestamp(regularMarketTimeEpoch, tz=timezone.utc).date()

            regularMarketPrice = float(tickerDetails.get('regularMarketPrice', 0))
            targetHighPrice = float(tickerDetails.get('targetHighPrice', 0))
            targetLowPrice = float(tickerDetails.get('targetLowPrice', 0))

            if regularMarketPrice != 0 and targetHighPrice != 0:
                targetGrowth = targetHighPrice - regularMarketPrice
                targetGrowthPercent = (targetHighPrice - regularMarketPrice) / regularMarketPrice * 100

            if regularMarketPrice != 0 and targetLowPrice != 0:
                targetFall = regularMarketPrice - targetLowPrice
                targetFallPercent = (regularMarketPrice - targetLowPrice) / regularMarketPrice * 100

            time.sleep(0.5)
            tickerISDetails = yf.Ticker(ticker)
            incomeStatementDF = tickerISDetails.income_stmt
            
            if 'Total Revenue' in incomeStatementDF.index:
                totalRevenue = incomeStatementDF.loc['Total Revenue'].iloc[0]
            else:
                totalRevenue = 0

            if 'Net Income' in incomeStatementDF.index:
                netIncome = incomeStatementDF.loc['Net Income'].iloc[0]
            else:
                netIncome = 0
            
            if 'Total Expenses' in incomeStatementDF.index:
                totalExpenses = incomeStatementDF.loc['Total Expenses'].iloc[0]
            else:
                totalExpenses = 0
            
            if 'Operating Income' in incomeStatementDF.index:
                operatingIncome = incomeStatementDF.loc['Operating Income'].iloc[0]
            else:
                operatingIncome = 0
            
            if 'Operating Expense' in incomeStatementDF.index:
                operatingExpenses = incomeStatementDF.loc['Operating Expense'].iloc[0]
            else:
                operatingExpenses = 0

            if 'Operating Revenue' in incomeStatementDF.index:    
                operatingRevenue = incomeStatementDF.loc['Operating Revenue'].iloc[0]
            else:
                operatingRevenue = 0

            tickerValue = str(ticker)

            if "." in tickerValue:
                tickerExchangeList = tickerValue.split('.')
                tickerSymbol = tickerExchangeList[0]
            else:
                tickerSymbol = tickerValue

            newStockInfo = [tickerSymbol, company_name, sector, industry, marketType, series, exchange, fullExchangeName, quoteSourceName, stockMarketCap, recommendationKey, analystRating, totalAnalystsOpinions, tradeCurrency, stockDataValidity, stockSourceInterval, exchangeDataDelayedBy, country, exchangeTimezone, exchangeTimezoneCode]
            stockInfoList.append(newStockInfo)

            newValuationRatios = [tickerSymbol, trailingPERatio, forwardPERatio, priceToBook, peGrowthRatio]
            valuationRatiosList.append(newValuationRatios)
            
            newProfitabilityMetrics = [tickerSymbol, trailingEPS, forwardEPS, earningsGrowth, revenueGrowth, operatingMargins, netProfitMargins, grossMargins]
            profitabilityMetricsList.append(newProfitabilityMetrics)
            
            newFinancialHealthLiquidity = [tickerSymbol, debtToEquityRatio, freeCashFlow, currentRatio]
            financialHealthLiquidityList.append(newFinancialHealthLiquidity)
            
            newRiskVolatility = [tickerSymbol, tickerBeta, stockVolatilityRisk]
            riskVolatilityList.append(newRiskVolatility)
            
            newCompanyValues =  [tickerSymbol, totalMarketValue, totalCash, totalDebt, totalRevenue, totalEmployees]
            companyValuesList.append(newCompanyValues)
            
            newSplitsDividendsDetail =  [tickerSymbol, lastSplitFactor, lastSplitDate, latestDividendValue, latestDividendDate, dividendRate, dividendYield, exDividendDate, payoutRatio, payoutRatioPercent, dividendSustainability]
            splitsDividendsDetailList.append(newSplitsDividendsDetail)
            
            newShares =  [tickerSymbol, sharesOutstanding, impliedSharesOutstanding, potentialConversionShares, sharesRemainingRatio, floatShares, insiderOwnershipGap]
            sharesList.append(newShares)
            
            newBidAskDetails =  [tickerSymbol, stockBid, stockAsk, bidAskSpread, tickerLiquidity, bidSize, askSize, priceMovement, bidAskPercent]
            bidAskDetailsList.append(newBidAskDetails)

            newTargetPricePrediction = [tickerSymbol, regularMarketTime, regularMarketPrice, targetHighPrice, targetLowPrice, targetGrowth, targetGrowthPercent, targetFall, targetFallPercent]
            targetPricePredictionList.append(newTargetPricePrediction)

            newIncomeStatement = [tickerSymbol, totalRevenue, netIncome, totalExpenses,  operatingIncome, operatingExpenses, operatingRevenue]
            incomeStatementList.append(newIncomeStatement)
        
        # Add lists to streamlit session state
        if 'valuationRatios' in st.session_state:
            del st.session_state.valuationRatios
        
        if 'valuationRatios' not in st.session_state:
            st.session_state.valuationRatios = valuationRatiosList

        if 'profitabilityMetrics' in st.session_state:
            del st.session_state.profitabilityMetrics

        if 'profitabilityMetrics' not in st.session_state:
            st.session_state.profitabilityMetrics = profitabilityMetricsList

        if 'financialHealthLiquidity' in st.session_state:
            del st.session_state.financialHealthLiquidity

        if 'financialHealthLiquidity' not in st.session_state:
            st.session_state.financialHealthLiquidity = financialHealthLiquidityList

        if 'riskVolatility' in st.session_state:
            del st.session_state.riskVolatility

        if 'riskVolatility' not in st.session_state:
            st.session_state.riskVolatility = riskVolatilityList

        if 'companyValues' in st.session_state:
            del st.session_state.companyValues

        if 'companyValues' not in st.session_state:
            st.session_state.companyValues = companyValuesList

        if 'splitsDividendsDetail' in st.session_state:
            del st.session_state.splitsDividendsDetail

        if 'splitsDividendsDetail' not in st.session_state:
            st.session_state.splitsDividendsDetail = splitsDividendsDetailList

        if 'shares' in st.session_state:
            del st.session_state.shares

        if 'shares' not in st.session_state:
            st.session_state.shares = sharesList

        if 'bidAskDetails' in st.session_state:
            del st.session_state.bidAskDetails

        if 'bidAskDetails' not in st.session_state:
            st.session_state.bidAskDetails = bidAskDetailsList

        if 'targetPricePrediction' in st.session_state:
            del st.session_state.targetPricePrediction

        if 'targetPricePrediction' not in st.session_state:
            st.session_state.targetPricePrediction = targetPricePredictionList

        if 'incomeStatement' in st.session_state:
            del st.session_state.incomeStatement

        if 'incomeStatement' not in st.session_state:
            st.session_state.incomeStatement = incomeStatementList

        # Stock Info Dataframe
        stockDataInfoDF = pd.DataFrame(stockInfoList, columns=stockInfoColumns)
        marketCapBins = [0, 50e6, 300e6, 2e9, 10e9, 200e9, np.inf]
        marketCapLabels = ['Nano-Cap', 'Micro-Cap', 'Small-Cap', 'Mid-Cap', 'Large-Cap', 'Mega-Cap']
        stockDataInfoDF['Market Capitalization BM'] = stockDataInfoDF['Market Capitalization'].apply(formatBigNumbers)
        stockDataInfoDF['Market Capitalization Category'] = pd.cut(stockDataInfoDF['Market Capitalization'], bins=marketCapBins, labels=marketCapLabels, right=False)

        # Last Trade price Dataframe
        tickerLastTradedPriceDF = tickerLiveDataDF.sort_values(by='Trade_Date', ascending=False).drop_duplicates(subset='Ticker', keep='first')
        finalStockCompanyInfoDF = pd.merge(stockDataInfoDF, tickerLastTradedPriceDF[['Ticker', 'Close', 'Trade_Date']], on='Ticker', how='left')
        finalStockCompanyInfoDF.rename(columns={'Close': 'Latest Price', 'Trade_Date': 'Last Trade Date'}, inplace=True)

        if 'companyInfo' in st.session_state:
            del st.session_state.companyInfo
        
        if 'companyInfo' not in st.session_state:
            st.session_state.companyInfo = finalStockCompanyInfoDF
        
        st.dataframe(finalStockCompanyInfoDF, hide_index=True)

        st.subheader("Investment Preferences")
        timelineCol, sectorPreferenceCol, industryPreferenceCol, marketCapPreferenceCol, investmentAmountCol, \
            investmentModeCol, numberOfStocksCol = st.columns(7)

        tickerLists = st.session_state.selectedTickerLists

        with timelineCol:
            investmentTimeline = st.radio("Select Timelines:", ['Short Term', 'Medium Term', 'Long Term'], index=0, horizontal=False)
        
        with sectorPreferenceCol:
            sectorPreference = st.radio("Select Sectors Preference:", ['Single', 'Multi'], index=0, horizontal=True)

        with industryPreferenceCol:
            industryPreference = st.radio("Select Industry Preference:", ['Single', 'Multi'], index=0, horizontal=True)

        with marketCapPreferenceCol:
            marketCapPreference = st.radio("Select Market Cap Preference:", ['Nano Cap', 'Micro Cap', 'Small Cap', 'Mid Cap', 'Large Cap', 'Mega Cap', 'Multi'], index=1, horizontal=False)
    
        with investmentAmountCol:
            investmentAmount = st.number_input("Enter Investment Amount (INR):", min_value=10000, max_value=1000000, step=5000, value=10000, \
                help="Enter the amount you plan to invest. Minimum is INR 10,000, maximum is INR 1,000,000, increments of INR 5,000.")

        with investmentModeCol:
            investmentMode = st.radio("Select Investment Mode:", ['Per Stock', 'Overall'], index=0, horizontal=False)

        with numberOfStocksCol:
            numberOfStocks = st.number_input("Number of Stocks to Invest In:", min_value=1, max_value=len(tickerLists), value=min(5, len(tickerLists)))

        if investmentTimeline == 'Short Term':
            investmentDuration = '1-3 Years'
        elif investmentTimeline == 'Medium Term':
            investmentDuration = '3-10 Years'
        elif investmentTimeline == 'Long Term':
            investmentDuration = '10+ Years'
        st.space("small") # "small", "medium", "large", or "stretch"
        st.divider()
        
        if 'stocksOptedForInvestment' not in st.session_state:
            st.session_state.selectedPills = []
        
        selectedStocks = st.pills("Select Stocks for Investment:", availableStocks, width="stretch", \
            selection_mode="multi", key='selectedPills', on_change= lambda:setPillSelectionLimit(numberOfStocks), \
            help=f"These are the stocks available for investment, select your preferred {numberOfStocks} \
            out of {len(availableStocks)} stocks.")
        
        if 'stocksOptedForInvestment' in st.session_state:
            del st.session_state.stocksOptedForInvestment
        
        if 'stocksOptedForInvestment' not in st.session_state:
            st.session_state.stocksOptedForInvestment = selectedStocks

        investmentStocks = ', '.join(selectedStocks)
        st.write(f"Stocks opted for Investment: {investmentStocks}")
        st.write(f"Investment Duration: {investmentTimeline} ({investmentDuration})")

        minMaxYears = re.findall(r'\d+', investmentDuration)
        minYear = int(minMaxYears[0])
        maxYear = int(minMaxYears[1])
        # st.write(minYear, maxYear)

        yearListByTimeline = []
        for year in range(minYear, maxYear + 1):
            yearListByTimeline.append(year)
        
        # st.write(yearListByTimeline)

        preferredInvestmentDuration = st.radio("Select Investment Term (Years):", yearListByTimeline, index=0, horizontal=True)

        if 'finalInvestmentDuration' in st.session_state:
            del st.session_state.finalInvestmentDuration

        if 'finalInvestmentDuration' not in st.session_state:
            st.session_state.finalInvestmentDuration = preferredInvestmentDuration 

        if 'investmentStartDate' not in st.session_state:
            st.session_state.investmentStartDate = date.today()

        if 'investmentEndDate' not in st.session_state:
            st.session_state.investmentEndDate = st.session_state.investmentStartDate.replace(year=st.session_state.investmentStartDate.year + preferredInvestmentDuration)

        defaultInvestmentEndDate = st.session_state.investmentStartDate.replace(year=st.session_state.investmentStartDate.year + preferredInvestmentDuration)

        investmentTimePeriodCol, investmentFrequencyCol, distributionSplitCol = st.columns(3)

        with investmentTimePeriodCol:
            investmentTimeStart = st.date_input("Select Investment Start:", value=st.session_state.investmentStartDate, \
            key="investmentStartDateKey", on_change=lambda: setInvestmentEndDate(st.session_state.investmentStartDate, preferredInvestmentDuration), \
            help="Select the date from which you plan to start your investment.")

            investmentTimeEnd = st.date_input("Investment End:", value=defaultInvestmentEndDate, key="investmentEndDate", \
            disabled=True, help="Select the date from which you plan to end your investment.")

        with investmentFrequencyCol:
            investmentFrequency = st.selectbox("Select Investment Frequency:", ['Monthly', 'Quarterly', 'Half-Yearly', 'Yearly', 'One Time'], index=0, \
                help="Select how frequently you plan to invest in the selected stocks.")
        if investmentMode == 'Overall':
            with distributionSplitCol:
                distributionSplit = st.selectbox("Select Distribution Split:", ['Equal Split', 'Custom Split'], index=0,
                    help="Select how you want to distribute your investment among the selected stocks.")
                
                if distributionSplit == 'Custom Split':
                    splitPercentages = st.text_input(f"Enter {numberOfStocks} integers separated by spaces (e.g., 10 20 30) for {numberOfStocks} stocks")

        st.space("small") # "small", "medium", "large", or "stretch"
        planInvestmentButton = st.button("Plan Investment Strategy")
        finalInvestmentReturnsDF = pd.DataFrame()    

        if planInvestmentButton:
            st.subheader("Planned Investment Strategy")
            try:
                # For demonstration, we will just show the selected options and a placeholder table
                investmentPlannerCol1, investmentPlannerCol2, investmentPlannerCol3 = st.columns(3)

                with investmentPlannerCol1:
                    st.write(f"Investment Timeline: {investmentTimeline} ({investmentDuration})")
                    st.write(f"Investment Period: {investmentTimeStart} to {investmentTimeEnd}")
                    st.write(f"Sector Preference: {sectorPreference}")
                    st.write(f"Industry Preference: {industryPreference}")

                with investmentPlannerCol2:    
                    st.write(f"Market Cap Preference: {marketCapPreference}")
                    st.write(f"Investment Amount: ₹{investmentAmount:.2f}")
                    st.write(f"Investment Mode: {investmentMode}")
                    st.write(f"Investment Frequency: {investmentFrequency}")

                if 'preferredInvestmentFrequency' in st.session_state:
                    del st.session_state.preferredInvestmentFrequency

                if 'preferredInvestmentFrequency' not in st.session_state:
                    st.session_state.preferredInvestmentFrequency = investmentFrequency
                
                finalSplitPercentages = ''

                if investmentMode == 'Overall':
                    if distributionSplit == 'Custom Split':
                        finalSplitPercentages = splitPercentages
                    else:
                        investmentPerStock = investmentAmount / numberOfStocks
                        investmentPercentagePerStock = (investmentPerStock / investmentAmount) * 100
                        individualSplitPercentages = str(int(investmentPercentagePerStock))

                        for i in range (int(numberOfStocks)):
                            finalSplitPercentages += f"{individualSplitPercentages} "
                else:
                    for i in range (int(numberOfStocks)):
                        finalSplitPercentages += f"100 "

                if investmentMode == 'Per Stock':
                    finalDistributionSplit = 'No Split'
                else: 
                    finalDistributionSplit = distributionSplit

                with investmentPlannerCol3:
                    st.write(f"Number of Stocks to Invest In: {numberOfStocks}")
                    st.write(f"Distribution Split Option: {finalDistributionSplit}")
                    st.write("Distribution Percentages are:", finalSplitPercentages)
                
                if finalSplitPercentages:
                    splitPercentagesList = [int(i) for i in finalSplitPercentages.split()]

                selectedStocks = st.session_state.stocksOptedForInvestment
                # st.write(selectedStocks)
                
                stockSplitDistributionDF = pd.DataFrame(splitPercentagesList, columns=['Distribution Split (%)'])
                stockSplitDistributionDF['Ticker'] = selectedStocks[:numberOfStocks]
                # st.dataframe(stockSplitDistributionDF)
                # st.space("small")
            
                # Placeholder for investment strategy table            
                investmentStrategyDF = pd.DataFrame({
                    'Ticker': selectedStocks,
                    # 'Planned Investment (INR)': investmentAmount,
                    'Expected Return (%)': [10 + i*2 for i in range(numberOfStocks)]  # Dummy expected returns
                })
                # st.dataframe(investmentStrategyDF)
                # st.space("small")

                finalInvestmentReturnsDF = pd.merge(investmentStrategyDF, stockSplitDistributionDF, on='Ticker', how='inner')   

                if investmentMode == 'Per Stock':
                    finalInvestmentReturnsDF['Planned Investment'] = investmentAmount
                else:
                    if distributionSplit == 'Custom Split':
                        finalInvestmentReturnsDF['Planned Investment'] = (investmentAmount * finalInvestmentReturnsDF['Distribution Split (%)']) / 100
                    else:
                        finalInvestmentReturnsDF['Planned Investment'] = investmentPerStock
                
                newColumnOrder = ['Ticker', 'Distribution Split (%)', 'Planned Investment', 'Expected Return (%)']
                finalInvestmentReturnsDF = finalInvestmentReturnsDF[newColumnOrder]
                
                if 'investmentReturnsDF' in st.session_state:
                    del st.session_state.investmentReturnsDF

                if 'investmentReturnsDF' not in st.session_state:
                    st.session_state.investmentReturnsDF = finalInvestmentReturnsDF

                st.dataframe(finalInvestmentReturnsDF, hide_index=True)
                st.space('small')
            except Exception as e:
                st.error(f"Error planning investment strategy: {e}")

        st.space("small") # "small", "medium", "large", or "stretch"
        investmentDetailsButton = st.button("Investment Details")

        if investmentDetailsButton:
            st.subheader("Investment History")

            # Investment returns dataframe
            finalInvestmentReturnsDF =  st.session_state.investmentReturnsDF
            # st.write("Final Investment Returns")
            # st.dataframe(finalInvestmentReturnsDF, hide_index=True)
            # st.space("small")
            
            # Company info of downloaded stocks
            finalStockCompanyInfoDF = st.session_state.companyInfo
            # st.write("Final Stock Info")
            # st.dataframe(finalStockCompanyInfoDF, hide_index=True)
            # st.space('small')

            investmentsHistoryDF = pd.merge(finalStockCompanyInfoDF, finalInvestmentReturnsDF[['Ticker', 'Planned Investment']], on='Ticker', how='inner')
            investmentsHistoryDF["Last Trade Date"] = pd.to_datetime(investmentsHistoryDF["Last Trade Date"]).dt.date
            investmentsHistoryDF["Units Purchased"] = investmentsHistoryDF["Planned Investment"] // investmentsHistoryDF["Latest Price"]
            investmentsHistoryDF["Purchase Amount"] = (investmentsHistoryDF["Units Purchased"] * investmentsHistoryDF["Latest Price"]).round(2)
            investmentsHistoryDF["Balance Amount"] = (investmentsHistoryDF["Planned Investment"] - investmentsHistoryDF["Purchase Amount"]).round(2)
            investmentsHistoryDF["Investment Date"] = date.today() # datetime.now()

            #Calculate number of months based on preferred invetment frequency
            investmentFrequency = st.session_state.preferredInvestmentFrequency
            monthsDuration = 0

            if investmentFrequency == 'Monthly':
                monthsDuration = 1
            elif investmentFrequency == 'Quarterly':
                monthsDuration = 3
            elif investmentFrequency == 'Half-Yearly':
                monthsDuration = 6
            elif investmentFrequency == 'Yearly':
                monthsDuration = 12
            else:
                monthsDuration = 95747

            investmentsHistoryDF["Next Investment Date"] = pd.to_datetime(investmentsHistoryDF["Investment Date"] + pd.DateOffset(months=monthsDuration)).dt.date # Date
            # investmentsHistoryDF["Next Investment Date"] = investmentsHistoryDF["Investment Date"] + pd.DateOffset(months=1) # Date and Time
            investmentsHistoryDF = investmentsHistoryDF.drop(['Market Type', 'Series', 'Exchange', 'Market Capitalization'], axis=1)
            st.write("Final Investment History")
            st.dataframe(investmentsHistoryDF, hide_index=True)
            st.space('small')
    except Exception as e: 
        st.error("Error generating and fetching investment data.")
        traceback.print_exc()
        st.exception(e)

with returnsCal:
    st.header("Returns Calculator")

    try:
        sqlite_returns_conn = sqlite3.connect(DB_NAME)
        # st.write(numberOfDays)

        plCurrentFuture = st.radio("Select investment type:", ['Completed', 'Current'], index=0, horizontal=True, \
                help="Select the option to see profit & loss for completed investment or calcuate for current investments.")
        
        if plCurrentFuture == "Completed":
            preferredDuration = st.session_state.finalInvestmentDuration
            numberOfDays = yearToDaysMathematical(preferredDuration)
            returnStartDate = (date.today() - timedelta(days=numberOfDays)).strftime("%Y-%m-%d")
            returnEndDate = date.today().strftime("%Y-%m-%d")
            
            downloadedTickersList = st.session_state['ticker_list']
            selectTicker = st.selectbox("Select Ticker for Profit Loss calculations", options=downloadedTickersList, index=0, key='tickerReturns', \
                        help="Select the ticker to display cose price trend.")
            # st.write(selectTicker)
            
            allReturnParams = (selectTicker, returnStartDate, returnEndDate)
            
            selectProfitLossData = f"""
                    SELECT Ticker, Trade_Date, Close AS 'Purchase Price'
                    FROM historical_live_trade_data
                    WHERE Ticker = ? AND Trade_Date BETWEEN ? AND ?; 
            """
            tradeDataReturnsDF = pd.read_sql_query(selectProfitLossData, sqlite_returns_conn, params=allReturnParams)
            tradeDataReturnsDF = tradeDataReturnsDF.sort_values(by=['Ticker', 'Trade_Date'], ascending=[True, True])
            tradeDataReturnsDF['Trade_Date'] = pd.to_datetime(tradeDataReturnsDF['Trade_Date'])
            # st.write(tradeDataReturnsDF.dtypes)
            
            latestReturnsDF = tradeDataReturnsDF.groupby(['Ticker', tradeDataReturnsDF['Trade_Date'].dt.to_period('M')]).agg(
                First_Actual_Date=('Trade_Date', 'min')
            ).reset_index()
            
            latestReturnsDF = latestReturnsDF.rename(columns={'Trade_Date': 'Trade Month', 'First_Actual_Date': 'Trade_Date'})
            latestPriceDF = pd.merge(latestReturnsDF, tradeDataReturnsDF, on=['Ticker', 'Trade_Date'], how='inner')
            latestPriceDF.drop_duplicates(inplace=True)
            latestPriceDF['Monthly Investment'] = 10000
            latestPriceDF['Units Purchased'] = (10000 / latestPriceDF['Purchase Price']).astype(int)
            latestPriceDF['Purchase Amount'] = (latestPriceDF['Purchase Price'] * latestPriceDF['Units Purchased']).round(2)
            latestPriceDF['Balance Amount'] = (latestPriceDF['Monthly Investment'] - latestPriceDF['Purchase Amount']).round(2)
            totalAmountInvested = latestPriceDF['Monthly Investment'].sum()
            totalPurchaseAmount = latestPriceDF['Purchase Amount'].sum()
            totalUnitsPurchased = latestPriceDF['Units Purchased'].sum()
            totalReturns = latestPriceDF['Purchase Price'].iloc[-1] * totalUnitsPurchased
            totalProfitLoss = totalReturns - totalPurchaseAmount
            rateOfInvestments = (totalReturns - totalPurchaseAmount) / totalPurchaseAmount      

            if selectTicker == "AAPL":
                st.dataframe(latestPriceDF, hide_index=True, column_config={'Trade Month': None})
                st.space('small')

                investmentSummaryKPI = {
                    'Total Amount Invested': [f"{totalPurchaseAmount:.2f}"],
                    'Total Returns on Stock': [f"{totalReturns:.2f}"],
                    'Total Units Purchased': [totalUnitsPurchased],
                    'Profit Loss': [f"{totalProfitLoss:.2f}"],
                    'Returns on Investment': [f"{rateOfInvestments:.4f}"]
                }

                investmentSummaryDF = pd.DataFrame(investmentSummaryKPI)
                investmentSummaryDF['Profit Loss'] = investmentSummaryDF['Profit Loss'].astype(float)
                investmentSummaryDF['Returns on Investment'] = investmentSummaryDF['Returns on Investment'].astype(float)
                investmentSummaryDF = investmentSummaryDF.style.applymap(setProfitLoss, subset=['Profit Loss', 'Returns on Investment'])
                investmentSummaryDF = investmentSummaryDF.format(
                    {"Profit Loss": "{:.2f}", "Returns on Investment": "{:.2%}"}
                )
                st.dataframe(investmentSummaryDF, hide_index=True)
            else:
                st.write("No investment data available.")
        elif plCurrentFuture == "Current":
            annualRate = 0.12
            yearsInvestmented = 3
            initialContribution = 15000
            monthlyContribution = 15000
            ratePerPeriod = annualRate / 12
            totalInvestmentPeriods = yearsInvestmented * 12
            totalUnitsPurchased = 61 * 12 * 3
            stockPrice = 242.66
            totalPurchaseAmount = 242.66 * 61 * 12 * 3 
            futureInvestment = npf.fv(ratePerPeriod, totalInvestmentPeriods, -monthlyContribution, initialContribution)
            totalProfitLoss = futureInvestment - totalPurchaseAmount
            rateOfInvestments = (futureInvestment - totalPurchaseAmount) / totalPurchaseAmount

            futureInvestmentSummaryKPI = {
                'Stock': "AMZN",
                'Total Amount Invested': [f"{totalPurchaseAmount:.2f}"],
                'Total Returns on Stock': [f"{futureInvestment:.2f}"],
                'Total Units Purchased': [totalUnitsPurchased],
                'Profit Loss': [f"{totalProfitLoss:.2f}"],
                'Returns on Investment': [f"{rateOfInvestments:.4f}"]
            }

            futureInvestmentSummaryDF = pd.DataFrame(futureInvestmentSummaryKPI)
            futureInvestmentSummaryDF['Profit Loss'] = futureInvestmentSummaryDF['Profit Loss'].astype(float)
            futureInvestmentSummaryDF['Returns on Investment'] = futureInvestmentSummaryDF['Returns on Investment'].astype(float)
            futureInvestmentSummaryDF = futureInvestmentSummaryDF.style.applymap(setProfitLoss, subset=['Profit Loss', 'Returns on Investment'])
            
            futureInvestmentSummaryDF = futureInvestmentSummaryDF.format(
                {"Profit Loss": "{:.2f}", "Returns on Investment": "{:.2%}"}
            )
            st.dataframe(futureInvestmentSummaryDF, hide_index=True)
    except Exception as e: 
        st.error("Error generating Returns!!!")
        traceback.print_exc()
        st.exception(e)
    finally:
        # Close the connection
        if sqlite_returns_conn:
            sqlite_returns_conn.close()

with priceTrends:
    downloadedTickersList = st.session_state['ticker_list']
    selectTickerTrend = st.selectbox("Select Ticker", options=downloadedTickersList, index=0, key='tickerTrend', \
                    help="Select the ticker to display cose price trend.")
    
    trendStartDate = (date.today() - timedelta(days=370)).strftime("%Y-%m-%d")
    trendEndDate = date.today().strftime("%Y-%m-%d")
    allTrendParams = (selectTickerTrend, trendStartDate, trendEndDate)

    try:
        sqlite_trend_conn = sqlite3.connect(DB_NAME)
        # sqlite_trend_cursor = sqlite_trend_conn.cursor() Trade_Date, Adjusted_Close, Close, High, Low, Open, Volume, Ticker
        selectTickerData = f"""
                SELECT Ticker, Trade_Date, Close, High, Low, Open, Volume
                FROM historical_live_trade_data
                WHERE Ticker = ? AND Trade_Date BETWEEN ? AND ?; 
        """
        
        tradeDataTrendDF = pd.read_sql_query(selectTickerData, sqlite_trend_conn, params=allTrendParams)
        tradeDataTrendDF['Previous Close'] = tradeDataTrendDF['Close'].shift(1)
        tradeDataTrendDF['Previous Volume'] = tradeDataTrendDF['Volume'].shift(1)
        tradeDataTrendDF['Previous Week Close'] = tradeDataTrendDF['Close'].shift(periods=5)
        tradeDataTrendDF['Previous Week Volume'] = tradeDataTrendDF['Volume'].shift(periods=5)
        tradeDataTrendDF['Previous Month Close'] = tradeDataTrendDF['Close'].shift(periods=22)
        tradeDataTrendDF['Previous Month Volume'] = tradeDataTrendDF['Volume'].shift(periods=22)
        tradeDataTrendDF['Previous Quarter Close'] = tradeDataTrendDF['Close'].shift(periods=63)
        tradeDataTrendDF['Previous Quarter Volume'] = tradeDataTrendDF['Volume'].shift(periods=63)
        tradeDataTrendDF['Previous Biannual Close'] = tradeDataTrendDF['Close'].shift(periods=126)
        tradeDataTrendDF['Previous Biannual Volume'] = tradeDataTrendDF['Volume'].shift(periods=126)
        tradeDataTrendDF['Previous Year Close'] = tradeDataTrendDF['Close'].shift(periods=250)
        tradeDataTrendDF['Previous Year Volume'] = tradeDataTrendDF['Volume'].shift(periods=250)
        tradeDataTrendDF['Trade Month'] = pd.to_datetime(tradeDataTrendDF['Trade_Date']).dt.strftime('%b-%Y')
        
        # st.dataframe(tradeDataTrendDF, hide_index=True)
        # st.write(len(tradeDataTrendDF))
        # Price columns for Metric
        currentPrice = tradeDataTrendDF['Close'].iloc[-1]
        
        # Previous Prices
        previousPrice = tradeDataTrendDF['Previous Close'].iloc[-1]
        previousWeekPrice = tradeDataTrendDF['Previous Week Close'].iloc[-1]
        previousMonthPrice = tradeDataTrendDF['Previous Month Close'].iloc[-1]
        previousQuarterPrice = tradeDataTrendDF['Previous Quarter Close'].iloc[-1]
        previousBiannualPrice = tradeDataTrendDF['Previous Biannual Close'].iloc[-1]
        previousYearPrice = tradeDataTrendDF['Previous Year Close'].iloc[-1]

        # Price Changes
        priceChange = currentPrice - previousPrice
        weeklyPriceChange = currentPrice - previousWeekPrice
        monthlyPriceChange = currentPrice - previousMonthPrice
        quarterlyPriceChange = currentPrice - previousQuarterPrice
        biannualPriceChange = currentPrice - previousBiannualPrice
        yearlyPriceChange = currentPrice - previousYearPrice

        #Price Change Percent
        priceChangePercent = (priceChange / previousPrice) * 100
        weeklyPriceChangePercent = (weeklyPriceChange / previousWeekPrice) * 100
        monthlyPriceChangePercent = (monthlyPriceChange / previousMonthPrice) * 100
        quarterlyPriceChangePercent = (quarterlyPriceChange / previousQuarterPrice) * 100
        biannualPriceChangePercent = (biannualPriceChange / previousBiannualPrice) * 100
        yearlyPriceChangePercent = (yearlyPriceChange / previousYearPrice) * 100
        # priceSeries = tradeDataTrendDF['Close']
        
        # Volume columns for Metric
        currentVolume = tradeDataTrendDF['Volume'].iloc[-1].astype(int)
        
        #Previous Volumes
        previousVolume = tradeDataTrendDF['Previous Volume'].iloc[-1].astype(int)
        previousWeekVolume = tradeDataTrendDF['Previous Week Volume'].iloc[-1].astype(int)
        previousMonthVolume = tradeDataTrendDF['Previous Month Volume'].iloc[-1].astype(int)
        previousQuarterVolume = tradeDataTrendDF['Previous Quarter Volume'].iloc[-1].astype(int)
        previousBiannualVolume = tradeDataTrendDF['Previous Biannual Volume'].iloc[-1].astype(int)
        previousYearVolume = tradeDataTrendDF['Previous Year Volume'].iloc[-1].astype(int)

        volumeChange = currentVolume - previousVolume
        weeklyVolumeChange = currentVolume - previousWeekVolume
        monthlyVolumeChange = currentVolume - previousMonthVolume
        quarterlyVolumeChange = currentVolume - previousQuarterVolume
        biannualVolumeChange = currentVolume - previousBiannualVolume
        yearlyVolumeChange = currentVolume - previousYearVolume

        #Price Change Percent
        volumeChangePercent = (volumeChange / previousVolume) * 100
        weeklyVolumeChangePercent = (weeklyVolumeChange / previousWeekVolume) * 100
        monthlyVolumeChangePercent = (monthlyVolumeChange / previousMonthVolume) * 100
        quarterlyVolumeChangePercent = (quarterlyVolumeChange / previousQuarterVolume) * 100
        biannualVolumeChangePercent = (biannualVolumeChange / previousBiannualVolume) * 100
        yearlyVolumeChangePercent = (yearlyVolumeChange / previousYearVolume) * 100
        # volumeSeries = tradeDataTrendDF['Volume']

        # priceCol, volumeCol = st.columns(2), with priceCol:
        dailyWeeklyRow = st.container(horizontal=True)
        monthlyQuarterlyRow = st.container(horizontal=True)
        biannualYearlyRow = st.container(horizontal=True)

        with dailyWeeklyRow:      
            st.metric(label='Price vs Previous Day Price', value=f"{currentPrice:.2f} ({previousPrice:.2f})", delta=f"{priceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Day Volume', value=f"{currentVolume} ({previousVolume})", delta=f"{volumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)      
            st.metric(label='Price vs Previous Week Price', value=f"{currentPrice:.2f} ({previousWeekPrice:.2f})", delta=f"{weeklyPriceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Week Volume', value=f"{currentVolume} ({previousWeekVolume})", delta=f"{weeklyVolumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)

        with monthlyQuarterlyRow:      
            st.metric(label='Price vs Previous Month Price', value=f"{currentPrice:.2f} ({previousMonthPrice:.2f})", delta=f"{monthlyPriceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Month Volume', value=f"{currentVolume} ({previousMonthVolume})", delta=f"{monthlyVolumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)      
            st.metric(label='Price vs Previous Quarter Price', value=f"{currentPrice:.2f} ({previousQuarterPrice:.2f})", delta=f"{quarterlyPriceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Quarter Volume', value=f"{currentVolume} ({previousQuarterVolume})", delta=f"{quarterlyVolumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)

        with biannualYearlyRow:      
            st.metric(label='Price vs Previous Biannual Price', value=f"{currentPrice:.2f} ({previousBiannualPrice:.2f})", delta=f"{biannualPriceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Biannual Volume', value=f"{currentVolume} ({previousBiannualVolume})", delta=f"{biannualVolumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)      
            st.metric(label='Price vs Previous Year Price', value=f"{currentPrice:.2f} ({previousYearPrice:.2f})", delta=f"{yearlyPriceChangePercent:.2f}%", delta_color="normal", border=True, width=325)
            st.metric(label='Volume vs Previous Year Volume', value=f"{currentVolume} ({previousYearVolume})", delta=f"{yearlyVolumeChangePercent:.2f}%", delta_color="normal", border=True, width=325)    

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=tradeDataTrendDF['Trade_Date'], y=tradeDataTrendDF['Close'], mode='lines', name='Close Price'))
        fig.update_layout(
            title=f'{selectTicker} Closing Price History',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)

        figcs = go.Figure(data=[go.Candlestick(
            x=tradeDataTrendDF['Trade_Date'],
            open=tradeDataTrendDF['Open'],
            high=tradeDataTrendDF['High'],
            low=tradeDataTrendDF['Low'],
            close=tradeDataTrendDF['Close'],
            name=selectTicker
        )])

        figcs.update_layout(
            title=f'{selectTicker} Stock Price',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            xaxis_rangeslider_visible=False,
            height=500
        )

        st.plotly_chart(figcs, use_container_width=True)
    except Exception as e:
        st.error("Error in generating reports!!!")
        traceback.print_exc()
        st.exception(e)
    finally:
        # Close the connection
        if sqlite_trend_conn:
            sqlite_trend_conn.close()

with keyIndicators:
    try:
        with st.expander("Key Technical Indicators Definition and Formula"):
            # st.subheader("Key Technical Indicators Definition and Formula")
            keyTechnicalIndicators = {
                'Name': ['Pivot Point', 'R1 (First Resistance)', 'R2 (Second Resistance)', 'R3 (Third Resistance)', \
                        'R4 (Outermost Resistance)', 'S1 (First Support)', 'S2 (Second Support)', 'S3 (Third Support)', \
                        'S4 (Outermost Support)','Typical Price Indicator', 'OHLC4', 'Median Price'],
                'Formula': ['(Previous High + Previous Low + Previous Close) / 3', '(2 * Pivot Point) - Previous Low', \
                        'Pivot Point + (Previous High - Previous Low)', 'Previous High + 2 * (Pivot Point - Previous Low)', \
                        'R3 + (R2 - R1)', '(2 * Pivot Point) - Previous High', 'Pivot Point - (Previous High - Previous Low)', \
                        'Previous Low - 2 * (Previous High - Pivot Poiint)', 'S3 + (S1 - S2)', '(High + Low + Close) / 3', \
                        '(Open + High + Low + Close) / 4', '(High + Low) / 2'],
                'Description': ['Identify potential support and resistance levels for the current trading session. Shows the potential Market reversals and price targets.', \
                    'First minor hurdle, a breakout suggests continued upward momentum but often finds profit-taking here.', \
                    'A stronger barrier, requiring more buying power to overcome; a breakout signals stronger bullish conviction.', \
                    'A major resistance level, indicating significant selling pressure; a break here often signals a powerful trend continuation or major reversal.', \
                    'It is a strong resistance level. If the price breaks above R4 with significant volume and momentum, it often signals the beginning of a strong bullish (upward) trend or breakout. Traders might open long positions (buy) in expectation of further price increases.', \
                    'This is the immediate, minor support level, suggesting where the first wave of significant buying interest might appear. It is the most likely initial pause point in a downtrend.', \
                    'If the price breaks below S1 (indicating that the initial demand was not strong enough), the market will likely move toward S2. This level represents a stronger area of demand, where nimore determined buyers are expected to step in.', \
                    'A break below S2 signals a potentially strong bearish momentum. S3 is considered a major support level, a "last line of defense" for the bulls, where a significant reversal or a major buying opportunity is anticipated due to very high demand expectations', \
                    'is a strong support level. If the price breaks below S4, it suggests strong bearish (downward) momentum, potentially signaling a strong downtrend or breakout. Traders might consider opening short positions (sell).', \
                    'Shows an asset''s average price for a period (like a day) It smooths price data, helps spot trends, and forms the basis for more complex indicators like the Money Flow Index (MFI) and Commodity Channel Index (CCI).', \
                    'The OHLC4 value for any given bar in CandleStick chart. This value is also referred to as the "Candle Price Average"', \
                    'Finds the middle value in a series of prices (like daily High/Low), offering insight into typical price levels over time, useful for spotting trends by averaging these midpoints']
            }

            keyTechnicalIndicatorsDF = pd.DataFrame(keyTechnicalIndicators)
            st.markdown(
                keyTechnicalIndicatorsDF.style.hide(axis="index").to_html(), unsafe_allow_html=True
            )

        historicalTradeDataDF = st.session_state.historicalLiveStockDataDF
        historicalTradeDataDF.sort_values(by=['Ticker', 'Trade_Date'], ascending=[True, True], inplace=True) # Sort values by Ticker and Trade Date ascending 
        # historicalTradeDataDF.drop(['Adjusted_Close', 'Daily Return', 'Trade Range'], axis=1, inplace=True)
        historicalTradeDataDF["Trade_Date"] = pd.to_datetime(historicalTradeDataDF["Trade_Date"]).dt.date
        
        # Creating previous day's Open, High, Low and CLose values for calculating Key Technical Indicators
        historicalTradeDataDF['Previous Close'] = historicalTradeDataDF['Close'].shift(1)
        historicalTradeDataDF['Previous High'] = historicalTradeDataDF['High'].shift(1)
        historicalTradeDataDF['Previous Low'] = historicalTradeDataDF['Low'].shift(1)
        historicalTradeDataDF['Previous Open'] = historicalTradeDataDF['Open'].shift(1)
        historicalTradeDataDF.sort_values(by=['Ticker', 'Trade_Date'], ascending=[True, False], inplace=True)
        
        # Hide unwanted columns
        hideColumns = {'Previous Close': None, 'Previous Open': None, 'Previous High': None, 'Previous Low': None}

        # Dataframe for Key Technical Indicators, filter for last 5 years data
        keyTechnicalIndicatorsDF = historicalTradeDataDF[pd.to_datetime(historicalTradeDataDF["Trade_Date"]).dt.year > 2020]
        keyTechnicalIndicatorsDF['Pivot Point'] = ((keyTechnicalIndicatorsDF['Previous High'] + keyTechnicalIndicatorsDF['Previous Low'] + \
                                    keyTechnicalIndicatorsDF['Previous Close']) / 3).astype(float).round(2)
        keyTechnicalIndicatorsDF['R1'] = ((2 * keyTechnicalIndicatorsDF['Pivot Point']) - \
                                    keyTechnicalIndicatorsDF['Previous Low']).astype(float).round(2)
        keyTechnicalIndicatorsDF['R2'] = (keyTechnicalIndicatorsDF['Pivot Point'] + (keyTechnicalIndicatorsDF['Previous High'] - \
                                    keyTechnicalIndicatorsDF['Previous Low'])).astype(float).round(2)
        keyTechnicalIndicatorsDF['R3'] = (keyTechnicalIndicatorsDF['Previous High'] + (2 * (keyTechnicalIndicatorsDF['Pivot Point'] - \
                                    keyTechnicalIndicatorsDF['Previous Low']))).astype(float).round(2)
        keyTechnicalIndicatorsDF['R4'] = (keyTechnicalIndicatorsDF['R3'] + (keyTechnicalIndicatorsDF['R2'] - keyTechnicalIndicatorsDF['R1'])).astype(float).round(2)
        keyTechnicalIndicatorsDF['S1'] = ((2 * keyTechnicalIndicatorsDF['Pivot Point']) - \
                                    keyTechnicalIndicatorsDF['Previous High']).astype(float).round(2)
        keyTechnicalIndicatorsDF['S2'] = (keyTechnicalIndicatorsDF['Pivot Point'] - (keyTechnicalIndicatorsDF['Previous High'] - \
                                    keyTechnicalIndicatorsDF['Previous Low'])).astype(float).round(2)
        keyTechnicalIndicatorsDF['S3'] = (keyTechnicalIndicatorsDF['Previous Low'] + (2 * (keyTechnicalIndicatorsDF['Previous High'] - \
                                keyTechnicalIndicatorsDF['Pivot Point']))).astype(float).round(2)
        keyTechnicalIndicatorsDF['S4'] = (keyTechnicalIndicatorsDF['S3'] - (keyTechnicalIndicatorsDF['S1'] - keyTechnicalIndicatorsDF['S2'])).astype(float).round(2)
        keyTechnicalIndicatorsDF['Typical Price'] = ((keyTechnicalIndicatorsDF['High'] + \
                    keyTechnicalIndicatorsDF['Low'] + keyTechnicalIndicatorsDF['Close']) / 3).astype(float).round(2)
        keyTechnicalIndicatorsDF['OHLC4'] = ((keyTechnicalIndicatorsDF['Open'] + keyTechnicalIndicatorsDF['High'] + \
                    keyTechnicalIndicatorsDF['Low'] + keyTechnicalIndicatorsDF['Close']) / 4).astype(float).round(2)
        keyTechnicalIndicatorsDF['Median Price'] = ((keyTechnicalIndicatorsDF['High'] +  keyTechnicalIndicatorsDF['Low']) / 2).astype(float).round(2)
        resetKTIColumnOrder = ['Ticker', 'Trade_Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Pivot Point', 'R1', 'R2', 'R3', 'R4', 'S1', 'S2', 'S3', 'S4', 'Typical Price', 'OHLC4', 'Median Price']
        keyTechnicalIndicatorsDF = keyTechnicalIndicatorsDF[resetKTIColumnOrder]
        st.space('small')
        st.subheader("Key Technical Indicators")
        st.dataframe(keyTechnicalIndicatorsDF, hide_index=True, column_config=hideColumns)

        # Sector & Industry KPIS
        currentDayTradeDF = historicalTradeDataDF.sort_values(by='Trade_Date', ascending=False).drop_duplicates(subset='Ticker', keep='first')
        
        advancingDecliningConditions = [
            (currentDayTradeDF['Close'] > currentDayTradeDF['Previous Close']),
            (currentDayTradeDF['Close'] < currentDayTradeDF['Previous Close'])
        ]
        advancingDecliningChoices = ['Advancing', 'Declining']
        
        currentDayTradeDF['Share Trend'] = np.select(advancingDecliningConditions, advancingDecliningChoices, default='No Change')
        dailyTradeStatusDF = pd.merge(currentDayTradeDF, finalStockCompanyInfoDF[['Ticker', 'Company Name', 'Sector', 'Industry']], on='Ticker', how='left')
        resetColumnOrder = ['Ticker', 'Trade_Date', 'Company Name', 'Sector', 'Industry', 'Open', 'High', 'Low', 'Close', 'Previous Close', 'Previous High', 'Previous Low', 'Previous Open', 'Volume', 'Share Trend']
        dailyTradeStatusDF = dailyTradeStatusDF[resetColumnOrder]
        advancesDeclinesDF = dailyTradeStatusDF.style.applymap(setStatusColor, subset=['Share Trend'])
        st.space('small')
        st.subheader("Advances and Declines")
        st.dataframe(advancesDeclinesDF, hide_index=True, column_config=hideColumns)
    except Exception as e:
        st.error("Error fetching Key Technical Indicators")
        traceback.print_exc()
        st.exception(e)

with companyMetrics:
    try:
        valuationRatios = st.session_state.valuationRatios 
        profitabilityMetrics = st.session_state.profitabilityMetrics 
        financialHealthLiquidity = st.session_state.financialHealthLiquidity 
        riskVolatility = st.session_state.riskVolatility 
        companyValues = st.session_state.companyValues 
        splitsDividendsDetail = st.session_state.splitsDividendsDetail 
        shares = st.session_state.shares 
        bidAskDetails = st.session_state.bidAskDetails 
        targetPricePrediction = st.session_state.targetPricePrediction 
        incomeStatement = st.session_state.incomeStatement

        segmentControlOptions = ["Value Ratios", "Profitability Metrics", "Financial Health Liquidity", "Risk Volatility", "Company Values", "Splits & Dividends", "Shares", "Bid & Ask", "Target Prices", "Income Statement"]

        stockAttributeGroupSC = st.segmented_control("Select attribute group to view metrics", options=segmentControlOptions, selection_mode="single")

        if stockAttributeGroupSC == "Value Ratios":
            valuationRatiosDF = pd.DataFrame(valuationRatios, columns=valuationRatiosColumns)
            st.dataframe(valuationRatiosDF, hide_index=True)
        elif stockAttributeGroupSC == "Profitability Metrics":
            profitabilityMetricsDF = pd.DataFrame(profitabilityMetrics, columns=profitabilityMetricsColumns)
            st.dataframe(profitabilityMetricsDF, hide_index=True)
        elif stockAttributeGroupSC == "Financial Health Liquidity":
            financialHealthLiquidityDF = pd.DataFrame(financialHealthLiquidity, columns=financialHealthLiquidityColumns)
            st.dataframe(financialHealthLiquidityDF, hide_index=True)
        elif stockAttributeGroupSC == "Risk Volatility":
            riskVolatilityDF = pd.DataFrame(riskVolatility, columns=riskVolatilityColumns)
            st.dataframe(riskVolatilityDF, hide_index=True)
        elif stockAttributeGroupSC == "Company Values":
            companyValuesDF = pd.DataFrame(companyValues, columns=companyValuesColumns)
            st.dataframe(companyValuesDF, hide_index=True)
        elif stockAttributeGroupSC == "Splits & Dividends":
            splitsDividendsDetailDF = pd.DataFrame(splitsDividendsDetail, columns=splitsDividendsDetailColumns)
            st.dataframe(splitsDividendsDetailDF, hide_index=True)
        elif stockAttributeGroupSC == "Shares":
            sharesDF = pd.DataFrame(shares, columns=sharesColumns)
            st.dataframe(sharesDF, hide_index=True)
        elif stockAttributeGroupSC == "Bid & Ask":
            bidAskDetailsDF = pd.DataFrame(bidAskDetails, columns=bidAskDetailsColumns)
            st.dataframe(bidAskDetailsDF, hide_index=True)
        elif stockAttributeGroupSC == "Target Prices":
            targetPricePredictionDF = pd.DataFrame(targetPricePrediction, columns=targetPricePredictionColumns)
            st.dataframe(targetPricePredictionDF, hide_index=True)
        elif stockAttributeGroupSC == "Income Statement":
            incomeStatementDF = pd.DataFrame(incomeStatement, columns=incomeStatementColumns)
            numericColumns = incomeStatementDF.select_dtypes(include=[np.number]).columns.tolist()

            for numericColumn in numericColumns:
                incomeStatementDF[numericColumn] = incomeStatementDF[numericColumn].apply(formatBigNumbers)

            incomeStatementInfo = st.popover(label="Income Statement Information")

            with incomeStatementInfo:
                st.write("The income statement attributes are derived from the dataframe returned by yfinance.Ticker(tickerSymbol).income_stmt")

            st.dataframe(incomeStatementDF, hide_index=True)
    except Exception as e:
        st.error("Error fetching Company metrics")
        traceback.print_exc()
        st.exception(e)

with sectorIndustry:
    try:
        historicalTradeDataDF = st.session_state.historicalLiveStockDataDF
        historicalTradeDataDF.sort_values(by=['Ticker', 'Trade_Date'], ascending=[True, True], inplace=True) # Sort values by Ticker and Trade Date ascending 
        historicalTradeDataDF.drop(['Adjusted_Close', 'Daily Return', 'Trade Range'], axis=1, inplace=True)
        historicalTradeDataDF["Trade_Date"] = pd.to_datetime(historicalTradeDataDF["Trade_Date"]).dt.date

        # Creating previous day's Open, High, Low and CLose values for calculating Key Technical Indicators
        historicalTradeDataDF['Previous Close'] = historicalTradeDataDF['Close'].shift(1)

        # hideColumns = {'Previous Close': None, 'Previous Open': None, 'Previous High': None, 'Previous Low': None}
        # Sector & Industry KPIS
        currentDayTradeDF = historicalTradeDataDF.sort_values(by='Trade_Date', ascending=False).drop_duplicates(subset='Ticker', keep='first')
        advancingDecliningConditions = [
            (currentDayTradeDF['Close'] > currentDayTradeDF['Previous Close']),
            (currentDayTradeDF['Close'] < currentDayTradeDF['Previous Close'])
        ]
        advancingDecliningChoices = ['Advancing', 'Declining']
        
        currentDayTradeDF['Share Trend'] = np.select(advancingDecliningConditions, advancingDecliningChoices, default='No Change')
        dailyTradeStatusDF = pd.merge(currentDayTradeDF, finalStockCompanyInfoDF[['Ticker', 'Company Name', 'Sector', 'Industry']], on='Ticker', how='left')
        resetColumnOrder = ['Ticker', 'Trade_Date', 'Company Name', 'Sector', 'Industry', 'Close', 'High', 'Low', 'Open', \
                        'Previous Close', 'Previous High', 'Previous Low', 'Previous Open', 'Volume', 'Share Trend']
        dailyTradeStatusDF = dailyTradeStatusDF[resetColumnOrder]
        validateColumn = 'No Change'

        sectorDetailsDF = dailyTradeStatusDF.groupby(['Sector', 'Share Trend']).size().reset_index(name='No of Companies')
        sectorDetailsDF = sectorDetailsDF.pivot(index='Sector', columns='Share Trend', values='No of Companies')
        sectorDetailsDF.reset_index(inplace=True)
        sectorDetailsDF['Advancing'] = sectorDetailsDF['Advancing'].fillna(0)
        sectorDetailsDF['Declining'] = sectorDetailsDF['Declining'].fillna(0)

        if validateColumn in sectorDetailsDF.columns:
            sectorDetailsDF['No Change'] = sectorDetailsDF['No Change'].fillna(0)
        else:
            sectorDetailsDF['No Change'] = 0

        sectorDetailsDF['Total Companies'] = sectorDetailsDF['Advancing'] + sectorDetailsDF['Declining'] + sectorDetailsDF['No Change']
        
        industryDetailsDF = dailyTradeStatusDF.groupby(['Industry', 'Share Trend']).size().reset_index(name='No of Companies')
        industryDetailsDF = industryDetailsDF.pivot(index='Industry', columns='Share Trend', values='No of Companies')
        industryDetailsDF.reset_index(inplace=True)
        industryDetailsDF['Advancing'] = industryDetailsDF['Advancing'].fillna(0)
        industryDetailsDF['Declining'] = industryDetailsDF['Declining'].fillna(0)

        if validateColumn in industryDetailsDF.columns:
            industryDetailsDF['No Change'] = industryDetailsDF['No Change'].fillna(0)
        else:
            industryDetailsDF['No Change'] = 0
        
        industryDetailsDF['Total Companies'] = industryDetailsDF['Advancing'] + industryDetailsDF['Declining']  + industryDetailsDF['No Change']
        
        if 'hasToggled' not in st.session_state:
            st.session_state.hasToggled = False
        if 'toggleLabel' not in st.session_state:
            st.session_state.toggleLabel = "Industries"

        toggleSectorIndustry = st.toggle(label=st.session_state.toggleLabel, key='hasToggled', value=False, on_change=onToggleChange)

        if toggleSectorIndustry:
            st.space('small')
            st.subheader("Sector Details")
            st.dataframe(sectorDetailsDF, hide_index=True)
        else:
            st.space('small')
            st.subheader("Industry Details")
            st.dataframe(industryDetailsDF, hide_index=True)
    except Exception as e:
        st.error("Error in fetching Sector and Industry data")
        traceback.print_exc()
        st.exception(e)