from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

# @app.get("/dataframe")
# async def get_dataframe():
#     data = {"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
#     df = pd.DataFrame(data)
#     return df.to_dict(orient="records")

@app.get("/stocks")
async def getStocks():
    stockData = pd.read_csv("DataSheetStocksFinal.csv")
    medians = pd.read_csv("Final.csv")
    final_data = pd.merge(stockData, medians, on=['Industry', 'Category'])
    final_data.drop("Unnamed: 0", axis=1, inplace=True)

    final_data['PE Score'] = (final_data['PE Median'] - final_data['P/E']) / final_data['PE Median']
    final_data['PS Score'] = (final_data['PS Median'] - final_data['CMP / Sales']) / final_data['PS Median']
    final_data['EV/EBIT Score'] = (final_data['EV / EBIT Median'] - final_data['EV / EBIT']) / final_data[
        'EV / EBIT Median']
    final_data['CMP/BV Score'] = (final_data['CMP / BV Median'] - final_data['CMP / BV']) / final_data[
        'CMP / BV Median']

    final_data['CMP/OCF Score'] = final_data.apply(
        lambda x: (x['CMP / OCF Median'] - x['CMP / OCF']) / x['CMP / OCF Median'] if x['CMP / OCF'] > 0 else (x[
                                                                                                                   'CMP / OCF'] -
                                                                                                               x[
                                                                                                                   'CMP / OCF Median']) /
                                                                                                              x[
                                                                                                                  'CMP / OCF Median'],
        axis=1)

    # final_data['CMP/OCF Score'] = (final_data['CMP / OCF Median']-final_data['CMP / OCF'])/final_data['CMP / OCF Median']
    final_data['Sales Score'] = (final_data['Sales growth %'] - final_data['Sales growth % Median']) / final_data[
        'Sales growth % Median']
    final_data['Profit Score'] = (final_data['Profit growth %'] - final_data['Profit growth % Median']) / final_data[
        'Profit growth % Median']
    final_data['OPM Score'] = (final_data['OPM %'] - final_data['OPM']) / final_data['OPM']
    final_data['EPS Score'] = final_data.apply(
        lambda x: (x['EPS Growth'] - x['EPS Growth_y']) / x['EPS Growth_y'] if x['EPS Growth_y'] != 0 else 0, axis=1)
    final_data['A/L Score'] = (final_data['Assets/Liabilities'] - final_data['Assets/Liab']) / final_data['Assets/Liab']
    final_data['FCF Score'] = (final_data['Free Cash Flow Rs.Cr.'] - final_data['Free Cash Flow']) / final_data[
        'Free Cash Flow']
    final_data['PEG Score'] = (final_data['PEG'] - final_data['PEG_y']) / final_data['PEG_y']

    final_data['D/E Score'] = final_data.apply(
        lambda x: (x['Debt/Eq.'] - x['Debt / Eq']) / x['Debt/Eq.'] if x['Debt/Eq.'] != 0 else 0, axis=1)
    # final_data['D/E Score'] = (final_data['Debt/Eq.'] - final_data['Debt / Eq'])/final_data['Debt/Eq.']
    final_data['ICR Score'] = (final_data['Int Coverage'] - final_data['Int. Cvg R']) / final_data['Int. Cvg R']
    # TODO
    final_data['W/C Sales Score'] = (final_data['WC to Sales %'] - final_data['W/C Sales']) / final_data['W/C Sales']
    final_data['Asset Score'] = (final_data['Asset Turnover'] - final_data['Asset Turnover_y']) / final_data[
        'Asset Turnover_y']
    final_data['Div Yld Score'] = final_data.apply(
        lambda x: (x['Div Yld %'] - x['Div Yld']) / x['Div Yld'] if x['Div Yld'] != 0 else 0, axis=1)
    ROA_Avg = final_data['ROA 12M %'].mean()
    final_data['ROA Score'] = (final_data['ROA 12M %'] - ROA_Avg) / ROA_Avg

    DII_Hold_Avg = final_data['DII Hold %'].mean()
    FII_Hold_Avg = final_data['FII Hold %'].mean()

    final_data['DII Score'] = (final_data['DII Hold %'] - DII_Hold_Avg) / DII_Hold_Avg
    final_data['FII Score'] = (final_data['FII Hold %'] - FII_Hold_Avg) / FII_Hold_Avg
    final_data['return on cash flow score'] = final_data['return on cash flow %'] / 10
    LONG_TERM_GROWTH = final_data['Long Term Growth'].mean()
    final_data['Long Term Growth Score'] = (final_data['Long Term Growth'] - LONG_TERM_GROWTH) / LONG_TERM_GROWTH
    final_data['Piotski Score'] = final_data['Piotski Scr'] / 10

    cols_to_normalize = ['Sales Score', 'Profit Score', 'OPM Score', 'EPS Score', 'FCF Score', 'Asset Score',
                         'Div Yld Score', 'DII Score', 'FII Score', 'return on cash flow score',
                         'Long Term Growth Score', 'Piotski Score', 'ROA Score']
    final_data[cols_to_normalize] = final_data[cols_to_normalize].apply(lambda x: (x - x.mean()) / x.std())

    final_data['Score'] = final_data.iloc[:, 65:].sum(axis=1)
    # print(final_data.head())
    final_data.sort_values(by=['Score'], ascending=False, inplace=True)

    cols_to_normalize = ['PE Score', 'PS Score', 'EV/EBIT Score', 'CMP/BV Score', 'CMP/OCF Score', 'Sales Score',
                         'Profit Score', 'OPM Score', 'EPS Score', 'A/L Score', 'FCF Score', 'PEG Score', 'D/E Score',
                         'ICR Score', 'W/C Sales Score', 'Asset Score', 'Div Yld Score', 'DII Score', 'FII Score',
                         'return on cash flow score', 'Long Term Growth Score', 'Piotski Score', 'ROA Score']

    Q1 = final_data[cols_to_normalize].quantile(0.25)
    Q3 = final_data[cols_to_normalize].quantile(0.75)
    IQR = Q3 - Q1

    from pyod.models.abod import ABOD

    final_data.dropna(inplace=True)
    print(final_data)
    X = final_data[cols_to_normalize].values
    # X.dropna()
    abod = ABOD()
    abod.fit(X)
    outlier_scores = abod.decision_function(X)
    final_data["abod_outlier_score"] = outlier_scores
    threshold = final_data["abod_outlier_score"].quantile(0.90)
    outliers = final_data[final_data["abod_outlier_score"] > threshold]
    print("Number of detected outliers:", len(outliers))
    final_data.drop(outliers.index, inplace=True)
    non_outliers = final_data[~final_data.index.isin(outliers.index)]

    non_outliers.to_csv('FinalStocks.csv')
    non_outliers = non_outliers.fillna('')
    # final_data = final_data.fillna('')

    return non_outliers.to_dict(orient="records")

@app.get('/stocks/value')
async def getStocksValue():
    value = pd.read_csv('FinalStocks.csv')
    value.drop("Unnamed: 0", axis=1, inplace=True)
    value['ValueScore'] = 17.5*value['PE Score'] + 10*value['PS Score'] + 15*value['EV/EBIT Score'] + 10*value['CMP/BV Score'] + 10*value['CMP/OCF Score']+ 5*value['FCF Score'] + 7.5*value['PEG Score'] + 7.5*value['D/E Score'] + 7.5*value['ICR Score'] + 10*value['ROA Score']
    value.sort_values(by=['ValueScore'], ascending=False, inplace=True)
    value_sorted = value.fillna('')
    return value_sorted.to_dict(orient="records")

@app.get('/stocks/income')
async def getStocksIncome():
    income = pd.read_csv('FinalStocks.csv')
    income.drop("Unnamed: 0", axis=1, inplace=True)
    income['IncomeScore'] = 10*income['PE Score'] + 10*income['PS Score'] + 15*income['Sales Score'] + 15*income['Profit Score'] + 10*income['OPM Score']+ 10*income['A/L Score'] + 10*income['D/E Score'] + 15*income['Div Yld Score'] + 5*income['ICR Score']
    income.sort_values(by=['IncomeScore'], ascending=False, inplace=True)
    income_sorted = income.fillna('')
    return income_sorted.to_dict(orient="records")

@app.get('/stocks/growth')
async def getStocksGrowth():
    growth = pd.read_csv('FinalStocks.csv')
    growth.drop("Unnamed: 0", axis=1, inplace=True)
    growth['GrowthScore'] = 10 * growth['PE Score'] + 10 * growth['PS Score'] + 20 * growth['Sales Score'] + 15 * \
                            growth['OPM Score'] + 10 * growth['Profit Score'] + 15 * growth['PEG Score'] + 10 * growth[
                                'Asset Score'] + 5 * growth['ROA Score'] + 5 * growth['EPS Score']
    growth.sort_values(by=['GrowthScore'], ascending=False, inplace=True)
    growth_sorted = growth.fillna('')
    return growth_sorted.to_dict(orient="records")

@app.get('/stocks/quality')
async def getStocksQuality():
    quality = pd.read_csv('FinalStocks.csv')
    quality.drop("Unnamed: 0", axis=1, inplace=True)
    quality['qualityScore'] = 10 * quality['PE Score'] + 15 * quality['EV/EBIT Score'] + 10 * quality['CMP/BV Score'] + 5 * \
                            quality['ICR Score'] + 10 * quality['W/C Sales Score'] + 10 * quality['Asset Score'] + 10 * quality[
                                'D/E Score'] + 15 * quality['ROA Score'] + 10 * quality['CMP/OCF Score'] + 5*quality['Sales Score'] + \
                              5+quality['OPM Score']
    quality.sort_values(by=['qualityScore'], ascending=False, inplace=True)
    quality_sorted = quality.fillna('')
    return quality_sorted.to_dict(orient="records")

@app.get('/stocks/dividend')
async def getStocksDividend():
    dividend = pd.read_csv('FinalStocks.csv')
    dividend.drop("Unnamed: 0", axis=1, inplace=True)
    dividend['dividendScore'] = 10 * dividend['CMP/BV Score'] + 10 * dividend['PS Score'] + 10 * dividend['Sales Score'] + 10 * \
                            dividend['OPM Score'] + 10 * dividend['Profit Score'] + 10 * dividend['OPM Score'] + 10 * dividend[
                                'A/L Score'] + 15 * dividend['FCF Score'] + 10 * dividend['D/E Score'] + 15*dividend['Div Yld Score']
    dividend.sort_values(by=['dividendScore'], ascending=False, inplace=True)
    dividend_sorted = dividend.fillna('')
    return dividend_sorted.to_dict(orient="records")