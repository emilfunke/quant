import databento
import requests
from io import StringIO
import pandas as pd
import os

def fetch_msci_constituents() -> pd.DataFrame:
    url = 'https://www.ishares.com/us/products/239696/ishares-msci-world-etf/1467271812596.ajax?fileType=csv&fileName=URTH_holdings&dataType=fund'
    response = requests.get(url)
    data = StringIO(response.text)
    df = pd.read_csv(data, skiprows=9)
    return df


def get_from_databento() -> None:
    """
    get the MSCI world constituents and downloads from Databento
    """
    msci_constituents = fetch_msci_constituents()
    equities = msci_constituents[msci_constituents['Asset Class'] == 'Equity']
    tickers = equities['Ticker'].dropna().str.strip().unique().tolist()

    client =databento.Historical(key='API KEY')

    details = client.batch.submit_job(
        encoding='csv',
        dataset="XNYS.PILLAR",
        symbols=tickers,
        schema="OHLCV-1h",
        start="2018-05-01T00:00:00",
        end="2025-05-28T00:00:00",
    )
    client.batch.download(
        job_id=details['id'],
        output_dir="./data",
    )

def read_databento(job_id: str) -> pd.DataFrame:
    """
    Reads and combines data downloaded from Databento.
    Args:
        job_id (str): The job ID for the Databento data.
    Returns:
        pd.DataFrame: Combined DataFrame with the downloaded data.
    """
    path = f'./data/'
    files = [f for f in os.listdir(path) if f.endswith('.zst')]
    data_frames = []
    for file in files:
        df = pd.read_csv(path+file, compression='zstd')
        data_frames.append(df)
    combined_df = pd.concat(data_frames, ignore_index=True)
    combined_df['time'] = pd.to_datetime(combined_df['ts_event'], unit='ns')
    combined_df.set_index('time', inplace=True)
    combined_df = combined_df.sort_index(inplace=False)
    combined_df = combined_df.sort_values(by=['instrument_id'])
    return combined_df

def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches the DataFrame containig Databento historical stock data with additional columns.
    Args:
        df (pd.DataFrame): The DataFrame to enrich.
    Returns:
        pd.DataFrame: Enriched DataFrame.
    """
    return df

def hourly_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts hourly data to daily data by resampling.
    Args:
        df (pd.DataFrame): The DataFrame with hourly data.
    Returns:
        pd.DataFrame: DataFrame with daily data.
    """
    df = df.sort_values(['instrument_id', 'time'])
    df_daily = (
        df
        .groupby('instrument_id')
        .resample('1D')
        .agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        .dropna()
        .reset_index()
    )
    return df_daily