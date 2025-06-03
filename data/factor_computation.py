import pandas as pd
from scipy.stats import zscore

def compute_momentum(df_daily: pd.DataFrame, lag_days: int = 21, lookback_days : int = 252) -> pd.DataFrame:
    df = df_daily.copy()
    df = df.sort_values(['instrument_id', 'time'])
    df['momentum'] = df.groupby('instrument_id')['close'].transform(
        lambda x: x.shift(lag_days) / x.shift(lag_days) - 1
    )
    return df

def compute_volatility(df_daily: pd.DataFrame, lookback_days: int = 21) -> pd.DataFrame:
    df = df_daily.copy()
    df = df.sort_values(['instrument_id', 'time'])
    df['volatility'] = df.groupby('instrument_id')['close'].transform(
        lambda x: x.pct_change().rolling(window=lookback_days).std()
    )
    return df

def compute_size(df_daily: pd.DataFrame, window=21) -> pd.DataFrame:
    df = df_daily.copy()
    df = df.sort_values(['instrument_id', 'time'])
    df['size'] = df.groupby('instrument_id')['volume'].transform(
        lambda x: x.rolling(window=window).mean()
    )
    return df

def normalise_factors(df: pd.DataFrame, factor_columns: list) -> pd.DataFrame:
    df_normalized = df.copy()
    for column in factor_columns:
        df_normalized[f'{column}_z'] = zscore(df[column], nan_policy='omit')
    return df_normalized

def compute_all_daily_factors(df_daily: pd.DataFrame) -> pd.DataFrame:
    df = df_daily.copy()
    df = compute_momentum(df)
    df = compute_volatility(df)
    df = compute_size(df)
    
    factor_columns = ['momentum', 'volatility', 'size']
    df = normalise_factors(df, factor_columns)
    
    return df