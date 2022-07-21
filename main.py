from types import FunctionType
from typing import Any, List
from pandas import Series, DataFrame


"""FUNCOES UTEIS"""

def map(df: DataFrame, col_list: list):
    return df.loc[:, col_list]

def delete_colunms(df: DataFrame, col_list: list):
    return df.drop(columns=col_list)

def show_dataframe(df: DataFrame):
    from IPython.display import display
    display(df)
    return df

def load_excel(df: DataFrame, filename: str, col_list: list = None, header: bool = True):
    df.to_excel(f'./../Output/{filename}', index=False, columns=col_list, header=header)
    return df

def load_file(df: DataFrame, filename: str, delimiter: str, col_list: list = None, header: bool = True):
    df.to_csv(f'./../Output/{filename}', index=False, sep=delimiter, columns=col_list, header=header)
    return df

def order_by(df: DataFrame, col_list, ascending: bool = True):
    return df.sort_values(col_list, ascending=ascending)

def create_colunm(df: DataFrame, colunm_name: str, default_value: Any = ''):
    df = df.copy()
    df.insert(len(df.columns), colunm_name, default_value)
    return df

def convert_colunm_datatype(df: DataFrame, colunm_name: str, datatype: Any = 'string'):
    df = df.copy()
    df[colunm_name] = df[colunm_name].astype(datatype)
    return df 

def distinct(df: DataFrame, col_list: List[str]):
    df = df.copy().loc[:, col_list]
    return df.drop_duplicates()

def filter(df: DataFrame, condition: Series):
    return df[condition]

def for_each(df: DataFrame, result_serie_name: str, fun: FunctionType):
    """Aplica a função informada a nível de linha. Para cada vez que for executada, o resultado da função sera retornado na coluna {result_serie_name}"""
    df_copy = df.copy()
    for index, row in df_copy.iterrows():
        df_copy.loc[index, result_serie_name] = fun(row)
    return df_copy

def inner_join(df_a: DataFrame, df_b: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_b, left_on=df_a_keys, right_on=df_b_keys, how='inner')

def left_join(df_a: DataFrame, df_b: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_b, left_on=df_a_keys, right_on=df_b_keys, how='left')

def right_join(df_a: DataFrame, df_b: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_b, left_on=df_a_keys, right_on=df_b_keys, how='right')

def full_join(df_a: DataFrame, df_b: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_b, left_on=df_a_keys, right_on=df_b_keys, how='outer')

def self_inner_join(df_a: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_a, left_on=df_a_keys, right_on=df_b_keys, how='inner')

def self_left_join(df_a: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_a, left_on=df_a_keys, right_on=df_b_keys, how='left')

def self_right_join(df_a: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_a, left_on=df_a_keys, right_on=df_b_keys, how='right')

def self_full_join(df_a: DataFrame, df_a_keys: List[str], df_b_keys: List[str]):
    return pd.merge(df_a, df_a, left_on=df_a_keys, right_on=df_b_keys, how='outer')

def union(df_a: DataFrame, df_b: DataFrame):
    return pd.concat([df_a, df_b])

def rows_quantity(df: pd.DataFrame):
    display(len(df))
    return df

def query(df: DataFrame, expr: str):
    return df.query(expr)

def rename_columns(df: DataFrame, column_name: dict):
    df = df.copy()
    df.rename(columns=column_name, inplace=True)
    return df
