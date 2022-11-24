import time
from datetime import datetime, timedelta
import requests
from extract.console import console

import pandas as pd

url = 'https://www.resultadofacil.com.br/resultados-caminho-da-sorte-do-dia-{}'
headers = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def extract_tables(start_date, end_date):
    """
    :param start_date: (str) beginning date of scrape
    :param end_date: (str) end date of scrape
    :return: (pandas.core.frame.DataFrame) dataframe with all results
    """

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    diff = end_dt - start_dt

    range_date = [start_dt + timedelta(days=n) for n in range(diff.days)]

    all_dfs = list()
    for dt in range_date:
        dt_str = dt.strftime('%Y-%m-%d')
        console.log(f'Tabelas do dia {dt_str}...')

        r = requests.get(url.format(dt_str), headers=headers)
        tables = pd.read_html(r.text)

        # "sorteio" column
        tables_ = [
            table.assign(sorteio=i + 1) for i, table in enumerate(tables)
        ]

        df_of_day = pd.concat(tables_)
        df_of_day['data'] = dt_str
        df_of_day.to_csv(f'src/datasets/{dt_str}.csv', sep=';', index=False)

        all_dfs.append(df_of_day)

        time.sleep(5)

    df_results = pd.concat(all_dfs)
    df_results.to_csv(
        f'src/datasets/results_{start_date}_{end_date}.csv',
        sep=';',
        index=False
    )
    console.log('Tarefa concluída. :white_check_mark:')
    return df_results
