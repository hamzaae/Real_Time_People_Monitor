import pandas as pd


data = pd.read_json('data.json')
pd.DataFrame(data)

data['time'] = pd.to_datetime(data['time'])

melted_df = pd.melt(data, id_vars=['time'], var_name='zone', value_name='value')
melted_df['value'] = pd.to_numeric(melted_df['value'])
melted_df['id_zone'] = melted_df['zone'].apply(lambda x: int(x.split('_')[1]))
melted_df.drop(columns=['zone'], inplace=True)
result_df = melted_df.groupby(['time', 'id_zone']).agg({'value': ['max', 'mean', 'sum']}).reset_index()
result_df.columns = ['time', 'id_zone', 'max', 'mean', 'total']


print(result_df)
