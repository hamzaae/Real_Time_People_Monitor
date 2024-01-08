import pandas as pd

def process_ini_data(file_name):
    df = pd.read_json(file_name)
    print(df)


    # Calculate min, max, total, and average for each zone across all rows
    df_aggregated = pd.DataFrame({
        'zone_1_min': df['zone_1'].min(),
        'zone_1_max': df['zone_1'].max(),
        'zone_1_total': df['zone_1'].sum(),
        'zone_1_avg': df['zone_1'].mean(),

        'zone_2_min': df['zone_2'].min(),
        'zone_2_max': df['zone_2'].max(),
        'zone_2_total': df['zone_2'].sum(),
        'zone_2_avg': df['zone_2'].mean(),

        'zone_3_min': df['zone_3'].min(),
        'zone_3_max': df['zone_3'].max(),
        'zone_3_total': df['zone_3'].sum(),
        'zone_3_avg': df['zone_3'].mean(),

        'zone_4_min': df['zone_4'].min(),
        'zone_4_max': df['zone_4'].max(),
        'zone_4_total': df['zone_4'].sum(),
        'zone_4_avg': df['zone_4'].mean(),

        'zone_5_min': df['zone_5'].min(),
        'zone_5_max': df['zone_5'].max(),
        'zone_5_total': df['zone_5'].sum(),
        'zone_5_avg': df['zone_5'].mean(),

        'zone_6_min': df['zone_6'].min(),
        'zone_6_max': df['zone_6'].max(),
        'zone_6_total': df['zone_6'].sum(),
        'zone_6_avg': df['zone_6'].mean(),

        'zone_7_min': df['zone_7'].min(),
        'zone_7_max': df['zone_7'].max(),
        'zone_7_total': df['zone_7'].sum(),
        'zone_7_avg': df['zone_7'].mean()
    }, index=[0])

    print(df_aggregated)

    return df_aggregated

# Example usage
file_name = "media/data.json"
result_df = process_ini_data(file_name)
