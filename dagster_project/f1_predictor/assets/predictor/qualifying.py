import datetime
import os
import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.discord_utils import DiscordUtils
from sklearn.linear_model import LinearRegression
from utils.file_utils import FileUtils
import matplotlib.pyplot as plt

data_loc = os.getenv('DATA_STORE_LOC')


@asset(required_resource_keys={"mysql"})
def qualifying_training_data_from_sql(context: AssetExecutionContext,
                                      session_info: dict):
    query = FileUtils.file_to_query('get_training_data')

    query = query.replace('{round_num}', str(session_info['round_number']))
    query = query.replace('{year}', str(session_info['year']))

    context.log.info(f'Query to run: \n{query}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset(required_resource_keys={"mysql"})
def qualifying_session_data_from_sql(context: AssetExecutionContext,
                                     session_info: dict):
    query = FileUtils.file_to_query('get_test_data')

    query = query.replace('{round_num}', str(session_info['round_number']))
    query = query.replace('{year}', str(session_info['year']))

    context.log.info(f'Query to run: \n{query}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def create_qualifying_prediction_model(context: AssetExecutionContext,
                                       qualifying_training_data_from_sql: pd.DataFrame):
    df = qualifying_training_data_from_sql

    lr = LinearRegression()

    y = df['Q_TIME']
    x = df.drop('Q_TIME', axis=1)

    lr.fit(x, y)

    return Output(
        value=lr
    )


@asset()
def create_qualifying_prediction(context: AssetExecutionContext,
                                 create_qualifying_prediction_model: LinearRegression,
                                 qualifying_session_data_from_sql: pd.DataFrame):
    lr = create_qualifying_prediction_model
    df = qualifying_session_data_from_sql

    predict_df = pd.DataFrame()
    for i in range(len(df)):
        vals = pd.DataFrame()
        temp = df.iloc[i].to_frame().transpose()
        predicted_time = lr.predict(temp.drop(['DRIVER_ID', 'CONSTRUCTOR_COLOUR'], axis=1))
        vals['DRIVER_ID'] = temp['DRIVER_ID']
        vals['CONSTRUCTOR_COLOUR'] = temp['CONSTRUCTOR_COLOUR']
        vals['PREDICTED_LAPTIME'] = predicted_time
        predict_df = pd.concat([predict_df, vals])

    predict_df = predict_df.sort_values('PREDICTED_LAPTIME')
    predict_df['PREDICTED_POS'] = predict_df['PREDICTED_LAPTIME'].rank(method='first')

    return Output(
        value=predict_df,
        metadata={
            'Markdown': MetadataValue.md(predict_df.head().to_markdown()),
            'Rows': len(predict_df)
        }
    )


@asset(io_manager_key='sql_io_manager', key_prefix=['PREDICTION', 'QUALIFYING_PREDICTION_DATA'])
def qualifying_prediction_data_to_sql(context: AssetExecutionContext,
                                      create_qualifying_prediction: pd.DataFrame,
                                      session_info: dict):
    event_cd = f"{session_info['year']}{session_info['round_number']}"
    df = create_qualifying_prediction

    df.drop(columns=['PREDICTED_POS', 'CONSTRUCTOR_COLOUR'], inplace=True)

    df.loc[:, 'EVENT_CD'] = event_cd

    df.rename(columns={'PREDICTED_LAPTIME': 'PREDICTED_LAPTIME_Q'}, inplace=True)

    df.loc[:, 'LOAD_TS'] = datetime.datetime.now()

    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )


@asset()
def create_qualifying_prediction_img(context: AssetExecutionContext,
                                     create_qualifying_prediction: pd.DataFrame,
                                     sql_driver_data: pd.DataFrame,
                                     session_info: dict):
    df = create_qualifying_prediction
    file_name = f'{session_info["round_number"]}.png'

    df = pd.merge(df, sql_driver_data, how='left', on='DRIVER_ID')

    output_df = pd.DataFrame()
    output_df['Driver'] = df['QUALI_CD']
    output_df['Predicted Position'] = df['PREDICTED_POS'].astype(int).astype(str)
    output_df['Predicted Time'] = df['PREDICTED_LAPTIME'].astype(float).round(3)
    output_df['CONSTRUCTOR_COLOUR'] = df['CONSTRUCTOR_COLOUR']
    if not os.path.exists(data_loc + f'{session_info["year"]}/'):
        os.makedirs(data_loc + f'{session_info["year"]}/')

    save_loc = data_loc + f'{session_info["year"]}/' + file_name

    # Sort by predicted time from smallest to largest
    output_df = output_df.sort_values('Predicted Time', ascending=False)

    fig, ax = plt.subplots(figsize=(10, 7.5))

    # Create horizontal bar chart
    bars = ax.barh(range(len(output_df)), output_df['Predicted Time'],
                   color=output_df['CONSTRUCTOR_COLOUR'],
                   edgecolor='black',
                   linewidth=0.5)

    # Add value labels on bars
    for i, (idx, row) in enumerate(output_df.iterrows()):
        time = row['Predicted Time']
        ax.text(time + 0.05, i, f" {row['Driver']} - {time}s",
               va='center', fontsize=10)

    # Styling
    ax.set_xlabel('Predicted Time (s)', fontsize=20, color='#15151E')
    ax.set_title('F1 Qualifying Laptime Prediction\n' + f"Round {session_info['round_number']}",
                fontsize=24, color='#15151E', fontweight='bold')

    # Remove y-axis labels and ticks
    ax.set_yticks([])

    # Set x-axis range
    x_min = output_df['Predicted Time'].min() - 0.25
    x_max = output_df['Predicted Time'].max() + 1.5
    ax.set_xlim(x_min, x_max)

    # Styling
    ax.set_facecolor('#f7f7f7')
    fig.patch.set_facecolor('white')
    ax.spines['top'].set_visible(True)
    ax.spines['right'].set_visible(True)
    ax.spines['left'].set_color('#15151E')
    ax.spines['bottom'].set_color('#15151E')
    ax.tick_params(colors='#15151E')

    plt.tight_layout()
    plt.savefig(save_loc, dpi=100, bbox_inches='tight')
    plt.close()

    return Output(value=save_loc,
                  metadata={
                      'File Name': file_name
                  }
                  )


@asset()
def send_qualifying_prediction_discord(context: AssetExecutionContext,
                                       create_qualifying_prediction_img: str,
                                       session_info: dict):
    year = session_info['year']
    round_number = session_info['round_number']

    dis = DiscordUtils()

    dis.send_message(message=f'New qualifying prediction is available for Round {round_number} - {year}!\n'
                             f'This is not gambling advice!',
                     attachment=[create_qualifying_prediction_img])
    return
