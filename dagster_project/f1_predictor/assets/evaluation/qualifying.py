from utils.discord_utils import DiscordUtils
from utils.file_utils import FileUtils
from dagster import asset, Output, AssetExecutionContext, MetadataValue
import pandas as pd
from utils.file_utils import FileUtils
import matplotlib.pyplot as plt
import os

data_loc = os.getenv('DATA_STORE_LOC')

@asset(required_resource_keys={"mysql"})
def get_qualifying_evaluation_data(context: AssetExecutionContext,
                                   session_info: dict):
    query = FileUtils.file_to_query('get_quali_eval_data')

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
def create_qualifying_position_evaluation_img(context: AssetExecutionContext,
                                              get_qualifying_evaluation_data: pd.DataFrame,
                                              session_info: dict):
    df = get_qualifying_evaluation_data

    file_name = f'{session_info["round_number"]}_laptime_eval.png'
    save_loc = data_loc + f'{session_info["year"]}/' + file_name

    fig, ax = plt.subplots(figsize=(7.5, 7))

    # Create scatter plot with colors from CONSTRUCTOR_COLOUR
    scatter = ax.scatter(df['PREDICTED_POSITION'],
                         df['ACTUAL_POSITION'],
                         c=df['CONSTRUCTOR_COLOUR'],
                         s=100,
                         alpha=0.7,
                         edgecolors='black',
                         linewidth=0.5)

    # Add driver text labels
    for idx, row in df.iterrows():
        ax.annotate(row['DRIVER'],
                   (row['PREDICTED_POSITION'], row['ACTUAL_POSITION']),
                   textcoords="offset points",
                   xytext=(0, 10),
                   ha='center',
                   fontsize=8)

    # Add diagonal reference line
    ax.plot([0, 20], [0, 20], 'grey', linestyle='--', linewidth=1, alpha=0.25, zorder=0)

    # Styling
    ax.set_xlabel('Predicted Position', fontsize=16, color='#15151E')
    ax.set_ylabel('Actual Position', fontsize=16, color='#15151E')
    ax.set_title('F1 Qualifying Prediction Accuracy\n' + f"Round {session_info['round_number']}",
                 fontsize=20, color='#15151E', fontweight='bold')

    # Invert y-axis to match original (reversed range)
    ax.invert_yaxis()

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
def create_qualifying_laptime_evaluation_img(context: AssetExecutionContext,
                                             get_qualifying_evaluation_data: pd.DataFrame,
                                             session_info: dict):
    df = get_qualifying_evaluation_data

    file_name = f'{session_info["round_number"]}_position_eval.png'
    save_loc = data_loc + f'{session_info["year"]}/' + file_name

    df.sort_values(by='ABS_LAPTIME_DIFFRENCE', inplace=True)

    fig, ax = plt.subplots(figsize=(10, 7.5))

    # Create bar chart with colors from CONSTRUCTOR_COLOUR
    bars = ax.bar(range(len(df)), df['LAPTIME_DIFFRENCE'],
                  color=df['CONSTRUCTOR_COLOUR'],
                  edgecolor='black',
                  linewidth=0.5)

    # Add value labels on bars
    for i, (idx, row) in enumerate(df.iterrows()):
        height = row['LAPTIME_DIFFRENCE']
        ax.text(i, height + 0.1, f"{row['DRIVER']}\n{height:.2f}s",
               ha='center', va='bottom', fontsize=9)

    # Styling
    ax.set_ylabel('Laptime Difference (s)', fontsize=16, color='#15151E')
    ax.set_title('F1 Qualifying Laptime Prediction Accuracy\n' + f"Round {session_info['round_number']}",
                 fontsize=20, color='#15151E', fontweight='bold')

    # Remove x-axis labels and ticks
    ax.set_xticks([])

    # Set y-axis range
    y_min = df['LAPTIME_DIFFRENCE'].min() - 0.5
    y_max = df['LAPTIME_DIFFRENCE'].max() + 0.5
    ax.set_ylim(y_min, y_max)

    # Styling
    ax.set_facecolor('#f7f7f7')
    fig.patch.set_facecolor('white')
    ax.spines['top'].set_visible(True)
    ax.spines['right'].set_visible(True)
    ax.spines['left'].set_color('#15151E')
    ax.spines['bottom'].set_color('#15151E')
    ax.tick_params(colors='#15151E')

    plt.tight_layout()
    plt.savefig(save_loc, dpi=600, bbox_inches='tight')
    plt.close()

    return Output(value=save_loc,
                  metadata={
                      'File Name': file_name
                  }
                  )


@asset()
def send_qualifying_evaluation_discord(context: AssetExecutionContext,
                                       create_qualifying_laptime_evaluation_img: str,
                                       create_qualifying_position_evaluation_img: str,
                                       session_info: dict):
    year = session_info['year']
    round_number = session_info['round_number']

    dis = DiscordUtils()

    dis.send_message(message=f'This is how well the prediction performed for Round {round_number} - {year}!',
                     attachment=[create_qualifying_position_evaluation_img, create_qualifying_laptime_evaluation_img])
    return
