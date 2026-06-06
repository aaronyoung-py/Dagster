from utils.discord_utils import DiscordUtils
from utils.file_utils import FileUtils
from dagster import asset, Output, AssetExecutionContext, MetadataValue
import pandas as pd
from utils.file_utils import FileUtils
import plotly.graph_objects as go
import os
import plotly.io as pio

data_loc = os.getenv('DATA_STORE_LOC')
pio.get_chrome()

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

    fig = go.Figure(data=[go.Scatter(x=df['PREDICTED_POSITION'],
                                     y=df['ACTUAL_POSITION'],
                                     mode='markers+text',
                                     text=df['DRIVER'],
                                     textposition="top center",
                                     marker=dict(color=df['CONSTRUCTOR_COLOUR'])
                                     )
                          ],
                    layout=dict(
                        title=dict(text="<B>F1 Qualifying Prediction Accuracy<B>",
                                   font=dict(color="#15151E",
                                             size=20),
                                   subtitle=dict(text=f"Round {session_info['round_number']}",
                                                 font=dict(color="#15151E",
                                                           size=16))),
                        xaxis=dict(title=dict(text='Predicted Posistion',
                                              font=dict(color="#15151E",
                                                        size=16)),
                                   zeroline=False,
                                   showgrid=False,
                                   mirror=True,
                                   ticks='outside',
                                   showline=True,
                                   linecolor="#15151E"
                                   ),
                        yaxis=dict(
                            title=dict(text='Actual Posistion',
                                       font=dict(color="#15151E",
                                                 size=16)),
                            showgrid=False,
                            zeroline=False,
                            mirror=True,
                            ticks='outside',
                            showline=True,
                            linecolor="#15151E",
                            autorange="reversed"
                        ),
                        width=750,
                        height=700,
                        margin=dict(l=100,
                                    r=100,
                                    b=100,
                                    t=100),
                        plot_bgcolor='#f7f7f7'
                    )
                    )

    fig.add_shape(type="line",
                  x0=0,
                  y0=0,
                  x1=20,
                  y1=20,
                  opacity=0.25,
                  layer='below',
                  line=dict(color="grey",
                            width=2,
                            dash="dot"
                            )
                  )

    fig.write_image(save_loc)

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

    fig = go.Figure(data=[go.Bar(x=df['DRIVER'],
                                 y=df['LAPTIME_DIFFRENCE'],
                                 texttemplate="%{x}<br>%{y}s",
                                 textposition="outside",
                                 marker_color=df['CONSTRUCTOR_COLOUR'])],
                    layout=dict(title=dict(text="<B>F1 Qualifying Laptime Prediction Accuracy<B>",
                                           font=dict(color="#15151E", size=20),
                                           subtitle=dict(text=f"Round {session_info['round_number']}",
                                                         font=dict(color="#15151E",
                                                                   size=16))),
                                xaxis=dict(title=dict(text=''),
                                           showgrid=False,
                                           mirror=True,
                                           ticks='',
                                           showticklabels=False,
                                           showline=True,
                                           linecolor="#15151E"
                                           ),
                                yaxis=dict(
                                    title=dict(text=''),
                                    showgrid=False,
                                    mirror=True,
                                    ticks='',
                                    showticklabels=False,
                                    showline=True,
                                    linecolor="#15151E",
                                    range=[df['LAPTIME_DIFFRENCE'].min() - 0.5,
                                           df['LAPTIME_DIFFRENCE'].max() + 0.5]
                                ),
                                width=1000,
                                height=750,
                                margin=dict(l=60,
                                            r=60,
                                            b=100,
                                            t=100,
                                            pad=10),
                                plot_bgcolor='#f7f7f7'
                                )
                    )

    fig.write_image(save_loc, scale=6)

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
