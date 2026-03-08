import pandas as pd
from dagster import (sensor,
                     RunRequest,
                     SkipReason,
                     DagsterRunStatus,
                     RunsFilter,
                     SensorEvaluationContext)
from utils.file_utils import FileUtils
from fastf1.core import DataNotLoadedError
from datetime import datetime, timedelta, date
from .jobs import *


@sensor(job=practice_data_load_job,
        minimum_interval_seconds=300,
        required_resource_keys={'fastf1', 'mysql'})
def practice_data_load_sensor(context: SensorEvaluationContext):
    today = datetime.utcnow()

    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["SESSION_ONE_TYPE"]}')

    if next_event_df.empty:
        return SkipReason('No next event data available')

    if next_event_df['SESSION_ONE_DT'].date() > today.date():
        return SkipReason('Session 1 is after today its on {}'.format(next_event_df['SESSION_ONE_DT']))

    if next_event_df['SESSION_THREE_DT'].date() < today.date():
        return SkipReason('Session 3 is before today its on {}'.format(next_event_df['SESSION_THREE_DT']))

    if next_event_df['EVENT_TYPE_CD'] == 1:
        sessions = ['SESSION_ONE', 'SESSION_TWO', 'SESSION_THREE']
    elif next_event_df['EVENT_TYPE_CD'] == 2:
        sessions = ['SESSION_ONE']
    else:
        raise Exception('Unexpected EVENT_TYPE_CD {} in Event {} - {}'.format(next_event_df['EVENT_TYPE_CD'],
                                                                              next_event_df['EVENT_CD'],
                                                                              next_event_df['EVENT_NAME']))

    session_df = pd.DataFrame()
    for session in sessions:
        session_df = pd.concat([session_df,
                                pd.DataFrame({'session_num': [session],
                                              'session_time': [next_event_df[f'{session}_DT']],
                                              'session_name': [next_event_df[f'{session}_TYPE']]})],
                               ignore_index=True)

    next_session = session_df[session_df['session_time']
                              >
                              today - timedelta(hours=3)]

    if len(next_session) == 0:
        return SkipReason(f"{next_event_df['EVENT_NAME']} has no more practice sessions to load")
    else:
        next_session = next_session.iloc[0]

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}':
        return SkipReason(f"{next_event_df['ROUND_NUMBER']} - {next_session['session_name']} has already been loaded!")

    session_time = next_session['session_time']
    session_time_modified = (session_time + timedelta(hours=1.5))

    if session_time_modified < today:
        try:
            api_data = context.resources.fastf1.get_practice_results(year=next_event_df['EVENT_YEAR'],
                                                                     round_number=next_event_df['ROUND_NUMBER'],
                                                                     practice_num=next_session['session_name'][-1],
                                                                     drivers=False).copy()

            drivers = pd.unique(api_data['Driver'])
            if len(drivers) <= 1:
                return SkipReason("Session data is not available as there is no drivers in the data")
        except KeyError:
            return SkipReason("Session data is not available (KeyError)")
        except DataNotLoadedError:
            return SkipReason("Session data is not available (DataNotLoadedError)")

        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}')
        return RunRequest(
            run_config={
                'ops': {'get_practice_data_api': {"config": {'practice_num': int(next_session['session_name'][-1]),
                                                             'round_number': int(next_event_df['ROUND_NUMBER']),
                                                             'year': int(next_event_df['EVENT_YEAR'])
                                                             }}}}
        )
    else:
        return SkipReason(f"It is not 30 mins after the {next_session['session_name']}, next session is on "
                          f"{session_time.date()} at {session_time.time()}")


@sensor(job=quali_data_load_job,
        minimum_interval_seconds=300,
        required_resource_keys={'fastf1', 'mysql'})
def qualifying_data_load_sensor(context: SensorEvaluationContext):
    today = datetime.utcnow()

    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["SESSION_FOUR_TYPE"]}')

    if next_event_df.empty:
        return SkipReason('No next event data available')

    if next_event_df['SESSION_TWO_DT'].date() > today.date():
        return SkipReason('Session 2 is before today its on {}'.format(next_event_df['SESSION_TWO_DT']))

    if next_event_df['SESSION_FOUR_DT'].date() < today.date():
        return SkipReason('Session 4 is after today its on {}'.format(next_event_df['SESSION_FOUR_DT']))

    if next_event_df['EVENT_TYPE_CD'] == 1:
        sessions = ['SESSION_FOUR']
    elif next_event_df['EVENT_TYPE_CD'] == 2:
        sessions = ['SESSION_TWO', 'SESSION_FOUR']
    else:
        raise Exception('Unexpected EVENT_TYPE_CD {} in Event {} - {}'.format(next_event_df['EVENT_TYPE_CD'],
                                                                              next_event_df['EVENT_CD'],
                                                                              next_event_df['EVENT_NAME']))

    session_df = pd.DataFrame()
    for session in sessions:
        session_df = pd.concat([session_df,
                                pd.DataFrame({'session_num': [session],
                                              'session_time': [next_event_df[f'{session}_DT']],
                                              'session_name': [next_event_df[f'{session}_TYPE']]})],
                               ignore_index=True)

    next_session = session_df[session_df['session_time']
                              >
                              today - timedelta(hours=5)]

    if len(next_session) == 0:
        return SkipReason(f"{next_event_df['EVENT_NAME']} has no more quali sessions to load")
    else:
        next_session = next_session.iloc[0]

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}':
        return SkipReason(f"{next_event_df['ROUND_NUMBER']} - {next_session['session_name']} has already been loaded!")

    session_time = next_session['session_time']
    session_time_modified = (session_time + timedelta(hours=1.5))

    if session_time_modified < today:
        try:
            session_name = next_session['session_name']
            if 'Sprint' in session_name:
                sprint = True
            else:
                sprint = False

            api_data = context.resources.fastf1.get_qualifying_results(year=next_event_df['EVENT_YEAR'],
                                                                       round_number=next_event_df['ROUND_NUMBER'],
                                                                       sprint=sprint).copy()

            drivers = pd.unique(api_data['Abbreviation'])
            if len(drivers) <= 1:
                return SkipReason("Session data is not available as there is no drivers in the data")
            if len(api_data[~api_data['Q1'].isnull()]) == 0:
                return SkipReason("Session data is not available as there is no lap times in the data")
        except KeyError:
            return SkipReason("Session data is not available (KeyError)")
        except DataNotLoadedError:
            return SkipReason("Session data is not available (DataNotLoadedError)")

        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}')
        return RunRequest(
            run_config={'ops': {'get_quali_data_api': {"config": {'round_number': int(next_event_df['ROUND_NUMBER']),
                                                                  'year': int(next_event_df['EVENT_YEAR']),
                                                                  'sprint': sprint
                                                                  }}}}
        )
    else:
        return SkipReason(f"It is not 30 mins after the {next_session['session_name']}, next session is on "
                          f"{session_time.date()} at {session_time.time()}")


@sensor(job=race_data_load_job,
        minimum_interval_seconds=300,
        required_resource_keys={'fastf1', 'mysql'})
def race_data_load_sensor(context: SensorEvaluationContext):
    today = datetime.utcnow()

    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["SESSION_FIVE_TYPE"]}')

    if next_event_df.empty:
        return SkipReason('No next event data available')

    if next_event_df['SESSION_THREE_DT'].date() > today.date():
        return SkipReason('Session 3 is before today its on {}'.format(next_event_df['SESSION_THREE_DT']))

    if next_event_df['SESSION_FIVE_DT'].date() < today.date():
        return SkipReason('Session 5 is after today its on {}'.format(next_event_df['SESSION_FIVE_TYPE']))

    if next_event_df['EVENT_TYPE_CD'] == 1:
        sessions = ['SESSION_FIVE']
    elif next_event_df['EVENT_TYPE_CD'] == 2:
        sessions = ['SESSION_THREE', 'SESSION_FIVE']
    else:
        raise Exception('Unexpected EVENT_TYPE_CD {} in Event {} - {}'.format(next_event_df['EVENT_TYPE_CD'],
                                                                              next_event_df['EVENT_CD'],
                                                                              next_event_df['EVENT_NAME']))

    session_df = pd.DataFrame()
    for session in sessions:
        session_df = pd.concat([session_df,
                                pd.DataFrame({'session_num': [session],
                                              'session_time': [next_event_df[f'{session}_DT']],
                                              'session_name': [next_event_df[f'{session}_TYPE']]})],
                               ignore_index=True)

    next_session = session_df[session_df['session_time']
                              >
                              today - timedelta(hours=8)]

    if next_session.empty:
        return SkipReason(f"{next_event_df['EVENT_NAME']} has no more race sessions to load")
    else:
        next_session = next_session.iloc[0]

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}':
        return SkipReason(f"{next_event_df['ROUND_NUMBER']} - {next_session['session_name']} has already been loaded!")

    session_time = next_session['session_time']
    session_time_modified = (session_time + timedelta(hours=1.5))

    if session_time_modified < today:
        try:
            session_name = next_session['session_name']
            if 'Sprint' in session_name:
                sprint = True
            else:
                sprint = False

            api_data = context.resources.fastf1.get_race_results(year=int(next_event_df['EVENT_YEAR']),
                                                                 round_number=int(next_event_df['ROUND_NUMBER']),
                                                                 sprint=sprint).copy()

            drivers = pd.unique(api_data['DriverId'])
            if len(drivers) <= 1:
                return SkipReason("Session data is not available as there is no drivers in the data")
        except KeyError:
            return SkipReason("Session data is not available (KeyError)")
        except DataNotLoadedError:
            return SkipReason("Session data is not available (DataNotLoadedError)")

        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}')
        return RunRequest(
            run_config={'ops': {'get_race_data_api': {"config": {'round_number': int(next_event_df['ROUND_NUMBER']),
                                                                 'year': int(next_event_df['EVENT_YEAR']),
                                                                 'sprint': sprint
                                                                 }}}}
        )
    else:
        return SkipReason(f"It is not 30 mins after the {next_session['session_name']}, next session is on "
                          f"{session_time.date()} at {session_time.time()}")


@sensor(job=race_laps_data_load_job,
        minimum_interval_seconds=300,
        required_resource_keys={'fastf1', 'mysql'})
def race_laps_data_load_sensor(context: SensorEvaluationContext):
    today = datetime.utcnow()

    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["SESSION_FIVE_TYPE"]}')

    if next_event_df.empty:
        return SkipReason('No next event data available')

    if next_event_df['SESSION_THREE_DT'].date() > today.date():
        return SkipReason('Session 3 is before today its on {}'.format(next_event_df['SESSION_THREE_DT']))

    if next_event_df['SESSION_FIVE_DT'].date() < today.date():
        return SkipReason('Session 5 is after today its on {}'.format(next_event_df['SESSION_FIVE_TYPE']))

    if next_event_df['EVENT_TYPE_CD'] == 1:
        sessions = ['SESSION_FIVE']
    elif next_event_df['EVENT_TYPE_CD'] == 2:
        sessions = ['SESSION_THREE', 'SESSION_FIVE']
    else:
        raise Exception('Unexpected EVENT_TYPE_CD {} in Event {} - {}'.format(next_event_df['EVENT_TYPE_CD'],
                                                                              next_event_df['EVENT_CD'],
                                                                              next_event_df['EVENT_NAME']))

    session_df = pd.DataFrame()
    for session in sessions:
        session_df = pd.concat([session_df,
                                pd.DataFrame({'session_num': [session],
                                              'session_time': [next_event_df[f'{session}_DT']],
                                              'session_name': [next_event_df[f'{session}_TYPE']]})],
                               ignore_index=True)

    next_session = session_df[session_df['session_time']
                              >
                              today - timedelta(hours=8)]

    if next_session.empty:
        return SkipReason(f"{next_event_df['EVENT_NAME']} has no more race sessions to load")
    else:
        next_session = next_session.iloc[0]

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}':
        return SkipReason(f"{next_event_df['EVENT_NAME']} - {next_session['session_name']} has already been loaded!")

    session_time = next_session['session_time']
    session_time_modified = (session_time + timedelta(hours=1.5))

    if session_time_modified < today:
        try:
            session_name = next_session['session_name']
            if 'Sprint' in session_name:
                sprint = True
            else:
                sprint = False

            api_data = context.resources.fastf1.get_race_results(year=int(next_event_df['EVENT_YEAR']),
                                                                 round_number=int(next_event_df['ROUND_NUMBER']),
                                                                 sprint=sprint,
                                                                 laps=True).copy()

            drivers = pd.unique(api_data['Driver'])
            if len(drivers) == 0:
                return SkipReason("Session data is not available")
        except KeyError:
            return SkipReason("Session data is not available (KeyError)")
        except DataNotLoadedError:
            return SkipReason("Session data is not available (DataNotLoadedError)")

        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_session["session_name"]}')
        return RunRequest(
            run_config={'ops': {'get_race_lap_data_api': {"config": {'round_number': int(next_event_df['ROUND_NUMBER']),
                                                                     'year': int(next_event_df['EVENT_YEAR']),
                                                                     'sprint': sprint
                                                                     }}}}
        )
    else:
        return SkipReason(f"It is not 30 mins after the {next_session['session_name']}, next session is on "
                          f"{session_time.date()} at {session_time.time()}")
