import pandas as pd
from dagster import ConfigurableResource
import fastf1 as ff1
from typing import Literal
from fastf1.core import Session, Laps
from fastf1.exceptions import DataNotLoadedError

class FastF1Client:

    def __init__(self, cache_loc: str):
        self.cache_loc = cache_loc
        self.sess = None
        ff1.Cache.enable_cache(self.cache_loc)

    def _load_session(self,
                      year: int,
                      gp: int,
                      identifier: str,
                      laps: bool = False):

        self.sess = ff1.get_session(year=year,
                                    gp=gp,
                                    identifier=identifier)
        self.sess.load(laps=laps)

    @staticmethod
    def _session_list(year: int, round_number: int):
        try:
            event = ff1.get_event(year, round_number)[
                ['Session1', 'Session2', 'Session3', 'Session4', 'Session5']].to_frame()
            return [x for c in event.columns for x in event[c]]
        except ValueError as err:
            if 'Failed to load any schedule data.' in err.args[0]:
                return ['Practice 1', 'Practice 2', 'Practice 3', 'Qualifying', 'Race']
            else:
                raise ValueError(err)

    def _fastest_laps(self):
        fastest_laps = list()
        for driver in self.sess.drivers:
            try:
                lap = self.sess.laps.pick_drivers(driver).pick_fastest()
                lap['Driver']
                fastest_laps.append(lap)
            except TypeError:
                pass
            except DataNotLoadedError:
                return pd.DataFrame(columns=['Driver', 'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time'])

        return Laps(fastest_laps).sort_values(by='LapTime').reset_index(drop=True)

    def _practice_results(self,
                          year: int,
                          round_number: int,
                          practice_num: Literal[1, 2, 3],
                          drivers: bool):

        identifier = 'FP{}'.format(practice_num)

        self._load_session(year=year,
                           gp=round_number,
                           identifier=identifier,
                           laps=True)

        if drivers:
            driver_df = self.sess.results[['DriverId', 'TeamId', 'Abbreviation']]
            df = self._fastest_laps()[['Driver', 'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time']]
            df = driver_df.set_index('Abbreviation').join(df.set_index('Driver'))
        else:
            df = self._fastest_laps()[['Driver', 'Team', 'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time']]
        return df

    def _get_race_laps(self):
        laps = pd.DataFrame()
        for driver in self.sess.drivers:
            laps = pd.concat([laps, self.sess.laps.pick_drivers(driver)])
        return laps

    def get_practice_results(self,
                             year: int,
                             round_number: int,
                             practice_num: Literal[1, 2, 3, None] = None,
                             drivers: bool = True):

        if practice_num is None:
            sessions = [x[-1] for x in self._session_list(year, round_number) if 'Practice' in x]
            df = pd.DataFrame()
            for session in sessions:
                api_data = self._practice_results(year, round_number, session, drivers).copy()
                api_data.loc[:, 'SESSION_CD'] = session
                df = pd.concat([df, api_data])
            return df
        else:
            api_data = self._practice_results(year, round_number, practice_num, drivers).copy()
            api_data.loc[:, 'SESSION_CD'] = practice_num
            return api_data

    def get_qualifying_results(self,
                               year: int,
                               round_number: int,
                               sprint: bool = False):

        if sprint and year == 2023:
            identifier = 'Sprint Shootout'
        elif sprint and year > 2023:
            identifier = 'Sprint Qualifying'
        elif not sprint:
            identifier = 'Qualifying'
        else:
            raise Exception('Sprint Qualifying is not supported before 2023')

        self._load_session(year=year,
                           gp=round_number,
                           identifier=identifier,
                           laps=True)

        df = self.sess.results

        return df[['Abbreviation', 'TeamName', 'Position', 'Q1', 'Q2', 'Q3']]

    def get_race_results(self,
                         year: int,
                         round_number: int,
                         sprint: bool = False,
                         laps: bool = False):

        if sprint and year >= 2021:
            identifier = 'Sprint'
        elif not sprint:
            identifier = 'Race'
        else:
            raise Exception('Sprint Race is not supported before 2021')

        if laps:
            self._load_session(year=year,
                               gp=round_number,
                               identifier=identifier,
                               laps=laps)
            return self._get_race_laps()
        else:
            self._load_session(year=year,
                               gp=round_number,
                               identifier=identifier)
            df = self.sess.results
            return df[['DriverId', 'TeamId', 'ClassifiedPosition', 'Position', 'Time', 'Status', 'Points']]


class FastF1Resource(ConfigurableResource):
    cache_loc: str

    def get_client(self) -> FastF1Client:
        return FastF1Client(cache_loc=self.cache_loc)

    def get_practice_results(self,
                             year: int,
                             round_number: int,
                             practice_num: Literal[1, 2, 3, None] = None,
                             drivers: bool = True) -> pd.DataFrame:
        client = self.get_client()
        return client.get_practice_results(year, round_number, practice_num, drivers)

    def get_qualifying_results(self,
                               year: int,
                               round_number: int,
                               sprint: bool = False) -> pd.DataFrame:
        client = self.get_client()
        return client.get_qualifying_results(year, round_number, sprint)

    def get_race_results(self,
                         year: int,
                         round_number: int,
                         sprint: bool = False,
                         laps: bool = False) -> pd.DataFrame:
        client = self.get_client()
        return client.get_race_results(year, round_number, sprint, laps)
