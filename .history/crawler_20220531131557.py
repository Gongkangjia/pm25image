import arrow
import requests
import pandas as pd
import numpy as np
from loguru import logger
from config import STATIONS_CNEMC


class Base:
    def __init__(self, datetime=None):
        if datetime is None:
            self.datetime = arrow.now()
        else:
            self.datetime = arrow.get(datetime)

    def run(self):
        raise NotImplemented


class Cnemc(Base):
    def __init__(self, datetime=None):
        super().__init__(datetime)
        self.base = "https://air.cnemc.cn:18007/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService" \
                    ".svc/binary/"

    def _wcf2json(self, wcf):
        from wcf.records import print_records, Record
        import io
        records = Record.parse(io.BytesIO(wcf))

        with io.StringIO() as f:
            print_records(records, fp=f)
            f.seek(0)
            xml_str = f.getvalue()
            xml_str = xml_str.replace("&mdash;", "")

        import xmltodict
        xd = xmltodict.parse(xml_str, process_namespaces=True, postprocessor=self._xml_postprocess)
        return xd

    @staticmethod
    def _xml_postprocess(path, key, value):
        key = key.rsplit(":", 1)[-1]
        if key in ("OpenAccessGenerated", "IsPublish", "Unheathful", "Measure"):
            return None
        else:
            return key, value

    def _get_action(self, action):
        res_action = requests.get(self.base + action, headers={'Content-Type': 'application/msbin1'})
        return self._wcf2json(res_action.content)

    @staticmethod
    def _to_int(x):
        try:
            if int(x)>0:
                return int(x)
            else:
                
        except (ValueError, TypeError) as e:
            return np.nan

    @staticmethod
    def _arrow2timestamp(arrow_time):
        return int(arrow_time.replace(tzinfo="UTC").timestamp() * 10000000 + 621355968000000000)

    def get_provinces(self):
        res = self._get_action("GetProvinces")
        data = res["GetProvincesResponse"]["GetProvincesResult"]["RootResults"]["Province"]
        return pd.DataFrame(data)

    def get_cities(self):
        res = self._get_action("GetCities")
        data = res["GetCitiesResponse"]["GetCitiesResult"]["RootResults"]["City"]
        return pd.DataFrame(data)

    def get_stations(self):
        res = self._get_action("GetStationConfigs")
        data = res["GetStationConfigsResponse"]["GetStationConfigsResult"]["RootResults"]["StationConfig"]
        return pd.DataFrame(data)

    def get_city_df(self, city_code, start, end):
        start = self._arrow2timestamp(start)
        end = self._arrow2timestamp(end)
        action = (
            f"GetCityAQIPublishHistories?$where=(it.CityCode%253d%253d{city_code})"
            f"&$where=((it.TimePoint%253e%253dDateTime({start}%252c%2522Local%2522))"
            f"%2526%2526(it.TimePoint%253cDateTime({end}%252c%2522Local%2522)))&$orderby=it.TimePoint"
        )
        res = self._get_action(action)
        try:
            data = res["GetCityAQIPublishHistoriesResponse"]["GetCityAQIPublishHistoriesResult"]["RootResults"][
                "CityAQIPublishHistory"]
            return pd.DataFrame(data)
        except TypeError as e:
            return pd.DataFrame()

    def get_station_df(self, station_code, start, end):
        start = self._arrow2timestamp(start)
        end = self._arrow2timestamp(end)
        action = (
            f"GetAQIDataPublishHistories?$where=(it.StationCode%253d%253d%2522{station_code}%2522)"
            f"&$where=((it.TimePoint%253e%253dDateTime({start}%252c%2522Local%2522))"
            f"%2526%2526(it.TimePoint%253cDateTime({end}%252c%2522Local%2522)))&$orderby=it.TimePoint"
        )
        res = self._get_action(action)
        try:
            data = res["GetAQIDataPublishHistoriesResponse"]["GetAQIDataPublishHistoriesResult"]["RootResults"][
                "AQIDataPublishHistory"]
            return pd.DataFrame(data)
        except Exception as e:
            return pd.DataFrame()

    def run(self, start=None, end=None):
        if start:
            arrow_start = arrow.get(start)
        else:
            arrow_start = arrow.now().shift(hours=-1).floor("days").shift(hours=1)

        if end:
            arrow_end = arrow.get(end)
        else:
            arrow_end = arrow.now()

        logger.info("start=>{}", arrow_start)
        logger.info("end=>{}", arrow_end)

        data_list = []
        dt_full = pd.Series(pd.date_range(start=arrow_start.format("YYYY-MM-DD HH:mm:ss"),
                                          end=arrow_end.format("YYYY-MM-DD HH:mm:ss"), freq="H"),
                            name="DATETIME").to_frame()
        # dt_full = pd.DataFrame()
        for station_name, station_id in STATIONS_CNEMC.items():

            df = self.get_station_df(station_code=station_id, start=arrow_start, end=arrow_end)
            df["O3"] = df["O3_24h"]
            df = df.loc[:, ["TimePoint", "PM2_5", "PM10", "NO2", "O3", ]]
            df = df.rename({"TimePoint": "DATETIME"}, axis=1)
            df["DATETIME"] = pd.to_datetime(df["DATETIME"])
            df = dt_full.merge(df, left_on="DATETIME", right_on="DATETIME", how="left")
            df = df.set_index("DATETIME").applymap(self._to_int)
            df["O3_8H"] = df["O3"].rolling(8, 6).mean()
            df.loc[df.index.hour < 8, "O3_8H"] = np.nan
            df["NAME"] = station_name
            data_list.append(df)

        for name, city_id in {"南京": 320100, "无锡": 320200, "苏州": 320500}.items():
            df = self.get_city_df(city_code=city_id, start=arrow_start, end=arrow_end)
            df = df.loc[:, ["TimePoint", "PM2_5", "PM10", "NO2", "O3", ]]
            df = df.rename({"TimePoint": "DATETIME"}, axis=1)
            df["DATETIME"] = pd.to_datetime(df["DATETIME"])
            df = dt_full.merge(df, left_on="DATETIME", right_on="DATETIME", how="left")
            df = df.set_index("DATETIME").applymap(self._to_int)

            df["O3_8H"] = df["O3"].rolling(8, 6).mean()
            df.loc[df.index.hour < 8, "O3_8H"] = np.nan
            df["NAME"] = name
            data_list.append(df)

        df = pd.concat(data_list,)
        df = df.reset_index()
        return df

        # day = df.groupby("NAME").mean()
        # day["MDA8"] = df.groupby("NAME").max()["O3_8H"]
        # print(day.round(0).applymap(self._to_int))


if __name__ == '__main__':
    c = Cnemc()
    # a = c.get_station_df(station_code="3422A", )
    # a = c.get_city_df(city_code="320100", start=arrow.get("2022-04-23 00:00:00"))

    # b = a.loc[:,["PM2_5","PM10","SO2"]].applymap(float)
    # print(a[["PM10","TimePoint"]])
    # print(a)
    # print(a)
    # print(a[])
    df1 = c.run()
    print(df1)
    df1.to_csv("test.csv")
