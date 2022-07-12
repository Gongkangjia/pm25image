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
                return np.nan
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
        df.loc[df["PM2_5"] > df["PM10"], "PM10"] = np.nan
        return df

        # day = df.groupby("NAME").mean()
        # day["MDA8"] = df.groupby("NAME").max()["O3_8H"]
        # print(day.round(0).applymap(self._to_int))


class Moji():
    API_HOST = "http://epapi.moji.com"
    SPECIES_ID = {
        "AQI": 1,
        "PM2_5": 2,
        "PM10": 3,
        "SO2": 4,
        "NO2": 5,
        "O3": 6,
        "CO": 7,
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'Host': 'epapi.moji.com',
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'User-Agent': 'AirMonitoring/4.3.8 (iPhone; iOS 15.5; Scale/3.00)',
            'Accept-Language': 'zh-Hans-CN;q=1, en-CN;q=0.9',
        }
        self.uid = 0
        self.register()

    @property
    def request_common(self):
        common = {
            'cityid': '320100',
            'app_version': '4504030802',
            'device': 'iPhone11,6',
            'apnsisopen': '1',
            'platform': 'iPhone',
            'uid': self.uid,
            'language': 'CN',
            'identifier': '9546AD25-48E7-48E8-8E5A-221B6751C16F',
        }
        return common

    def register(self):
        params = {'uuid': '5CF37C3A0ECB4135B8E41645B55BD0B1'}
        req = {"common": self.request_common, "params": params}
        res = self.session.post(
            self.API_HOST + "/json/device/register", json=req).json()
        logger.info("register res=>{}", res)
        self.uid = res["data"]["userId"]

    def get_city_list(self):
        params = {'type': '5', 'sort': 1}
        req = {"common": self.request_common, "params": params}
        res = self.session.post(
            self.API_HOST + "/json/citylist/cityList", json=req).json()
        logger.info("get_city_list res=>{}", res)
        return res["cityAqis"]

    def get_station_list_by_city_id(self, city_id):
        params = {'cityId': city_id}
        req = {"common": self.request_common, "params": params}
        res = self.session.post(
            self.API_HOST + "/json/epa/cityStationList", json=req).json()
        logger.info("get_station_list_by_city_id res=>{}", res)
        return res["list"]

    def get_value_by_station_and_species(self, species_id, city_id, station_id):
        params = {
            'aqiType': species_id,
            'cityId': city_id,
            'stationId': station_id,
        }
        req = {"common": self.request_common, "params": params}
        res = self.session.post(
            self.API_HOST + "/json/epa/newTrend", json=req).json()
        # logger.info("get_station_list_by_city_id res=>{}", res)
        return res["trendList"]["list"]

    def get_value_by_city_and_species(self, species_id, city_id):
        params = {
            'aqiType': species_id,
            'cityId': city_id,
            "timeRange": 72

        }
        req = {"common": self.request_common, "params": params}
        res = self.session.post(
            self.API_HOST + "/json/epa/trend", json=req).json()
        logger.info("get_value_by_city_and_species res=>{}", res)
        return res["trendList"]["list"]

    @staticmethod
    def _to_int(x):
        try:
            if int(x)>0:
                return int(x)
            else:
                return np.nan
        except (ValueError, TypeError) as e:
            return np.nan

    def run(self, start=None, end=None):
        if start:
            arrow_start = arrow.get(start)
        else:
            arrow_start = arrow.now().shift(hours=-1).floor("days").shift(hours=1)

        if end:
            arrow_end = arrow.get(end)
        else:
            arrow_end = arrow.now()

        dt_full = pd.Series(pd.date_range(start=arrow_start.format("YYYY-MM-DD HH:mm:ss"),
                                          end=arrow_end.format("YYYY-MM-DD HH:mm:ss"), freq="H"),
                            name="DATETIME").to_frame()

        city_id = 320100
        stations = [
                    {'stationName': '玄武湖', 'stationId': 362},
                    {'stationName': '瑞金路', 'stationId': 363},
                    {'stationName': '中华门', 'stationId': 364},
                    {'stationName': '草场门', 'stationId': 365},
                    {'stationName': '山西路', 'stationId': 366},
                    {'stationName': '仙林大学城', 'stationId': 367},
                    {'stationName': '奥体中心', 'stationId': 368},
                    {'stationName': '浦口', 'stationId': 369},
                    {'stationName': '迈皋桥', 'stationId': 370},
                    {'stationName': '彩虹桥', 'stationId': 14318},
                    {'stationName': '雄州', 'stationId': 14319},
                    {'stationName': '永阳', 'stationId': 14320},
                    {'stationName': '老职中', 'stationId': 14321}
                    
                    ]
        res = []
        for station in stations:
            station_species_list = []
            for species in ("PM2_5", "PM10", "NO2", "O3"):
                species_id = self.SPECIES_ID[species]
                species_res = self.get_value_by_station_and_species(
                    species_id=species_id, 
                    city_id=city_id,
                     station_id=station["stationId"]
                     )
                df =  pd.DataFrame(species_res)
                df["DATETIME"] = pd.to_datetime(df.time.astype(int)+8*3600*1000,unit="ms")
                df["SPECIES"] = species
                station_species_list.append(df)
            station_df = pd.concat(station_species_list)

            station_df = station_df.pivot(index="DATETIME",columns="SPECIES",values="value")
            station_df = station_df.applymap(self._to_int)

            #选择当天数据
            station_df = dt_full.merge(station_df, left_on="DATETIME", right_index=True, how="left")
            station_df = station_df.set_index("DATETIME")
            station_df["O3_8H"] = station_df["O3"].rolling(8, 6).mean()
            station_df.loc[station_df.index.hour < 8, "O3_8H"] = np.nan
            station_df["NAME"] = station["stationName"]
            res.append(station_df)
            # return
                # for item in species_res:
                #     print(item)
                #     item["NAME"]=station["stationName"]
                #     item["SPECIES"] = species
                #     res.append(item)
        for name, city_id  in {"南京": 320100, "无锡": 320200, "苏州": 320500}.items():
            station_species_list = []
            for species in ("PM2_5", "PM10", "NO2", "O3"):
                species_id = self.SPECIES_ID[species]
                species_res = self.get_value_by_city_and_species(
                    species_id=species_id, 
                    city_id=city_id
                    )
                df =  pd.DataFrame(species_res)
                df["DATETIME"] = pd.to_datetime(df.time.astype(int)+8*3600*1000,unit="ms")
                df["SPECIES"] = species
                station_species_list.append(df)
            station_df = pd.concat(station_species_list)

            station_df = station_df.pivot(index="DATETIME",columns="SPECIES",values="value")
            station_df = station_df.applymap(self._to_int)
            # print(station_df)
            #选择当天数据
            station_df = dt_full.merge(station_df, left_on="DATETIME", right_index=True, how="left")
            station_df = station_df.set_index("DATETIME")
            station_df["O3_8H"] = station_df["O3"].rolling(8, 6).mean()
            station_df.loc[station_df.index.hour < 8, "O3_8H"] = np.nan
            station_df["NAME"] = name
            res.append(station_df)
        # print(res[0])
        df = pd.concat(res)
        df = df.reset_index()
        df.loc[df["PM2_5"] > df["PM10"], "PM10"] = np.nan
        return df


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
