import arrow
import requests
import json
import ast
import os
import io
import xmltodict
from wcf.records import print_records, Record
from loguru import logger


class CNEMC:
    def __init__(self):
        self.base = "https://air.cnemc.cn:18007" \
                    "/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/"

    def _wcf2json(self, wcf):

        records = Record.parse(io.BytesIO(wcf))

        with io.StringIO() as f:
            print_records(records, fp=f)
            f.seek(0)
            xml_str = f.getvalue()
            xml_str = xml_str.replace("&mdash;", "")

        with open("test.xml", "w") as f:
            f.write(xml_str)
        xd = xmltodict.parse(xml_str, process_namespaces=True, postprocessor=self._xml_postprocess)
        return xd

    @staticmethod
    def _xml_postprocess(path, key, value):
        # logger.info(path)
        key = key.rsplit(":", 1)[-1]
        if key in ("OpenAccessGenerated", "IsPublish", "Unheathful"):
            return None
        else:
            return key, value

    def get_action(self, action):
        logger.info("正在调用get_action=>{}", action)
        action_res = requests.get(self.base + action, headers={'Content-Type': 'application/msbin1'})
        return self._wcf2json(action_res.content)

    def get_city_live(self, city_code="310000"):
        action = f"GetCityAQIPublishLives?$where=(it.CityCode%253d%253d{city_code})"
        return self.get_action(action)

    def get_station_live(self, station_code="3422A"):
        action = f"GetAQIDataPublishLives?$where=(it.StationCode%253d%253d%2522{station_code}%2522)"
        return self.get_action(action)

    def get_city_history(self, city_code=150900, start=None, end=None):

        start = int(arrow.get(start).timestamp() * 10000000 + 621355968000000000)
        end = int(arrow.get(end).timestamp() * 10000000 + 621355968000000000)

        action = f"GetCityAQIPublishHistories?$where=(it.CityCode%253d%253d{city_code})\
                  &$where=((it.TimePoint%253e%253dDateTime({start}%252c%2522Local%2522)) \
                  %2526%2526(it.TimePoint%253cDateTime({end}%252c%2522Local%2522)))\
                  &$orderby=it.TimePoint"
        return self.get_action(action)

    def get_station_history(self, station_code):
        action = f"GetAQIDataPublishHistories?$where=(it.StationCode%253d%253d%2522{station_code}%2522)&$orderby=it.TimePoint"
        return self.get_action(action)


if __name__ == '__main__':
    service = CNEMC()
    res = service.get_city_live(city_code="320200")
    with open("city_live.json", "w") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    print(dict(res))

    start = arrow.now().shift(hours=-1).floor("day").shift(hours=1).format("YYYYMMDD HH:mm:SS")
    end = arrow.now().floor("day").shift(days=2).format("YYYY-MM-DD HH:mm:SS")
    print(start,end)
    res = service.get_city_history(city_code="320200", start=start, end=end)
    with open("city_live.json", "w") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    # print(dict(res))

    # res = service.get_station_live()
    d = res["GetCityAQIPublishHistoriesResponse"]["GetCityAQIPublishHistoriesResult"]["RootResults"][
        "CityAQIPublishHistory"]
    import pandas as pd

    df = pd.DataFrame(d)
    df = df.loc[:, ["TimePoint", "PM2_5", "PM10", "NO2", "O3", "CO", "SO2", "AQI"]]
    df = df.set_index("TimePoint")
    print(df)

    df.index = pd.to_datetime(df.index,format="%Y-%m-%dT%H:%M:%S")
