import time
import numpy as np
import openpyxl
import arrow
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import json
from lxml import etree
from pathlib import Path
from loguru import logger
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import pytz

from .vc import VC
from .config import STATIONS
from .cnemc import CNEMC

from matplotlib import font_manager
import os

font_manager.fontManager.addfont(str(Path(__file__).parent.parent.absolute().joinpath("static/kjgong.ttf")))

config = {
    "font.family": "kjgong",
    "font.size": 24,
    "mathtext.fontset": "stix",
    "xtick.direction": "in",
    "ytick.direction": "in",
    "axes.linewidth": 2,
    "xtick.major.size": 8,
    "xtick.major.width": 2,
    "xtick.major.pad": 12,
    "xtick.minor.size": 5,
    "xtick.minor.width": 2,
    "ytick.major.size": 8,
    "ytick.major.width": 2,
    "ytick.minor.size": 5,
    "ytick.minor.width": 2,
    "ytick.minor.width": 2
}
plt.rcParams.update(config)


class Crawler:
    def __init__(self) -> None:
        self.rt = Path(__file__).parent.parent.absolute() / "rt"
        self.cookies_path = self.rt / Path(".cookies.json")
        self.validate_code_path = self.rt / Path("vc.png")
        self.rt_html_path = self.rt / Path("rt_data.html")
        self.datetime_tag_path = self.rt / Path("datetime.tag")
        self.rt_data_path = self.rt / Path("rt_data.csv")
        self.daily_data_path = self.rt / Path("daily_data.csv")
        self.all_data_path = self.rt / Path("all_data.csv")
        self.wuxi_data_path = self.rt / Path("wuxi_data.csv")
        self.suzhou_data_path = self.rt / Path("suzhou_data.csv")
        self.init_session()
        self.load_session()

    def init_session(self):
        self.session = requests.Session()
        self.session.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Referer': 'http://112.25.188.53:12080/njeqs/mainFrame.aspx',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        }
        self.session.keep_alive = False
        self.session.mount('http://', HTTPAdapter(max_retries=3))
        self.session.mount('https://', HTTPAdapter(max_retries=3))

    def get_login_params(self):
        url = 'http://112.25.188.53:12080/njeqs/Default.aspx'
        logger.info("????????????index??????")
        response = self.session.get(url, verify=False)
        html = etree.HTML(response.text)

        __VIEWSTATE = html.xpath("*//input[@id='__VIEWSTATE']/@value")
        __VIEWSTATEGENERATOR = html.xpath(
            "*//input[@id='__VIEWSTATEGENERATOR']/@value")
        __EVENTVALIDATION = html.xpath(
            "*//input[@id='__EVENTVALIDATION']/@value")

        if __VIEWSTATE and __VIEWSTATEGENERATOR and __EVENTVALIDATION:
            params = {
                "__VIEWSTATE": __VIEWSTATE[0],
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR[0],
                "__EVENTVALIDATION": __EVENTVALIDATION[0]
            }
            logger.info("??????????????????=>{}", params)
        else:
            logger.error("????????????????????????=>{}", response.text)

        data = {
            'userId': 'Daqs',
            'pwd': 'Daqs@123',
            'tbValidateCode': self.get_validate_code_word(),
            'loginBtn.x': '48',
            'loginBtn.y': '17',
            **params
        }
        return data

    def get_validate_code_word(self):
        # ???????????????
        logger.info("???????????????????????????")
        url = "http://112.25.188.53:12080/njeqs/ValidateCodeCreate.aspx"
        img = self.session.get(url)
        if img.status_code == 200:
            logger.info("???????????????????????????,????????????=>{}", self.validate_code_path)
        self.validate_code_path.write_bytes(img.content)
        logger.info("????????????=>{}", self.validate_code_path)
        vc = VC()
        words = vc.get_words()
        return words

    def dump_session(self):
        logger.info("dumping session=>{}", self.cookies_path)
        with open(self.cookies_path, "w") as f:
            cookies_dict = requests.utils.dict_from_cookiejar(
                self.session.cookies)
            logger.info("?????????cookies???=>{}", cookies_dict)
            json.dump(cookies_dict, f)
        logger.info("??????????????????=>{}", self.cookies_path)

    def load_session(self):
        if self.cookies_path.is_file():
            logger.info("??????cookies??????,????????????")
            with open(self.cookies_path, "r") as f:
                try:
                    cookies_dict = json.load(f)
                    cookies = requests.utils.cookiejar_from_dict(cookies_dict)
                    self.session.cookies = cookies
                    logger.info("??????????????????")
                except Exception as e:
                    logger.error("??????????????????=>{}", e)
                    logger.info("????????????...")
                    self.login()
        else:
            logger.info("??????cookies,????????????...")
            self.login()

    def login(self):
        for t in range(10):
            self.init_session()
            logger.info("???{}?????????..", t + 1)
            data = self.get_login_params()
            logger.info("???????????????=>{}", data)
            logger.info("????????????")
            login_url = 'http://112.25.188.53:12080/njeqs/Default.aspx'
            login_res = self.session.post(login_url, data=data)

            if login_res.status_code == 200 and "????????????????????????" in login_res.text:
                logger.info("????????????")
                self.dump_session()
                break
            else:
                html = etree.HTML(login_res.text)
                error_msg = html.xpath("*//span[@id='hintTD']/text()")
                logger.error("????????????=>{}", login_res.text)
                logger.error("???????????????=>{}", error_msg)
                logger.error("??????????????????...")
                time.sleep(10)
        else:
            logger.info("??????????????????,???????????????...")
            from push import EmailPush
            push = EmailPush()
            push.mail("???????????????????????????????????????", content="??????????????????,???????????????...")

    def get_rt_data(self):
        url = 'http://112.25.188.53:12080/njeqs/DataQuery/AirStationLastHourDataStat.aspx'
        params = {
            'strStationIds': 'Station.id in(2001,2002,2003,2004,2006,2007,2008,2009,2010,2016,2022,2024,2036)',
            'ftid': '578'
        }
        logger.info("??????????????????,??????url???=>{}", url)
        response = self.session.post(url, params=params, timeout=60)
        logger.info("?????????????????????=>{}", len(response.text))
        # ??????????????????????????????
        if "loginBtn" in response.text:
            logger.error("cookies??????,????????????...")
            self.login()
            return self.get_rt_data()
        else:
            logger.info("????????????????????????")
            self.rt_html_path.write_text(response.text)
            logger.info("??????????????????????????????=>{}", self.rt_html_path)

    def get_datetime(self):
        html = etree.HTML(self.rt_html_path.read_text())
        datetime = html.xpath("*//input[@id='strStartTime']/@value")
        datetime = arrow.get(datetime[0]).shift(hours=1)
        return datetime

    def is_update(self):
        self.get_rt_data()
        new = self.get_datetime().format("YYYY-MM-DDTHH")

        if self.datetime_tag_path.is_file():
            last = self.datetime_tag_path.read_text()
        else:
            last = None

        return new != last

    @staticmethod
    def get_df_from_html(html):
        # logger.info("html=>{}",html)
        if isinstance(html, Path):
            html = etree.parse(str(html), etree.HTMLParser(encoding="utf-8"))
        else:
            html = etree.HTML(html)
        table = html.xpath("//table[@id='containerTB']")
        if table:
            table = table[0]
        else:
            logger.error("Table not found???")
            logger.error(etree.tostring(html))
            return None

        columns = None
        data = []
        for tr in table.xpath(".//tr"):
            if tr.attrib["class"] in ("tableHead44",):
                columns = tr.xpath("./th/text()")
                print(columns)
            elif tr.attrib["class"] in ("tableRow1", "tableRow2"):
                v = []
                for cell in tr.xpath("./td"):
                    text = cell.xpath("./text()")
                    print(text)
                    if text:
                        v.append(text[0])
                    else:
                        v.append(np.nan)
                v = np.array(v)
                mask = np.array(tr.xpath("./td/@class"))
                print(v)
                print(mask)
                v = np.where((mask == "td1-NotIsValid1") | (mask == "td1-IsOs") | (v == "\xa0"), np.nan, v)
                data.append(v)

        df = pd.DataFrame(data, columns=columns)
        print(df)
        return df

    def save_rt_data(self):
        html = etree.parse(str(self.rt_html_path), etree.HTMLParser(encoding="utf-8"))
        table = html.xpath("//table[@id='containerTB']")
        if table:
            table = table[0]
        else:
            logger.error("Table not found???")
            logger.error(etree.tostring(html))
            return None

        columns = None
        data = []
        for tr in table.xpath(".//tr"):
            if tr.attrib["class"] in ("tableHead44",):
                columns = tr.xpath("./th/text()")
                print(columns)
            elif tr.attrib["class"] in ("tableRow1", "tableRow2"):
                v = []
                for cell in tr.xpath("./td"):
                    text = cell.xpath("./text()")
                    print(text)
                    if text:
                        v.append(text[0])
                    else:
                        v.append(np.nan)
                v = np.array(v)
                mask = np.array(tr.xpath("./td/@class"))
                print(v)
                print(mask)
                v = np.where((mask == "td1-NotIsValid1") | (mask == "td1-IsOs") | (v == "\xa0"), np.nan, v)
                data.append(v)

        df = pd.DataFrame(data, columns=columns)

        df = df[["??????", "????????????", "PM2.5(mg/m3)", "PM10(mg/m3)", "NO2(mg/m3)", "O3(mg/m3)"]]
        df = df.rename({"??????": "ID", "????????????": "STATION_NAME", "PM2.5(mg/m3)": "PM25",
                        "PM10(mg/m3)": "PM10", "NO2(mg/m3)": "NO2", "O3(mg/m3)": "O3"}, axis=1)

        df.set_index("ID", inplace=True)
        df["PM25"] = df["PM25"].astype(float) * 1000
        df["PM10"] = df["PM10"].astype(float) * 1000
        df["NO2"] = df["NO2"].astype(float) * 1000
        df["O3"] = df["O3"].astype(float) * 1000

        # ???????????????
        df = df.set_index("STATION_NAME")
        df = df.applymap(lambda x: float(x) if 0 < float(x) < 1000 else np.nan)
        df.loc[df["PM25"] > df["PM10"], "PM10"] = np.nan

        df = df.rename({"????????????": "??????", "????????????": "??????",
                        "???????????????": "?????????", "???????????????": "?????????"})
        sites = ['?????????', '?????????', '????????????', '?????????', '?????????', '?????????', '???????????????',
                 '?????????', '?????????', '??????', '??????', '??????', '?????????']
        df = df.loc[sites, :]
        df.to_csv(self.rt_data_path, float_format="%.0f")
        logger.info("rt_data????????????=>{}", self.rt_data_path)
        return True

    def save_wuxi_data(self):
        service = CNEMC()
        try:
            res = service.get_city_history(city_code="320200")

            d = res["GetCityAQIPublishHistoriesResponse"]["GetCityAQIPublishHistoriesResult"]["RootResults"][
                "CityAQIPublishHistory"]
            df = pd.DataFrame(d)
            df = df.loc[:, ["TimePoint", "PM2_5", "PM10", "NO2", "O3", "CO", "SO2", "AQI"]]
            df = df.set_index("TimePoint")
            df.index = pd.to_datetime(df.index, format="%Y-%m-%dT%H:%M:%S")
            logger.info(df)
        except Exception as e:
            logger.error(e)
            return None

        data_hour = df.index.max().strftime("%Y%m%d%H")
        now_hour = arrow.now().shift(minutes=-27).format("YYYYMMDDHH")
        logger.info("data_hour=>{}", data_hour)
        logger.info("now_hour=>{}", now_hour)
        if data_hour == now_hour:
            df.to_csv(self.wuxi_data_path)
            return d

    def save_suzhou_data(self):
        service = CNEMC()
        try:
            res = service.get_city_history(city_code="320500")

            d = res["GetCityAQIPublishHistoriesResponse"]["GetCityAQIPublishHistoriesResult"]["RootResults"][
                "CityAQIPublishHistory"]
            df = pd.DataFrame(d)
            df = df.loc[:, ["TimePoint", "PM2_5", "PM10", "NO2", "O3", "CO", "SO2", "AQI"]]
            df = df.set_index("TimePoint")
            df.index = pd.to_datetime(df.index, format="%Y-%m-%dT%H:%M:%S")
            logger.info(df)
        except Exception as e:
            logger.error(e)
            return None

        data_hour = df.index.max().strftime("%Y%m%d%H")
        now_hour = arrow.now().shift(minutes=-27).format("YYYYMMDDHH")
        # now_hour = arrow.now().format("YYYYMMDDHH")
        logger.info("data_hour=>{}", data_hour)
        logger.info("now_hour=>{}", now_hour)
        if data_hour == now_hour:
            df.to_csv(self.suzhou_data_path)
            return d

    def save_daily_data(self):
        dfs = []
        for station_name, station_id in STATIONS.items():
            logger.info("????????????????????????=>{},{}", station_name, station_id)
            station_df = self.get_station_data(station_id, station_name)
            dfs.append(station_df)
        logger.info("????????????????????????")
        all_station = pd.concat(dfs)
        daily_mean = all_station.groupby("STATION_NAME").mean()
        daily_mean.to_csv(self.daily_data_path, float_format="%.0f")
        logger.info("daily_data????????????=>{}", self.daily_data_path)

    def get_starions_data_minute(self):
        # ????????????????????????
        url = "http://112.25.188.53:12080/njeqs/RTDataShow/AirHISDataShow.aspx"
        params = {
            'fid': '689'
        }
        response = self.session.get(url, params=params)
        logger.info(response)
        html = etree.HTML(response.text)

        __VIEWSTATE = html.xpath("*//input[@id='__VIEWSTATE']/@value")
        __VIEWSTATEGENERATOR = html.xpath(
            "*//input[@id='__VIEWSTATEGENERATOR']/@value")
        __EVENTVALIDATION = html.xpath(
            "*//input[@id='__EVENTVALIDATION']/@value")

        if __VIEWSTATE and __VIEWSTATEGENERATOR and __EVENTVALIDATION:
            data_params = {
                "__VIEWSTATE": __VIEWSTATE[0],
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR[0],
                "__EVENTVALIDATION": __EVENTVALIDATION[0]
            }
            logger.info("??????????????????????????????=>{}", data_params)
        else:
            logger.error("??????????????????????????????=>{}", response.text)

        all_station = []
        for station_name, station_id in STATIONS.items():
            logger.info("????????????????????????=>{},{}", station_name, station_id)
            start = arrow.now().shift(hours=-1).floor("day").format("YYYY-MM-DD HH:00")
            end = arrow.now().shift(hours=-1).floor("day").shift(days=1).format("YYYY-MM-DD HH:00")
            end = arrow.now().format("YYYY-MM-DD HH:00")

            logger.info(start, end)
            data = {
                **data_params,
                'strAuditData': '',
                'ddlStationID': station_id,
                'strStartTime': start,
                'strEndTime': end,
                'btnQuery': '\u67E5 \u8BE2',
                'dimMonOptTypeId': '6'
            }
            try:
                response = self.session.post(url, params=params, data=data, timeout=10)
            except requests.exceptions.RequestException as e:
                logger.error("??????3??????????????????=>{}", e)
                return False

            html = etree.HTML(response.text)

            df = pd.read_html(io.StringIO(response.text),
                              encoding="utf-8", attrs={'id': 'tblContainer'})[0]
            df = df.set_index("??????")
            df.index = pd.to_datetime(df.index, format="%Y-%m-%d %H:%M", errors="coerce")

            s = df.loc[df.index.notna(), "O3 (mg/m3)"]
            s.name = station_name
            all_station.append(s)

        all_station_minute_df = pd.concat(all_station, axis=1) * 1000
        return all_station_minute_df

    def save_daily_data2(self):
        # ????????????????????????
        url = 'http://112.25.188.53:12080/njeqs/DataQuery/AirStationDataStat.aspx'
        params = (
            ('strDimMonOptTypeID', '6'),
            ('strTimeGranularity', '20'),
            ('ftid', '603'),
        )
        data = {
            'ddlStationID': '2001',
        }
        response = self.session.post(url, params=params, data=data)
        html = etree.HTML(response.text)

        __VIEWSTATE = html.xpath("*//input[@id='__VIEWSTATE']/@value")
        __VIEWSTATEGENERATOR = html.xpath(
            "*//input[@id='__VIEWSTATEGENERATOR']/@value")
        __EVENTVALIDATION = html.xpath(
            "*//input[@id='__EVENTVALIDATION']/@value")

        if __VIEWSTATE and __VIEWSTATEGENERATOR and __EVENTVALIDATION:
            data_params = {
                "__VIEWSTATE": __VIEWSTATE[0],
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR[0],
                "__EVENTVALIDATION": __EVENTVALIDATION[0]
            }
            logger.info("??????????????????????????????=>{}", data_params)
        else:
            logger.error("??????????????????????????????=>{}", response.text)
        # ??????????????????
        dfs = []
        for station_name, station_id in STATIONS.items():
            logger.info("????????????????????????=>{},{}", station_name, station_id)
            start = arrow.now().shift(hours=-1).floor("day").format("YYYY-MM-DD HH:00")
            end = arrow.now().shift(hours=-1).floor("day").shift(days=1).format("YYYY-MM-DD HH:00")
            end = arrow.now().format("YYYY-MM-DD HH:00")

            logger.info(start, end)
            data = {
                **data_params,
                'ddlStationID': station_id,
                'strAuditData': '',
                'strAuditButton': '',
                'ddlDataType': '2',
                'strStartTime': start,
                'strEndTime': end,
                'btnQuery': '\u67E5 \u8BE2'
            }
            try:
                response = self.session.post('http://112.25.188.53:12080/njeqs/DataQuery/AirStationDataStat.aspx',
                                             params=params, data=data, timeout=10)
            except requests.exceptions.RequestException as e:
                logger.error("??????3??????????????????=>{}", e)
                return False

            html = etree.HTML(response.text)

            # df = pd.read_html(io.StringIO(response.text),
            #                   encoding="utf-8", attrs={'id': 'tblContainer'})[0]
            table = html.xpath("//table[@id='tblContainer']")
            if table:
                table = table[0]
            else:
                logger.error("Table not found???")
                logger.error(etree.tostring(html))
                return None

            columns = None
            data = []
            for tr in table.xpath(".//tr"):
                if tr.attrib["class"] in ("tableHead44",):
                    columns = tr.xpath("./th/text()")
                elif tr.attrib["class"] in ("tableRow1", "tableRow2"):
                    v = []
                    for cell in tr.xpath("./td"):
                        text = cell.xpath("./text()")
                        if text:
                            v.append(text[0])
                        else:
                            v.append(np.nan)
                    v = np.array(v)
                    mask = np.array(tr.xpath("./td/@class"))
                    v = np.where((mask == "td1-NotIsValid1") | (mask == "td1-IsOs") | (v == "\xa0"), np.nan, v)
                    data.append(v)

            df = pd.DataFrame(data, columns=columns)

            datetime = self.get_datetime()
            history_file = self.rt.parent.joinpath("api").joinpath(
                f"{datetime.format('YYYY-MM-DDTHH')}_{station_name}.csv")

            df.to_csv(history_file, index=None)
            df = df.loc[:, ["??????", "PM2.5(mg/m3)", "PM10(mg/m3)", "NO2(mg/m3)", "O3(mg/m3)"]]
            df = df.rename({"??????": "DATETIME",
                            "PM2.5(mg/m3)": "PM25_CUM",
                            "PM10(mg/m3)": "PM10_CUM",
                            "NO2(mg/m3)": "NO2_CUM",
                            "O3(mg/m3)": "O3_CUM"
                            }, axis=1)
            df = df.set_index("DATETIME")
            df.index = pd.to_datetime(df.index.astype(str), format="%Y-%m-%d %H:%M", errors="coerce")
            df = df.loc[df.index.notna()]
            df = df.shift(periods=1, freq="H")
            # ???????????????
            df = df.applymap(lambda x: float(x) * 1000 if 0 < float(x) < 1.0 else np.nan)
            # PM25>PM10
            df.loc[df["PM25_CUM"] > df["PM10_CUM"], "PM10_CUM"] = np.nan
            df["O38H"] = df["O3_CUM"].rolling(8, 6).mean()
            df["STATION_NAME"] = station_name
            logger.info(df)
            dfs.append(df)

        logger.info("????????????????????????")
        all_station = pd.concat(dfs)
        daily_mean = all_station.groupby("STATION_NAME").mean()
        daily_mean["O3_CUM"] = all_station.groupby("STATION_NAME").max()["O38H"]

        daily_mean.to_csv(self.daily_data_path, float_format="%.0f")
        logger.info("daily_data????????????=>{}", self.daily_data_path)

    def get_station_data(self, station_id, station_name):
        url = "http://112.25.188.53:12080/njeqs/RTDataShow/AirHISDataShow_RTDB.aspx"
        params = {
            'strStationID': str(station_id)
        }
        logger.info("??????????????????...")
        station_res = self.session.get(url, params=params, timeout=30)

        df = pd.read_html(io.StringIO(station_res.text),
                          encoding="utf-8", attrs={'id': 'tblContainer'})[0]

        df = df.loc[:, ["??????", "PM2.5(mg/m3)", "PM10(mg/m3)", "NO2(mg/m3)", "O3(mg/m3)"]].iloc[:-3]
        df = df.rename({"??????": "DATETIME",
                        "PM2.5(mg/m3)": "PM25_CUM",
                        "PM10(mg/m3)": "PM10_CUM",
                        "NO2(mg/m3)": "NO2_CUM",
                        "O3(mg/m3)": "O3_CUM"
                        }, axis=1)
        df = df.set_index("DATETIME")
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d %H:%M", errors="ignore")
        df = df.loc[df.index.minute == 0, :]
        df = df * 1000
        df["STATION_NAME"] = station_name
        logger.info("????????????...")
        return df

    def save_txt(self, rt_df, daily_df, wu_data, suzhou_data):

        rt_df.loc["??????", :] = rt_df.mean()
        rt_df.loc["??????", :] = wu_data.iloc[-1, :].values
        rt_df.loc["??????", :] = suzhou_data.iloc[-1, :].values

        daily_df.loc["??????", :] = daily_df.mean()
        daily_df.loc["??????", :] = wu_data.mean().values
        daily_df.loc["??????", :] = suzhou_data.mean().values

        rts = rt_df.stack()
        rts.name = "??????"

        daily = daily_df.rename({"PM25_CUM": "PM25", "PM10_CUM": "PM10", "NO2_CUM": "NO2", "O3_CUM": "O3"}, axis=1)
        print(daily_df)
        dailys = daily.stack(dropna=False)
        dailys.name = "????????????"
        print(rts, dailys)
        res = pd.concat([rts, dailys], axis=1, )
        resstr = res.loc[rt_df.index].applymap(lambda x: "" if np.isnan(x) else round(x))
        resstr.index.names = ["??????", "??????"]
        datetime = self.get_datetime()
        resstr.to_csv(f"history/{datetime.format('YYYY-MM-DDTHH')}.txt")

    def write_excel(self):
        # ???????????????
        rt_df = pd.read_csv(self.rt_data_path, index_col=0)
        daily_df = pd.read_csv(self.daily_data_path, index_col=0)
        all_df = pd.concat([rt_df, daily_df], axis=1)
        all_df = all_df[["PM25", "PM25_CUM", "PM10", "PM10_CUM", "NO2", "NO2_CUM", "O3", "O3_CUM"]]
        # all_df.iloc[-2:,:] = np.nan
        datetime = self.get_datetime()
        wb = openpyxl.load_workbook(f"static/template5.xlsx")
        ws = wb["DATA"]

        ws["C1"].value = self.get_datetime().format("YYYY-MM-DD HH:mm")

        strdf = all_df.applymap(lambda x: int(x) if not np.isnan(x) else "-")
        for i, row in enumerate(ws["C4:H16"]):
            for j, col in enumerate(row):
                col.value = strdf.iloc[i, j]

        # ??????
        wu_data = pd.read_csv(self.wuxi_data_path, index_col=0, parse_dates=True, na_values=["-", "???", ""])
        wu_data = wu_data.loc[:, ["PM2_5", "PM10", "NO2", "O3"]]
        wu_day = wu_data.mean()
        wu_day_o38h = wu_data["O3"].rolling(8, 8).mean().max()

        ws["C18"] = wu_data.iloc[-1, 0]
        ws["D18"] = round(wu_day[0])
        ws["E18"] = wu_data.iloc[-1, 1]
        ws["F18"] = round(wu_day[1])
        ws["G18"] = wu_data.iloc[-1, 2]
        ws["H18"] = round(wu_day[2])
        ws["I18"] = wu_data.iloc[-1, 3]
        ws["J18"] = "-" if np.isnan(wu_day_o38h) else round(wu_day_o38h)

        # ??????
        suzhou_data = pd.read_csv(self.suzhou_data_path, index_col=0, parse_dates=True, na_values=["-", "???", ""])
        suzhou_data = suzhou_data.loc[:, ["PM2_5", "PM10", "NO2", "O3"]]
        suzhou_day = suzhou_data.mean()
        suzhou_day_o38h = suzhou_data["O3"].rolling(8, 8).mean().max()

        ws["C19"] = suzhou_data.iloc[-1, 0]
        ws["D19"] = round(suzhou_day[0])
        ws["E19"] = suzhou_data.iloc[-1, 1]
        ws["F19"] = round(suzhou_day[1])
        ws["G19"] = suzhou_data.iloc[-1, 2]
        ws["H19"] = round(suzhou_day[2])
        ws["I19"] = suzhou_data.iloc[-1, 3]
        ws["J19"] = "-" if np.isnan(suzhou_day_o38h) else round(suzhou_day_o38h)

        for i, row in enumerate(ws["C4:J16"]):
            for j, col in enumerate(row):
                col.value = strdf.iloc[i, j]
        excel_path = f"excel/{datetime.format('YYYY-MM-DDTHH')}.xlsx"
        logger.info("???????????????=>{}", excel_path)
        wb.save(excel_path)

        # ?????????????????????
        all_df.to_csv(self.all_data_path, float_format="%.0f")

        # ??????????????????

        # self.save_txt(rt_df,daily_df,wu_data,suzhou_data)

    def plot_ts(self):

        # df = self.get_starions_data_minute()
        # df["??????"] = df.mean(axis=1)
        # df.to_csv("minute.csv")
        df = pd.read_csv("minute.csv", index_col=0, parse_dates=True)
        df = df.applymap(lambda x: x if x < 300 else np.nan)
        print(df)
        fig = plt.figure(figsize=(18, 10))
        ax = fig.add_subplot(111)
        for name, d in df.iteritems():
            ax.plot(d.index, d, label=name)

        ax.set_ylabel("$O_{3}$?????? $\mu g/m^{3}$")

        ax.set_xlim(df.index.min())
        ax.xaxis.set_major_locator(mdate.HourLocator(byhour=[0, 3, 6, 9, 12, 15, 18, 21]))
        ax.xaxis.set_major_formatter(mdate.DateFormatter('%H'))  # ??????????????????????????????
        ax.legend(frameon=False, ncol=7, loc="upper center", bbox_to_anchor=(0.5, -0.1), fontsize=16)
        plt.savefig(f"o3.png", dpi=400, bbox_inches='tight')

    def run(self, force=False):
        if self.is_update() or force:
            datetime = self.get_datetime()
            logger.info("???????????????=>{}", datetime)

            self.save_rt_data()
            self.save_daily_data2()
            if self.save_wuxi_data() and self.save_suzhou_data():
                logger.info("???????????????")
                self.write_excel()
                self.datetime_tag_path.write_text(datetime.format("YYYY-MM-DDTHH"))
                return True
        else:
            logger.info("??????????????????")
            return False


if __name__ == "__main__":
    crawer = Crawler()
    # crawer.run()
    # crawer.get_starions_data_minute()
    # crawer.get_all_station()
    crawer.plot_ts()
    # logger.info(crawer.get_dataframe())
