import time
import numpy as np
import openpyxl
import arrow
import pandas as pd
import requests
import json
from lxml import etree
from pathlib import Path
from loguru import logger

from .vc import VC


class Crawler:
    def __init__(self) -> None:
        self.rt = Path(__file__).parent.parent.absolute()/ "rt"
        self.cookies_path = self.rt / Path(".cookies.json")
        self.validate_code_path = self.rt / Path("vc.png")
        self.data_html_path = self.rt / Path("data.html")
        self.datetime_tag_path = self.rt / Path("datetime.tag")

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

    def get_login_params(self):
        url = 'http://112.25.188.53:12080/njeqs/Default.aspx'
        logger.info("开始获取index页面")
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
            logger.info("获取登录参数=>{}", params)
        else:
            logger.error("获取登录参数失败=>{}", response.text)

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
        # 获取验证码
        logger.info("正在获取验证码图片")
        url = "http://112.25.188.53:12080/njeqs/ValidateCodeCreate.aspx"
        img = self.session.get(url)
        if img.status_code == 200:
            logger.info("获取验证码图片成功,正在保存=>{}", self.validate_code_path)
        self.validate_code_path.write_bytes(img.content)
        logger.info("保存成功=>{}", self.validate_code_path)
        vc = VC()
        words = vc.get_words()
        return words

    def dump_session(self):
        logger.info("dumping session=>{}", self.cookies_path)
        with open(self.cookies_path, "w") as f:
            cookies_dict = requests.utils.dict_from_cookiejar(
                self.session.cookies)
            logger.info("获取到cookies为=>{}", cookies_dict)
            json.dump(cookies_dict, f)
        logger.info("保存会话成功=>{}", self.cookies_path)

    def load_session(self):
        if self.cookies_path.is_file():
            logger.info("发现cookies文件,正在恢复")
            with open(self.cookies_path, "r") as f:
                try:
                    cookies_dict = json.load(f)
                    cookies = requests.utils.cookiejar_from_dict(cookies_dict)
                    self.session.cookies = cookies
                    logger.info("恢复会话成功")
                except Exception as e:
                    logger.error("恢复会话失败=>{}", e)
                    logger.info("重新登录...")
                    self.login()
        else:
            logger.info("没有cookies,重新登录...")
            self.login()

    def login(self):
        for t in range(10):
            self.init_session()
            logger.info("第{}次登录..", t+1)
            data = self.get_login_params()
            logger.info("登录数据为=>{}", data)
            logger.info("开始登录")
            login_url = 'http://112.25.188.53:12080/njeqs/Default.aspx'
            login_res = self.session.post(login_url, data=data)

            if login_res.status_code == 200 and "注消当前登录用户" in login_res.text:
                logger.info("登录成功")
                self.dump_session()
                break
            else:
                html = etree.HTML(login_res.text)
                error_msg = html.xpath("*//span[@id='hintTD']/text()")
                logger.error("登录失败=>{}", login_res.text)
                logger.error("错误信息为=>{}", error_msg)
                logger.error("等待十秒再试...")
                time.sleep(10)
        else:
            logger.info("十次登录失败,请及时处理...")
            from push import EmailPush
            push = EmailPush()
            push.mail("【空气质量速报】告警！！！", content="十次登录失败,请及时处理...")

    def get_data(self):
        url = 'http://112.25.188.53:12080/njeqs/DataQuery/AirStationLastHourDataStat.aspx'
        params = {
            'strStationIds': 'Station.id in(2001,2002,2003,2004,2006,2007,2008,2009,2010,2016,2022,2024,2036)',
            'ftid': '578'
        }
        logger.info("开始获取数据,请求url为=>{}", url)
        response = self.session.post(url, params=params)
        logger.info("请求结果长度为=>{}", len(response.text))
        # 判断请求数据是否正常
        if "loginBtn" in response.text:
            logger.error("cookies失效,重新登录...")
            self.login()
            return self.get_data()
        else:
            logger.info("获取数据成功可用")
            self.data_html_path.write_text(response.text)
            logger.info("请求结果写入文件成功=>{}", self.data_html_path)

    def get_datetime(self):
        html = etree.HTML(self.data_html_path.read_text())
        datetime = html.xpath("*//input[@id='strStartTime']/@value")
        datetime = arrow.get(datetime[0]).shift(hours=1)
        datetime = datetime.format("YYYY-MM-DD HH:mm")
        return datetime

    def get_dataframe(self):
        df = pd.read_html(self.data_html_path, encoding="utf-8",
                          attrs={'id': 'containerTB'})[0]

        df = df[["序号", "站点名称", "PM2.5(mg/m3)", "PM10(mg/m3)", "NO2(mg/m3)"]]
        df = df.rename({"序号": "ID", "站点名称": "NAME", "PM2.5(mg/m3)": "PM25",
                        "PM10(mg/m3)": "PM10", "NO2(mg/m3)": "NO2"}, axis=1)

        df.set_index("ID", inplace=True)
        df["PM25"] = df["PM25"]*1000
        df["PM10"] = df["PM10"]*1000
        df["NO2"] = df["NO2"]*1000

        df = df.set_index("NAME")

        df = df.rename({"六合雄州": "雄州", "溧水永阳": "永阳",
                       "高淳老职中": "老职中", "江宁彩虹桥": "彩虹桥"})
        sites = ['玄武湖', '瑞金路', '奥体中心',  '草场门', '山西路', '迈皋桥', '仙林大学城',
                 '中华门', '彩虹桥', '浦口', '雄州', '永阳', '老职中']
        return df.loc[sites, :]

    def write_excel(self):
        df = self.get_dataframe()
        datetime = self.get_datetime()

        wb = openpyxl.load_workbook(f"static/template.xlsx")
        ws = wb["DATA"]

        ws["C1"].value = self.get_datetime()

        strdf = df.applymap(lambda x: int(x) if not np.isnan(x) else "-")
        for i, row in enumerate(ws["C3:E15"]):
            for j, col in enumerate(row):
                col.value = strdf.iloc[i, j]
        wb.save(f"excel/{datetime.replace(' ', 'T')}.xlsx")

    def run(self):
        self.get_data()
        new = self.get_datetime()

        if self.datetime_tag_path.is_file():
            last = self.datetime_tag_path.read_text()
        else:
            last = None
            
        if  new == last:
            logger.info("还没有新数据")
            return False
        else:
            self.write_excel()
            self.datetime_tag_path.write_text(new)
            return True

if __name__ == "__main__":
    crawer = Crawler()
    crawer.run()
    logger.info(crawer.get_dataframe())
