import abc
from .config import CORPID, CORPSECRET, AGENTID, USER, HOST, PASSWORD
import requests
from loguru import logger
import yagmail
import shortuuid
import arrow
from pathlib import Path


class Push(metaclass=abc.ABCMeta):
    def __init__(self):
        self.rt = Path(__file__).parent.parent.absolute()/"rt"
        self.datetime_tag = self.rt / "datetime.tag"
        self.filename = self.datetime_tag.read_text()
        self.datetime = arrow.get(self.filename, "YYYY-MM-DDTHH")
        self.image_path = self.rt / f"{self.filename}.png"
        self.excel_path = Path(__file__).parent.parent.absolute()/ "excel" /f"{self.filename}.xlsx"

    @abc.abstractmethod
    def run(self):
        pass


class QiyeWechatPush(Push):
    def __init__(self):
        super().__init__()
        self.access_token = None
        self.CORPID = CORPID
        self.CORPSECRET = CORPSECRET

    def get_access_token(self):

        logger.info("开始获取access_token")
        token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"

        params = {
            "corpid": self.CORPID,
            "corpsecret": self.CORPSECRET
        }

        res = requests.get(token_url, params=params).json()
        self.access_token = res.get("access_token")

        logger.info("获取成功=>{}", self.access_token)

    def upload(self, filetype="image"):
        url = 'https://qyapi.weixin.qq.com/cgi-bin/media/upload'

        filename = f"{self.filename}_{shortuuid.uuid()}.png"

        params = {"access_token": self.access_token, "type": filetype}
        data = {"file": (filename, self.image_path.read_bytes())}

        upload_res = requests.post(url, files=data, params=params).json()
        logger.info("上传成功=>{}", upload_res)
        return upload_res["media_id"]

    def run(self):
        self.get_access_token()

        notify_url = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
        params = {
            "access_token": self.access_token
        }
        data = {
            "touser": "@all",
            "toparty": "@all",
            "totag": "@all",
            "msgtype": "image",
            "agentid": AGENTID,
            "image": {
                "media_id": self.upload()
            },
            "safe": 0,
            "enable_duplicate_check": 0,
            "duplicate_check_interval": 1800
        }
        for _ in range(3):
            notify_json = requests.post(
                notify_url, params=params, json=data).json()
            logger.info("通知发送结果=>{}", notify_json)
            if notify_json.get("errcode") == 0:
                logger.info("通知发送结果=>成功")
                break
            else:
                self.get_access_token()
                logger.error("通知发送结果=>失败")
                self.get_access_token()
        self.upload()


class EmailPush(Push):
    def __init__(self):
        super().__init__()
        self.yag = yagmail.SMTP(user=USER,
                                password=PASSWORD,
                                host=HOST)

    def mail(self, subject, contents=None, attachments=None) -> None:

        logger.info("开始发送邮件 =>{},{},{}", subject, contents, attachments)

        sta = self.yag.send(to=[
            "njhbjdqcgy@163.com"
        ],
            cc="kjgong@kjgong.cn",
            subject=subject,
            contents=contents,
            attachments=attachments)

        if sta is not False:
            logger.info("邮件发送成功")
        else:
            logger.error("邮件发送不成功 => {}", sta)

    def run(self):
        dt = self.datetime.format('MM月DD日HH时')
        contents = []
        contents.append(yagmail.inline(str(self.image_path)))
        footer = """
        数据来源:南京市环境空气质量自动监测平台&环境监测总站
        ---------------------------------
        龚康佳
        南京信息工程大学
        环境科学与工程学院
        kjgong@nuist.edu.cn; gongkangjia@gmail.com
        """
        contents.append(footer)

        self.mail(f"【空气质量速报】{dt}", contents=contents,attachments=[str(self.image_path),str(self.excel_path)])


if __name__ == "__main__":
    # push = QiyeWechatPush()
    # push.run()
    push = EmailPush()
    push.run()
