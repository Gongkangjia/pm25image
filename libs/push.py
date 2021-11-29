import abc
from .config import CORPID, CORPSECRET, AGENTID, USER, HOST, PASSWORD
import requests
from loguru import logger
import yagmail
import shortuuid
from pathlib import Path


class Push(metaclass=abc.ABCMeta):
    def __init__(self):
        self.rt = Path(__file__).parent.parent.absolute()/"rt"
        self.image_path = self.rt / Path("result.png")
        self.datetime_tag = self.rt / "datetime.tag"

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
        datetime = self.datetime_tag.read_text()
        filename = f"{datetime.replace(' ','T')}_{shortuuid.uuid()}.png"

        params = {"access_token": self.access_token, "type": filetype}
        data = {"file": (filename,self.image_path.read_bytes())}

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

    def mail(self, subject, content=None, files=None) -> None:
        logger.info("开始发送邮件 =>{},{},{}", subject, content, files)
        sta = self.yag.send(["gongkangjia@qq.com"], subject, content, files)
        if sta is not False:
            logger.info("邮件发送成功")
        else:
            logger.error("邮件发送不成功 => {}", sta)

    def run(self):
        self.mail("【空气质量速报】", files=self.image_path)


if __name__ == "__main__":
    # push = QiyeWechatPush()
    # push.run()
    push = EmailPush()
    push.run()
