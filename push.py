import abc
from config import CORPID, CORPSECRET, AGENTID, USER, HOST, PASSWORD
import requests
from loguru import logger
import yagmail
import shortuuid
import arrow
from pathlib import Path
import json
import itchat
from func_timeout import func_set_timeout

class WeComPush:
    def __init__(self):
        self.CORPID = "ww8eba91f3487e5d6e"
        self.CORPSECRET = "4A4cHr1yDuvrPsDsgNCBoxDrHB876rV4MSaMgfVsH8Y"
        self.AGENTID = 1000004
        self.TOKEN_TIME = None
        self.TOKEN_EXPIRE = 7000
        self.ACCESS_TOKEN_PATH = Path("~/.access_token.json").expanduser()

    @property
    def access_token(self):
        if self.ACCESS_TOKEN_PATH.is_file():
            logger.info("access_token存在=>{}", self.ACCESS_TOKEN_PATH)
            with open(self.ACCESS_TOKEN_PATH) as f:
                access_json = json.load(f)
                logger.info(access_json)
                logger.info("access_time=>{}", arrow.get(access_json["time"]))
                if arrow.now() > arrow.get(access_json["time"]).shift(seconds=self.TOKEN_EXPIRE):
                    logger.info("token 过期，正在重新获取...")
                    return self.__get_access_token()
                else:
                    logger.info("access_token有效，直接返回")
                    return access_json["token"]
        else:
            logger.info("access_token{}不存在,正在创建", self.ACCESS_TOKEN_PATH)
            return self.__get_access_token()

    def __get_access_token(self):
        logger.info("开始获取access_token")
        token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        params = {"corpid": self.CORPID, "corpsecret": self.CORPSECRET}
        res = requests.get(token_url, params=params).json()
        access_token = res.get("access_token")
        with open(self.ACCESS_TOKEN_PATH, "w") as f:
            dump_json = {
                "token": access_token,
                "time": arrow.now().format()
            }
            json.dump(dump_json, f)
        return access_token

        # 管理多媒体文件

    def upload_media(self, media_type, media_file):
        """
            上传媒体文件
            参数	必须	说明
            access_token	是	调用接口凭证
            type	是	媒体文件类型，分别有图片（image）、语音（voice）、视频（video），普通文件(file)
            media	是	form-data中媒体文件标识，有filename、filelength、content-type等信息
        """
        if isinstance(media_file, str):
            media_file = Path(media_file)
        url = 'https://qyapi.weixin.qq.com/cgi-bin/media/upload'
        params = {"access_token": self.access_token, "type": media_type}
        files = {"file": (media_file.name, media_file.read_bytes())}
        upload_res = requests.post(url, files=files, params=params).json()
        logger.info("上传成功=>{}", upload_res)
        if upload_res["errcode"] == 40014:
            logger.error("token失效,重新获取")
            self.__get_access_token()
            return self.upload_media(media_type, media_file)
        return upload_res["media_id"]

    def send(self, content, msgtype=None, touser="", toparty="", totag="", ):

        if not msgtype and isinstance(content, str):
            msgtype = "text"
        elif not msgtype and isinstance(content, Path):
            if content.suffix in (".png", "jpg"):
                msgtype = "image"
            else:
                msgtype = "file"
        notify_url = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
        params = {
            "access_token": self.access_token
        }
        data = {
            "touser": touser,
            "toparty": toparty,
            "totag": totag,
            "safe": 0,
            "msgtype": msgtype,
            "agentid": self.AGENTID,
            "enable_duplicate_check": 0,
            "duplicate_check_interval": 1800

        }
        if msgtype == "text":
            data["text"] = {"content": content}
        elif msgtype == "image":
            data["image"] = {"media_id": self.upload_media(msgtype, content)}
        elif msgtype == "file":
            data["file"] = {"media_id": self.upload_media(msgtype, content)}
        else:
            logger.error("error media type..")

        for _ in range(3):
            notify_json = requests.post(
                notify_url, params=params, json=data).json()
            logger.info("通知发送结果=>{}", notify_json)
            if notify_json.get("errcode") == 0:
                logger.info("通知发送结果=>成功")
                return True
                break
            elif notify_json.get("errcode") == 40014:
                self.__get_access_token()
                self.send(content, msgtype=msgtype, touser=touser, toparty=toparty, totag=totag)
                break
            else:
                logger.error(notify_json)
        else:
            self.send("预报发送失败.", touser="GongKangJia")


class EmailPush():
    def __init__(self):
        self.yag = yagmail.SMTP(user=USER,
                                password=PASSWORD,
                                host=HOST)

    def mail(self, subject, contents=None, attachments=None) -> None:

        logger.info("开始发送邮件 =>{},{},{}", subject, contents, attachments)

        sta = self.yag.send(to=[
            "qq45934861@163.com",
            "njhbjdqcgy@163.com",
            #            "kjgong@kjong.cn"
        ],
            cc="kjgong@kjgong.cn",
            subject=subject,
            contents=contents,
            attachments=attachments)

        if sta is not False:
            logger.info("邮件发送成功")
        else:
            logger.error("邮件发送不成功 => {}", sta)

class WechatPush():
    def __init__(self) -> None:
        pass

    @func_set_timeout(60)
    def send(self,content, msgtype=None, to=""):
        itchat.auto_login(hotReload=True,enableCmdQR=2)
        tos = itchat.search_friends(to) or itchat.search_chatrooms(to)
        if not tos:
            logger.error("没有找到用户或聊天室=>{}",to)
            return False
        to = tos[0].UserName
        if msgtype == "image":
            itchat.send_image(content,toUserName=to)
        else:
            itchat.send_file(content,toUserName=to)
        itchat.dump_login_status()



if __name__ == "__main__":
    # push = WeComPush()
    # push.send("ddddd",touser="noreply")
    wechat = WechatPush()
    wechat.send("./output/2022-12-18T23.png",msgtype="image",to="Gongbot")
    # itchat.auto_login(hotReload=True,enableCmdQR=2)
    # itchat.send_image("./output/2022-12-18T23.png",toUserName="filehelper")
    # itchat.send_msg("ddd",toUserName="Gongbot")
