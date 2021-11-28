import requests
import base64
from pathlib import Path
from .config import AK, SK
from loguru import logger

class VC:
    def __init__(self) -> None:
        self.rt = Path(__file__).parent.parent.absolute()/ "rt"
        self.validate_code_path = self.rt / Path("vc.png")
        self.access_token_path = self.rt / Path(".access_token")

    @logger.catch
    def refresh_token(self):
        logger.info("call refresh_token...")
        host = "https://aip.baidubce.com/oauth/2.0/token"
        params = {
            "grant_type": "client_credentials",
            "client_id": AK,
            "client_secret": SK
        }
        logger.info("refresh token...")
        response = requests.get(host, params=params).json()
        self.access_token_path.write_text(response["access_token"])
        return response["access_token"]

    @property
    def access_token(self):
        if self.access_token_path.is_file():
            return self.access_token_path.read_text()
        else:
            return self.refresh_token()

    @logger.catch
    def get_words(self, image=None):
        logger.info("打开验证码图片,获取数据")
        img = base64.b64encode(self.validate_code_path.read_bytes())
        logger.info("获取数据成功,数据大小为=>{}",len(img))

        logger.info("开始请求,获取验证码数字")
        host = "https://aip.baidubce.com/rest/2.0/ocr/v1/numbers"
        params = {"access_token": self.access_token}
        data = {"image": img}
        headers = {'content-type': 'application/x-www-form-urlencoded'}

        response = requests.post(host, params=params,
                                 data=data, headers=headers).json()
        logger.info("请求结果为=>{}",response)

        if response.get("error_code") == 110:
            logger.info("access_token过期,重新获取...")
            self.refresh_token()
            return self.get_words()
        if response["words_result_num"] != 1:
            logger.error("验证码识别错误")
            return None
        else:
            code = response["words_result"][0]["words"]
            logger.info("验证码识别成功=>{}",code)

            return code

if __name__ == "__main__":
    vc = VC()
    res = vc.get_words()
    print(res)
