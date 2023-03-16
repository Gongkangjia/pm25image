#!/home/kjgong/software/python3/bin/python3
from loguru import logger
from pathlib import Path
import os
import arrow
import time 
import click
import yagmail

from crawler import Cnemc,Moji
from generator import ExcelGenerator,ImageGenerator,JiangningImage,JiangningTextGenerator
from push import WeComPush, EmailPush




os.chdir(Path(__file__).absolute().parent)
print(os.getcwd())
TODAY = arrow.now().format("YYYYMMDD")

logger.add(f"logs/{TODAY}.log")

@logger.catch()
@click.option("-t", "--test",is_flag=True,help='test')
@click.option("-d", "--date",help='report date')
@click.option("-f", "--force",is_flag=True,help='Run force')
@click.option("-s", "--source",help='Data source')
@click.command()
def main(date,test,force,source):
    datetime_tag_file = Path("datetime.tag")
    if datetime_tag_file.is_file():
        last_tag = datetime_tag_file.read_text().strip()
    else:
        last_tag = None
    logger.info("last_tag=>{}",last_tag)
    now = arrow.now()

    if now.minute < 30:
        datetime_tag = now.shift(hours=-1).format("YYYY-MM-DDTHH")
    else:
        datetime_tag = now.format("YYYY-MM-DDTHH")

    logger.info("now_tag=>{}",datetime_tag)

    if not test:
        if datetime_tag == last_tag and not force:
            logger.error("本小时已经推送过=>{}",datetime_tag)
            return None

    time_h = arrow.now().shift(minutes=-30)
    if source == "cneme":
        df = Cnemc().run()
        logger.info("数据源为CNEME")
    elif source == "moji":
        df = Moji().run()
        logger.info("数据源为Moji")
    else:
        df = Cnemc().run()
        logger.info("数据源为CNEME")

    output_image = ImageGenerator(df).run()
    output_excel = ExcelGenerator(df).run()
    jnoutput_image = JiangningImage(df).run()
    jiangning_text = JiangningTextGenerator(df).run()
    print(jiangning_text)

    if test and output_image and output_excel:
        push = WeComPush()
        push.send(output_image, msgtype="image",touser="GongKangJia")
        push.send(output_excel, msgtype="file", touser="GongKangJia")
    ###邮箱推送
    if not test and output_image and output_excel:
        push  = EmailPush()
        dt = time_h.format('MM月DD日HH时')
        contents = []
        contents.append(yagmail.inline(str(output_image)))
        footer = """
        数据来源:南京市环境空气质量自动监测平台&环境监测总站
        ---------------------------------
        龚康佳
        南京信息工程大学
        环境科学与工程学院
        kjgong@nuist.edu.cn; gongkangjia@gmail.com
        """
        contents.append(footer)
        push.mail(f"【空气质量速报】{dt}", contents=contents, attachments=[str(output_excel)])
        datetime_tag_file.write_text(datetime_tag)
        


    if jnoutput_image and jiangning_text:
        # push.send(jnoutput_image, msgtype="image", touser="GongKangJia")
        # push.send(jnoutput_image, msgtype="image", touser="noreply")
        # push.send(jnoutput_image, msgtype="image", touser="ZhangHaoRan")
        # push.send(jiangning_text, totag=5)
        push = WeComPush()
        push.send(jiangning_text, totag=5)
        push.send(jnoutput_image, msgtype="image", totag=5)

    else:
        push = WeComPush()
        push.send("小时推送暂时无数据！", totag=5)


if __name__ == "__main__":
    main()
    #wechat = WechatPush()
    #wechat.send("./output/2022-12-18T23.png",msgtype="image",to="Gongbot")
