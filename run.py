#!/home/kjgong/software/python3/bin/python3
from loguru import logger
from pathlib import Path
import os
import arrow
import time 
import click
import yagmail
from func_timeout import FunctionTimedOut

from crawler import Cnemc,Moji
from generator import ExcelGenerator,ImageGenerator,JiangningImage,JiangningTextGenerator
from push import WeComPush, EmailPush, WechatPush




os.chdir(Path(__file__).absolute().parent)
print(os.getcwd())
TODAY = arrow.now().format("YYYYMMDD")

logger.add(f"logs/{TODAY}.log")


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
    datetime_tag =  arrow.now().format("YYYY-MM-DDTHH")
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

        #推送江宁的
#        try:
#           logger.info("开始微信推送")
#            wechat = WechatPush()
#            wechat.send(str(jnoutput_image),msgtype="image",to="指挥中心")
#        except FunctionTimedOut as e:
#            logger.error("江宁微信推送失败")
#            push = WeComPush()
#            push.send("江宁微信推送失败!", msgtype="text", touser="GongKangJia")
    push = WeComPush()
    push.send(output_image, msgtype="image",touser="GongKangJia")
    push.send(output_excel, msgtype="file", touser="GongKangJia")


    if jnoutput_image and jiangning_text:
        # push.send(jnoutput_image, msgtype="image", touser="GongKangJia")
        # push.send(jnoutput_image, msgtype="image", touser="noreply")
        # push.send(jnoutput_image, msgtype="image", touser="ZhangHaoRan")
        # push.send(jiangning_text, totag=5)
        push.send(jiangning_text, totag=5)
        push.send(jnoutput_image, msgtype="image", totag=5)

    else:
        push.send("小时推送暂时无数据！", totag=5)

#
# @click.command()
# @click.option("-d", "--daemon",is_flag=True,help='Run daemon')
# def main(force, daemon):
#     if force:
#         run(force)
#         return
#     if daemon:
#         while True:
#             now =  arrow.now()
#             logger.info("现在的时间=>{}",now.format())
#             # As crond "*/5"
#             delay = 300-int(arrow.now().timestamp())%300
#             logger.info("下次执行时间为=>{}",now.shift(seconds=delay).format())
#             logger.info("等待{}秒",delay)
#             time.sleep(delay)
#             logger.info("开始执行=>{}",now)
#             run()
#     else:
#         run()

if __name__ == "__main__":
    main()
    #wechat = WechatPush()
    #wechat.send("./output/2022-12-18T23.png",msgtype="image",to="Gongbot")
