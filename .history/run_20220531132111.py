#!/home/kjgong/software/python3/bin/python3
from loguru import logger
from pathlib import Path
import os
import arrow
import time 
import click
import yagmail

from crawler import Cnemc
from generator import ExcelGenerator,ImageGenerator
from push import WeComPush, EmailPush


os.chdir(Path(__file__).absolute().parent)
print(os.getcwd())
TODAY = arrow.now().format("YYYYMMDD")

logger.add(f"logs/{TODAY}.log")


@click.option("-t", "--test",is_flag=True,help='test')
@click.option("-d", "--date",help='report date')
@click.command()
def main(date,test):
    datetime_tag_file = Path("datetime.tag")
    if datetime_tag_file.is_file():
        last_tag = datetime_tag_file.read_text().strip()
    else:
        last_tag = None
    logger.info("last_tag=>{}",last_tag)
    datetime_tag =  arrow.now().format("YYYY-MM-DDTHH")
    logger.info("now_tag=>{}",datetime_tag)

    if not test:
        if datetime_tag == last_tag:
            logger.error("本小时已经推送过=>{}",datetime_tag)
            return None

    time_h = arrow.now().shift(minutes=-30)
    df = Cnemc().run()
    output_image = ImageGenerator(df).run()
    output_excel = ExcelGenerator(df).run()

    ###邮箱推送
    if not test:
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


    push = WeComPush()
    push.send(output_image, msgtype="image",touser="GongKangJia")
    push.send(output_excel, msgtype="file", touser="GongKangJia")


    datetime_tag_file.write_text(datetime_tag)
#
# @click.command()
# @click.option("-f", "--force",is_flag=True,help='Run force')
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

