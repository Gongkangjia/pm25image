from loguru import logger
from pathlib import Path
import os
import arrow
import time 
import click

from libs.draw import DrawImage
from libs.crawler import Crawler
from libs.push import QiyeWechatPush,EmailPush


os.chdir(Path(__file__).parent.absolute())
TODAY = arrow.now().format("YYYYMMDD")

logger.add(f"logs/{TODAY}.log")


def run(force=False):
    crawler = Crawler()

    if crawler.run() or force:
        d = DrawImage()
        d.run()

        PUSHCLASS = [
            QiyeWechatPush,
            EmailPush
            ]

        for push in PUSHCLASS:
            pusher = push()
            pusher.run()


@click.command()
@click.option("-f", "--force",is_flag=True,help='Run force')
def main(force):
    if force:
        run(force)
    else:
        while True:
            now =  arrow.now()
            logger.info("现在的时间=>{}",now.format())
            # As crond "*/5"
            delay = 300-int(arrow.now().timestamp())%300
            logger.info("下次执行时间为=>{}",now.shift(seconds=delay).format())
            logger.info("等待{}秒",delay)
            time.sleep(delay)
            logger.info("开始执行=>{}",now)
            run()

if __name__ == "__main__":
    main()

