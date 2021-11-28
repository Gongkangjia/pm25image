from PIL import Image, ImageDraw, ImageFont
import numpy as np
import pandas as pd
import arrow
from pathlib import Path
from lxml import etree
from loguru import logger

class DrawImage:
    def __init__(self):
        self.root = Path(__file__).parent.parent.absolute()
        self.data_html_path = self.root/ "rt" / Path("data.html")
        self.image_path = self.root/ "rt" / Path("result.png")
        self.datetime_tag_path = self.root/ "rt"  / Path("datetime.tag")

        self.columns_width = [850, 850, 1200, 1200, 1200]
        self.row_height = [180, 620, *[180]*14]
        self.fonesize = 140
        self.font = ImageFont.truetype(font=str(self.root/'static/kjgong.ttf'), size=self.fonesize)
        self.linewidth = 6
        self.spacing = self.fonesize*0.5
        self.start = (10, 10)

    def create_figure(self):
        logger.info("创建画布")
        width = sum(self.columns_width)
        height = sum(self.row_height)
        self.image = Image.new(mode="RGB", size=(width+20, height+20), color="white")
        self.draw = ImageDraw.Draw(self.image)
        x, y, w, h = *self.start, width, height
        logger.info("画大矩形")
        self.draw.rectangle(xy=((x, y), (x+w, y+h)),
                            fill=None, outline="black", width=12)

    def draw_rec_text(self, loc, text,fill="black"):
        x, y, w, h = self.get_location(*loc)
        self.draw.rectangle(xy=((x, y), (x+w, y+h)), fill=None,
                            outline="black", width=self.linewidth)
        if isinstance(text, str):
            text = [text]
        for i, t in enumerate(text):
            fsize = self.font.getsize(t)
            height_offset = (i-len(text)/2+0.5)*(self.spacing+self.fonesize)
            self.draw.text(xy=(
                x+w/2-fsize[0]/2, y+h/2-fsize[1]/2+height_offset), text=t, fill=fill, font=self.font)

    def get_location(self, row_start, col_start, row_end=None, col_end=None,):
        if not (row_end or col_end):
            row_end = row_start
            col_end = col_start
        x = sum(self.columns_width[:col_start])+self.start[0]
        y = sum(self.row_height[:row_start])+self.start[1]
        w = sum(self.columns_width[col_start:col_end+1])
        h = sum(self.row_height[row_start:row_end+1])
        return x, y, w, h

    def get_dataframe(self):
        logger.info("读取html=>{}",self.data_html_path)
        df = pd.read_html(self.data_html_path, encoding="utf-8",attrs={'id': 'containerTB'})[0]
        df = df[["序号", "站点名称", "PM2.5(mg/m3)", "PM10(mg/m3)", "NO2(mg/m3)"]]
        df = df.rename({"序号": "ID",
                        "站点名称": "NAME",
                        "PM2.5(mg/m3)": "PM25",
                        "PM10(mg/m3)": "PM10",
                        "NO2(mg/m3)": "NO2"}, axis=1)
        df = df.set_index("NAME")
        df = df.rename({"六合雄州": "雄州", "溧水永阳": "永阳",
                       "高淳老职中": "老职中", "江宁彩虹桥": "彩虹桥"})
        sites = ['玄武湖', '瑞金路', '奥体中心',  '草场门', '山西路', '迈皋桥', '仙林大学城',
                 '中华门', '彩虹桥', '浦口', '雄州', '永阳', '老职中']
        df = df.loc[sites, :]

        df["PM25"] = df["PM25"]*1000
        df["PM10"] = df["PM10"]*1000
        df["NO2"] = df["NO2"]*1000
        df = df.reset_index()
        df["ID"] = df.index+1
        return df

    def draw_data(self):
        self.create_figure()
        logger.info("正在绘制表头")
        self.draw_rec_text((0, 0, 1, 0), "序号")
        self.draw_rec_text((0, 1, 1, 1), "点位")
        self.draw_rec_text((0, 2, 0, 4), self.datetime_tag_path.read_text())
        self.draw_rec_text((1, 2), ["PM2.5", "（微克/立方米）"])
        self.draw_rec_text((1, 3), ["PM10", "（微克/立方米）"])
        self.draw_rec_text((1, 4), ["NO2", "（微克/立方米）"])
        df = self.get_dataframe()
        for i,d in df.iterrows():
            self.draw_rec_text((d.ID+1,0),str(d.ID))
            self.draw_rec_text((d.ID+1,1),d.NAME)

        self.draw_rec_text((15,0,15,1), "全市")

        logger.info("正在绘制数据列")
        for species_index,species in enumerate(["PM25","PM10","NO2"]):
            d = df[species]
            nl = d.isin(d.nlargest(3))
            nl.name = "NL"
            d.name = "VALUE"
            tmp_df = pd.concat([d,nl],axis=1)
            #全市
            self.draw_rec_text((15,species_index+2),str(round(d.mean())))

            for index,species_value in tmp_df.iterrows():
                t = "-" if np.isnan(species_value.VALUE) else str(round(species_value.VALUE))
                fill = "red" if species_value.NL else "black"
                self.draw_rec_text((index+2,species_index+2),t,fill=fill)

    def save(self):
        logger.info("正在保存图片=>{}",self.image_path)
        self.image = self.image.resize((2000,int(self.image.size[1]/self.image.size[0]*2000)),Image.ANTIALIAS)
        self.image.save(self.image_path)

    def run(self):
        self.draw_data()
        self.save()


if __name__ == "__main__":
    d = DrawImage()
    d.run()

