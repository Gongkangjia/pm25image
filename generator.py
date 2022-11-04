from PIL import Image, ImageDraw, ImageFont
import numpy as np
import pandas as pd
import arrow
from pathlib import Path
from loguru import logger
from openpyxl import load_workbook
from config import STATIONS_CNEMC


class GeneratorBase:
    def __init__(self, df):
        self.df = df.reset_index()
        self.root = Path(__file__).absolute().parent
        self.output_dir = self.root.joinpath("output")
        self.time_h = arrow.now().shift(minutes=-30)


class ImageGenerator(GeneratorBase):
    def __init__(self, df,is_jn=False):
        super(ImageGenerator, self).__init__(df)
        self.is_jn  = is_jn
        self.df = self.preprocessing(df)
        self.ax = None
        self.columns_width = np.array([8, 12, 10, 10, 10, 10, 10, 10, 10, 10]) * 60
        self.row_height = [180, 360, 180, *[180] * 16]
        self.fonesize = 140
        self.font = ImageFont.truetype(font=str(self.root / 'static/kjgong.ttf'), size=self.fonesize)
        self.fontbd = ImageFont.truetype(font=str(self.root / 'static/Times New Roman Bold.ttf'), size=self.fonesize)
        self.linewidth = 6
        self.spacing = self.fonesize * 0.1
        self.start = (10, 10)

    def preprocessing(self, df):
        df = df.set_index("DATETIME")
        df = df.rename({"PM2_5": "PM25"}, axis=1)
        realtime = df.loc[self.time_h.format("YYYY-MM-DD HH:00")]
        day = df.groupby("NAME").mean()
        day["MDA8"] = df.groupby("NAME")["O3_8H"].max()
        day.at["南京","MDA8"] = day.loc[STATIONS_CNEMC.keys(),"MDA8"].mean()
        res_df = realtime.merge(day, left_on="NAME", right_index=True, suffixes=["_RT", "_DAY"])
        res_df = res_df.set_index("NAME")
        return res_df

    def draw_rec_text(self, loc, text, fill="black",font=None,rfill=None):
        if font is None:
            font =  self.font
        else:
            font = self.fontbd

        x, y, w, h = self.get_location(*loc)


        self.drawer.rectangle(xy=((x, y), (x + w, y + h)), fill=rfill,
                              outline="black", width=self.linewidth)

        text = self.get_str(text)
        for i, t in enumerate(text):
            fsize = font.getsize(t)
            height_offset = (i - len(text) / 2 + 0.5) * (self.spacing + self.fonesize)
            self.drawer.text(xy=(x + w / 2 - fsize[0] / 2, y + h / 2 - fsize[1] / 2 + height_offset),
                            text=t,
                            fill=fill,
                            font=font
               )

    def get_location(self, row_start, col_start, row_end=None, col_end=None, ):
        if not (row_end or col_end):
            row_end = row_start
            col_end = col_start
        x = sum(self.columns_width[:col_start]) + self.start[0]
        y = sum(self.row_height[:row_start]) + self.start[1]
        w = sum(self.columns_width[col_start:col_end + 1])
        h = sum(self.row_height[row_start:row_end + 1])
        return x, y, w, h

    @staticmethod
    def get_str(x):
        if isinstance(x, str):
            return [x]
        if isinstance(x, list):
            return x
        try:
            return [str(round(x))]
        except Exception as e:
            return ["-"]

    def run(self, image_time=None):
        if image_time is None:
            image_time = arrow.now().shift(minutes=-30)
        if self.is_jn:
            self.output = self.output_dir.joinpath(f"JN_{image_time.format('YYYY-MM-DDTHH')}.png")
        else:
            self.output = self.output_dir.joinpath(f"{image_time.format('YYYY-MM-DDTHH')}.png")

        logger.info("创建画布")
        width = sum(self.columns_width)
        height = sum(self.row_height)
        self.ax = Image.new(mode="RGB", size=(width + 20, height + 20), color="white")
        logo = Image.open(self.root.joinpath("static").joinpath("logo.v1.png"))
        logo = logo.resize((width, int(logo.height*width/logo.width)))
        self.ax.paste(logo)
        self.drawer = ImageDraw.Draw(self.ax)

        logger.info("画大矩形")
        x, y, w, h = *self.start, width, height
        self.drawer.rectangle(xy=((x, y), (x + w, y + h)), fill=None, outline="black", width=12)

        self.draw_rec_text((0, 0, 2, 0), "序号")
        self.draw_rec_text((0, 1, 2, 1), "点位")
        # datetime = arrow.get(self.datetime_tag_path.read_text(), "YYYY-MM-DDTHH")
        self.draw_rec_text((0, 2, 0, 9), image_time.format("YYYY-MM-DD HH:00"))

        self.draw_rec_text((1, 2, 1, 3), ["PM2.5", "（微克/立方米）"])
        self.draw_rec_text((2, 2), "实时")
        self.draw_rec_text((2, 3), "当日累计")

        self.draw_rec_text((1, 4, 1, 5), ["PM10", "（微克/立方米）"])
        self.draw_rec_text((2, 4), "实时")
        self.draw_rec_text((2, 5), "当日累计")

        self.draw_rec_text((1, 6, 1, 7), ["NO2", "（微克/立方米）"])
        self.draw_rec_text((2, 6), "实时")
        self.draw_rec_text((2, 7), "当日累计")

        self.draw_rec_text((1, 8, 1, 9), ["O3", "（微克/立方米）"])
        self.draw_rec_text((2, 8), "实时")
        self.draw_rec_text((2, 9), "MDA8")
        # self.ax = self.ax.resize((2000, int(self.ax.size[1] / self.ax.size[0] * 2000)), Image.ANTIALIAS)
        # self.ax = self.ax.quantize(colors=128, method=2)

        # 开始画数据
        for station_index, station_name, in enumerate(STATIONS_CNEMC.keys()):
            if station_name == "彩虹桥" and self.is_jn:
                self.draw_rec_text((station_index + 3, 0), station_index + 1,font=True,rfill="#ffff00")
                self.draw_rec_text((station_index + 3, 1), station_name,rfill="#ffff00")
            else:
                self.draw_rec_text((station_index + 3, 0), station_index + 1)
                self.draw_rec_text((station_index + 3, 1), station_name)
        if self.is_jn:
            self.draw_rec_text((16, 0, 16, 1), "全市",rfill="#ffff00")  
        else:
            self.draw_rec_text((16, 0, 16, 1), "全市")  

        self.draw_rec_text((17, 0, 17, 1), "无锡")
        self.draw_rec_text((18, 0, 18, 1), "苏州")

        for species_index, species in enumerate(["PM25_RT", "PM25_DAY", "PM10_RT", "PM10_DAY",
                                                 "NO2_RT", "NO2_DAY", "O3_RT", "MDA8"]):
            # 画国控点
            df_species = self.df.loc[STATIONS_CNEMC.keys(), species].to_frame(name="VALUE")
            df_species["COLOR"] = "black"
            df_species["FONT"] = None
            df_species.loc["彩虹桥","FONT"]=True

            df_species["RFILL"] = None
            df_species.loc["彩虹桥","RFILL"]="#ffff00"

            df_species.loc[df_species["VALUE"].isin(df_species["VALUE"].nlargest(3)), "COLOR"] = "red"
            df_species = df_species.reset_index()
            for index, row_data in df_species.iterrows():
                # print(index, species_index)
                logger.info(df_species)
                if self.is_jn and  row_data.FONT:
                    self.draw_rec_text((index + 3, species_index + 2),
                     row_data.VALUE,
                     fill=row_data.COLOR,
                     font=True,
                     rfill=row_data.RFILL
                     )
                else:
                    self.draw_rec_text((index + 3, species_index + 2), row_data.VALUE, fill=row_data.COLOR)


            # 画城市
            df_species = self.df.loc[["南京", "无锡","苏州"], species].to_frame(name="VALUE")
            df_species["COLOR"] = "black"
            df_species["FONT"] = None
            df_species.loc["南京","FONT"]=True
            df_species["RFILL"] = None
            df_species.loc["南京","RFILL"]="#ffff00"

            df_species.loc[df_species["VALUE"] < df_species.at["南京", "VALUE"], "COLOR"] = "green"
            df_species = df_species.reset_index()
            for index, row_data in df_species.iterrows():
                print(index, species_index)
                if self.is_jn and row_data.FONT:
                    self.draw_rec_text((index + 16, species_index + 2), 
                    row_data.VALUE,
                     fill=row_data.COLOR,
                     font=True,
                     rfill=row_data.RFILL
                     )
                else:
                    self.draw_rec_text((index + 16, species_index + 2), row_data.VALUE, fill=row_data.COLOR)

        logger.info("绘制完整正在保存=>{}", self.output)
        self.ax = self.ax.resize((2000, int(self.ax.size[1] / self.ax.size[0] * 2000)), Image.ANTIALIAS)
        self.ax.save(self.output)
        return self.output


class ExcelGenerator(GeneratorBase):
    def __init__(self, df):
        super(ExcelGenerator, self).__init__(df)
        self.output = self.output_dir.joinpath(f"{self.time_h.format('YYYY-MM-DDTHH')}.xlsx")
        self.wb = load_workbook(self.root.joinpath("static").joinpath("template8.xlsx"))

    @staticmethod
    def _write_row(ws, row_no, data_list):
        # print(data_list)
        for col, i in enumerate(data_list):
            # print(dict(column=col, row=row_no, value=i))
            _ = ws.cell(column=col + 1, row=row_no, value=i)

    def run(self, image_time=None):
        if image_time is None:
            image_time = arrow.now().shift(minutes=-30)
        ws = self.wb["DATA"]
        station_list = self.df.NAME.unique()
        for station_index,station_name in enumerate(station_list):
            station_df = self.df.loc[self.df.NAME==station_name]
            station_df = station_df.set_index("DATETIME")
            station_df = station_df.reindex(pd.date_range(start=station_df.index.min(), freq="H", periods=24, name="DATETIME"))
            station_df["NAME"] = station_df.NAME.fillna(station_name)
            station_df = station_df.reset_index()
            for index, d in station_df.iterrows():
                data_list = [d["NAME"], d.DATETIME.strftime("%Y-%m-%dT%H:%M"), d["PM2_5"], d["PM10"], d["NO2"], d["O3"]]
                print(data_list)
                self._write_row(ws, station_index*24+ index + 2, data_list=data_list)

        ws = self.wb["IMAGE"]
        ws.cell(column=3, row=1, value=image_time.format("YYYY-MM-DDTHH:00"))
        self.wb.save(self.output)
        return self.output


if __name__ == "__main__":
    from crawler import Cnemc

    c = Cnemc()

    g = ExcelGenerator(df=c.run())
    g.run()
    # print(g.root)
    # d = DrawImage()
    # d.run()
    g = ImageGenerator(df=c.run())
    g.run()
