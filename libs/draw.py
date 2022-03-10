from PIL import Image, ImageDraw, ImageFont
import numpy as np
import pandas as pd
import arrow
from pathlib import Path
from loguru import logger


class DrawImage:
    def __init__(self):
        self.root = Path(__file__).parent.parent.absolute()
        self.all_data_path = self.root / "rt" / Path("all_data.csv")
        self.datetime_tag_path = self.root / "rt" / Path("datetime.tag")
        self.image_path = self.root / "rt" / f"{self.datetime_tag_path.read_text()}.png"
        self.wuxi_data_path = self.root / "rt" / Path("wuxi_data.csv")
        self.suzhou_data_path = self.root / "rt" / Path("suzhou_data.csv")

        self.columns_width = np.array([8, 12, 10, 10, 10, 10, 10, 10, 10, 10]) * 60
        self.row_height = [180, 360, 180, *[180] * 16]
        self.fonesize = 140
        self.font = ImageFont.truetype(font=str(self.root / 'static/kjgong.ttf'), size=self.fonesize)
        self.linewidth = 6
        self.spacing = self.fonesize * 0.1
        self.start = (10, 10)

    def create_figure(self):
        logger.info("创建画布")
        width = sum(self.columns_width)
        height = sum(self.row_height)
        self.image = Image.new(mode="RGB", size=(width + 20, height + 20), color="white")
        self.draw = ImageDraw.Draw(self.image)
        x, y, w, h = *self.start, width, height
        logger.info("画大矩形")
        self.draw.rectangle(xy=((x, y), (x + w, y + h)),
                            fill=None, outline="black", width=12)

    def draw_rec_text(self, loc, text, fill="black"):
        x, y, w, h = self.get_location(*loc)
        self.draw.rectangle(xy=((x, y), (x + w, y + h)), fill=None,
                            outline="black", width=self.linewidth)
        if isinstance(text, str):
            text = [text]
        for i, t in enumerate(text):
            fsize = self.font.getsize(t)
            height_offset = (i - len(text) / 2 + 0.5) * (self.spacing + self.fonesize)
            self.draw.text(xy=(
                x + w / 2 - fsize[0] / 2, y + h / 2 - fsize[1] / 2 + height_offset), text=t, fill=fill, font=self.font)

    def get_location(self, row_start, col_start, row_end=None, col_end=None, ):
        if not (row_end or col_end):
            row_end = row_start
            col_end = col_start
        x = sum(self.columns_width[:col_start]) + self.start[0]
        y = sum(self.row_height[:row_start]) + self.start[1]
        w = sum(self.columns_width[col_start:col_end + 1])
        h = sum(self.row_height[row_start:row_end + 1])
        return x, y, w, h

    def draw_data(self):
        self.create_figure()
        logger.info("正在绘制表头")
        self.draw_rec_text((0, 0, 2, 0), "序号")
        self.draw_rec_text((0, 1, 2, 1), "点位")
        datetime = arrow.get(self.datetime_tag_path.read_text(), "YYYY-MM-DDTHH")
        self.draw_rec_text((0, 2, 0, 9), datetime.format("YYYY-MM-DD HH:mm"))

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
        self.draw_rec_text((2, 9), "O3_8H")

        df = pd.read_csv(self.all_data_path)

        for i, d in df.iterrows():
            self.draw_rec_text((i + 3, 0), str(i + 1))
            self.draw_rec_text((i + 3, 1), d["STATION_NAME"])
        self.draw_rec_text((16, 0, 16, 1), "全市")
        self.draw_rec_text((17, 0, 17, 1), "无锡")
        self.draw_rec_text((18, 0, 18, 1), "苏州")
        # 读取无锡数据
        wuxi_data = pd.read_csv(self.wuxi_data_path, index_col=0, parse_dates=True, na_values=["-", "—", ""])
        wuxi_data = wuxi_data.loc[:, ["PM2_5", "PM10", "NO2","O3"]]
        wuxi_p_series = pd.concat([wuxi_data.iloc[-1], wuxi_data.mean().astype(int)])
        wuxi_p_series.index = ["PM25", "PM10", "NO2", "O3", "PM25_CUM", "PM10_CUM", "NO2_CUM", "O3_CUM"]
        wuxi_p_series["O3_CUM"] = wuxi_data["O3"].rolling(8,8).mean().iloc[-1]
        # 读取苏州数据
        suzhou_data = pd.read_csv(self.suzhou_data_path, index_col=0, parse_dates=True, na_values=["-", "—", ""])
        suzhou_data = suzhou_data.loc[:, ["PM2_5", "PM10", "NO2","O3"]]
        suzhou_p_series = pd.concat([suzhou_data.iloc[-1], suzhou_data.mean().astype(int)])
        suzhou_p_series.index = ["PM25", "PM10", "NO2", "O3", "PM25_CUM", "PM10_CUM", "NO2_CUM", "O3_CUM"]
        suzhou_p_series["O3_CUM"] = suzhou_data["O3"].rolling(8,8).mean().iloc[-1]

        logger.info("正在绘制数据列")
        for species_index, species in enumerate(["PM25", "PM25_CUM", "PM10", "PM10_CUM", "NO2", "NO2_CUM", "O3", "O3_CUM"]):
            d = df[species]
            nl = d.isin(d.nlargest(3))
            nl.name = "NL"
            d.name = "VALUE"
            tmp_df = pd.concat([d, nl], axis=1)
            # 全市
            mean_str = "-" if np.isnan(d.mean()) else str(round(d.mean()))
            self.draw_rec_text((16, species_index + 2), mean_str)

            fill = "green" if mean_str != "-" and d.mean() > wuxi_p_series[species] else "black"
            self.draw_rec_text((17, species_index + 2), str(round(wuxi_p_series[species])), fill=fill)

            fill = "green" if mean_str != "-" and d.mean() > suzhou_p_series[species] else "black"
            self.draw_rec_text((18, species_index + 2), str(round(suzhou_p_series[species])), fill=fill)

            for index, species_value in tmp_df.iterrows():
                t = "-" if np.isnan(species_value.VALUE) else str(round(species_value.VALUE))
                fill = "red" if species_value.NL else "black"
                self.draw_rec_text((index + 3, species_index + 2), t, fill=fill)

    def save(self):
        logger.info("正在保存图片=>{}", self.image_path)
        self.image = self.image.resize((2000, int(self.image.size[1] / self.image.size[0] * 2000)), Image.ANTIALIAS)
        self.image = self.image.quantize(colors=16, method=2)
        self.image.save(self.image_path)

    def run(self):
        self.draw_data()
        self.save()


if __name__ == "__main__":
    d = DrawImage()
    d.run()
