import numpy as np


class AQI:
    def __init__(self):
        self._AQI = np.array([0, 50, 100, 150, 200, 300, 400, 500])

        self._CONC = {
            "SO2_24H": np.array([0, 50, 150, 475, 800, 1600, 2100, 2620]),
            "SO2": np.array([0, 150, 500, 650, 800, 800, 800, 800]),
            "NO2_24H": np.array([0, 40, 80, 180, 280, 565, 750, 940]),
            "NO2": np.array([0, 100, 200, 700, 1200, 2340, 3090, 3840]),
            "CO_24H": np.array([0, 2, 4, 14, 24, 36, 48, 60]),
            "CO": np.array([0, 5, 10, 35, 60, 90, 120, 150]),
            "O3": np.array([0, 160, 200, 300, 400, 800, 1000, 1200]),
            "O3_8H": np.array([0, 100, 160, 215, 265, 800, 800, 800]),
            "PM25_24H": np.array([0, 35, 75, 115, 150, 250, 350, 500]),
            "PM10_24H": np.array([0, 50, 150, 250, 350, 420, 500, 600]),
        }

    def conc2iaqi(self, species, conc):
        res = np.interp(conc, self._CONC[species], self._AQI)
        return np.ceil(res).astype(int)

    def iaqi2iconc(self, species, iaqi):
        res = np.interp(iaqi, self._AQI, self._CONC[species])
        return np.ceil(res).astype(int)

    def conc2aqi1h(self, pm25, pm10, so2, no2, o3, co):
        iaqi = np.array([
            self.conc2iaqi("PM25_24H", pm25),
            self.conc2iaqi("PM10_24H", pm10),
            self.conc2iaqi("SO2", so2),
            self.conc2iaqi("NO2", no2),
            self.conc2iaqi("O3", o3),
            self.conc2iaqi("CO", co),
        ])
        conc = np.array([pm25, pm10, so2, no2, o3, co])
        aqi = np.array(iaqi).max(axis=0)

        species = np.array(["PM25", "PM10", "SO2", "NO2", "O3", "CO"])
        primary = species[np.array(iaqi).argmax(axis=0)]
        primary_conc = conc[np.array(iaqi).argmax(axis=0),range(conc.shape[1])]
        primary = np.where(aqi > 50, primary, None)
        primary_conc = np.where(aqi > 50, primary_conc, None)
        # print(np.array(iaqi).argmax(axis=0).shape)
        if 0 <= aqi <= 50:
            rank = "优"
        elif  50 < aqi <= 100:
            rank = "良"
        elif  100 < aqi <= 150:
            rank = "轻度污染"
        elif  150 < aqi <= 200:
            rank = "中度污染"
        elif  100 < aqi <= 300:
            rank = "重度污染"
        else:
            rank = "严重污染"
        res = {
            "aqi":aqi,
            "rank":rank,
            "primary":primary,
            "primary_conc":primary_conc
        }
        return res

    def conc2aqi24h(self, pm25, pm10, so2, no2, o3, co):
        iaqi = np.array([
            self.conc2iaqi("PM25_24H", pm25),
            self.conc2iaqi("PM10_24H", pm10),
            self.conc2iaqi("SO2_24H", so2),
            self.conc2iaqi("NO2_24H", no2),
            self.conc2iaqi("O3_8H", o3),
            self.conc2iaqi("CO_24H", co),
        ])

        conc = np.array([pm25, pm10, so2, no2, o3, co])
        aqi = np.array(iaqi).max(axis=0)

        species = np.array(["PM25", "PM10", "SO2", "NO2", "O3", "CO"])
        primary = species[np.array(iaqi).argmax(axis=0)].data
        primary_conc = conc[np.array(iaqi).argmax(axis=0)]
        primary = np.where(aqi > 50, primary, None)
        primary_conc = np.where(aqi > 50, primary_conc, None)
        if 0 <= aqi <= 50:
            rank = "优"
        elif  50 < aqi <= 100:
            rank = "良"
        elif  100 < aqi <= 150:
            rank = "轻度污染"
        elif  150 < aqi <= 200:
            rank = "中度污染"
        elif  100 < aqi <= 300:
            rank = "重度污染"
        else:
            rank = "严重污染"
        res = {
            "aqi":aqi,
            "rank":rank,
            "primary":primary,
            "primary_conc":primary_conc
        }

        return res


if __name__ == "__main__":
    res = AQI().conc2aqi24h(76,1,1,1,11,1)
    for i in res:
        print(i)