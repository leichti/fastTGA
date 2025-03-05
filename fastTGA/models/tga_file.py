from datetime import datetime
import polars as pl
import re
from pathlib import Path


class TGAFile():
    def __init__(self, path_to_file):
        self.path = Path(path_to_file)
        self.metadata = {}
        self.data = None

        self.parse_file()

    @property
    def id(self):
        return self.metadata.get('name', None)

    def _parse_date(self, date_str):
        """Parse German date format to datetime object."""
        date_str = date_str.strip('# ')

        de_month_map = {
            'Jan': '01', 'Feb': '02', 'Mrz': '03', 'Apr': '04',
            'Mai': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Okt': '10', 'Nov': '11', 'Dez': '12'
        }

        try:
            pattern = r'(?:\w{2}\s)?(\w{3})\s(\d{1,2})\s(\d{2}):(\d{2}):(\d{2})\s(\d{4})'
            match = re.search(pattern, date_str)

            if match:
                month, day, hour, minute, second, year = match.groups()
                month = de_month_map[month]
                date_str = f"{year}-{month}-{day:>02} {hour}:{minute}:{second}"
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"Error parsing date: {date_str}, Error: {e}")
            return None

    def _parse_weight(self, weight_str):
        """Parse weight value, handling different formats."""
        weight_str = weight_str.strip('# ').replace('Weight:', '').replace('mg', '').strip()
        try:
            return float(weight_str)
        except ValueError:
            print(f"Error parsing weight: {weight_str}")
            return None

    def parse_file(self):
        row_offset = 0

        with open(self.path, 'r', encoding='cp1252') as file:
            for line in file:
                row_offset += 1

                if not line.startswith('#'):
                    break

                line = line.strip()
                if 'Export date and time:' in line:
                    self.metadata['export_date'] = self._parse_date(line.split(':', 1)[1])
                elif 'Measurement date and time:' in line:
                    self.metadata['measurement_date'] = self._parse_date(line.split(':', 1)[1])
                elif 'Name:' in line:
                    self.metadata['name'] = line.split(':', 1)[1].strip()
                elif 'Weight:' in line:
                    self.metadata['weight'] = self._parse_weight(line)

        # Read data with polars, using calculated offset
        self.data = pl.read_csv(self.path,
                                separator=',',
                                encoding='cp1252',
                                skip_rows=row_offset - 1,
                                infer_schema_length=30000)

        rename_dict = {
            "Time(s)": "t_s",
            "Temperature(°C)": "T_C",
            "Corrected delta m(mg)": "dm_mg",
            "Delta m(mg)": "dm_mg",
            "Gas 1(sccm/min)": "gas1_l_min",
            "Gas 2(sccm/min)": "gas2_l_min",
            "Purge(sccm/min)": "purge_l_min",
            "DTA_RAW(1)": "DTA1",
            "POWER(%)": "power_pct",
            "TEMP_CJR_FURNACE(K)": "T_cjr_furnace_K",
            "TEMP_CJR_SAMPLE(K)": "T_cjr_sample_K",
            "TEMP_FURNACE(K)": "T_furnace_K",
            "TEMP_NOM_FURNACE(K)": "T_nom_furnace_K",
            "TGA_RAW(mg)": "TGA_raw_mg",
            # Add more mappings as needed
        }
        self.data = self.data.rename(rename_dict, strict=False)

    def downsample(self, downsample_frequency, unit='s'):
        df = self.data
        df = self._convert_time_to_milliseconds(df)
        downsample_frequency = self._convert_frequency_to_milliseconds(downsample_frequency, unit)
        df = self._downsample_data(df, downsample_frequency)
        self.data = self._convert_time_back_to_seconds(df)

    def _convert_time_to_milliseconds(self, df):
        return df.with_columns(
            (pl.col("t_s") * 1000).cast(pl.Int64),
        )

    def _convert_frequency_to_milliseconds(self, frequency, unit):
        if unit == 's':
            return int(frequency * 1000)
        elif unit == 'm':
            return int(frequency * 1000 * 60)
        elif unit == 'h':
            return int(frequency * 1000 * 60 * 60)
        else:
            raise ValueError(f"Unsupported unit: {unit}")

    def _downsample_data(self, df, downsample_frequency):
        return df.group_by_dynamic("t_s", every=f"{downsample_frequency}i").agg(pl.all().mean())

    def _convert_time_back_to_seconds(self, df):
        return df.with_columns(
            (pl.col("t_s") / (1000 * 60)).cast(pl.Float64),
        )

    def calculate_dm_dt_in_s(self):
        # recalculate dmdt as mg per minute
        self.data = self.data.with_columns(
            (pl.col("dm_mg").diff() / pl.col("t_s").diff()).alias("dmdt_mg_s")
        )


if __name__ == "__main__":
    file = TGAFile(
        '/Users/manuelleuchtenmuller/Library/CloudStorage/OneDrive-HydrogenReductionLab/H2Lab Projects/H2Lab_D2V_24_9 Melting Behaviour/TGA/1745_RT12.txt')
    file.parse_file()

    print("Metadata:", file.metadata)
    print("\nData Schema:", file.data.schema)
    print("\nFirst few rows:", file.data.head())