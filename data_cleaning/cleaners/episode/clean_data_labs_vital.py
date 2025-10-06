import pandas as pd
from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner


class LabVitalsCleaner(EpisodeCleaner):
    def __init__(self):
        super().__init__()

    def clean_measurement(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        df = df.copy()

        # Only keep rows that contains atleast one digit
        condition = df[col_name].str.contains(r"\d", na=False)
        df = df[condition]

        new_col = col_name + "_cleaned"
        # remove all characters that are not digits, comma or dot.
        df[new_col] = df[col_name].str.replace(r"[^\d,\.]", "", regex=True)

        # if value starts or ends with a comma, then remove it.
        df[new_col] = df[new_col].str.replace(r"^,+|,+$", "", regex=True)

        # fix repetitive numbers
        # df[new_col] = df[new_col].str.replace(r'^(\d+)\1', r'\1', regex=True)

        # replace ',' with '.'
        # df['vital_result_cleaned'] = df['vital_result_cleaned'].str.replace(r'(\d+),(\d+)', r'\1.\2', regex=True)
        df[new_col] = df[new_col].str.replace(r",", r".", regex=True)

        # convert to numeric
        df[new_col] = pd.to_numeric(df[new_col], errors="coerce")

        return df

    def calculate_reasonability_vitals(
        self, df: pd.DataFrame, result_column: str, vital_name: str, ranges: dict = {}
    ) -> pd.DataFrame:
        df = df.copy()

        df["reasonable"] = None

        for vital, (low, high) in ranges.items():
            mask = df[vital_name] == vital
            df.loc[
                mask & (df[result_column].between(low, high, inclusive="both")),
                "reasonable",
            ] = True
            df.loc[
                mask & ~df[result_column].between(low, high, inclusive="both"),
                "reasonable",
            ] = False

        df["reasonable"] = df["reasonable"].fillna(True)

        return df

    def calculate_reasonability_lab(
        self, df: pd.DataFrame, result_column: str, lab_name: str, ranges: dict = {}
    ) -> pd.DataFrame:

        df["reasonable"] = None

        for lab, (low, high) in ranges.items():
            mask = df[lab_name] == lab
            df.loc[
                mask & (df[result_column].between(low, high, inclusive="both")),
                "reasonable",
            ] = True
            df.loc[
                mask & ~df[result_column].between(low, high, inclusive="both"),
                "reasonable",
            ] = False

        df["reasonable"] = df["reasonable"].fillna(True)

        return df
