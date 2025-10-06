from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner
import pandas as pd


class OrbitCleaner(EpisodeCleaner):
    def __init__(self):
        super().__init__()

    def remove_mild_procedures(
        self, df: pd.DataFrame, code_col_name: str
    ) -> pd.DataFrame:
        """
        Remove minor procedures from the DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing a column with procedure codes.
        code_col_name : str
            Column name of the column containing procedure codes.

        Returns
        -------
        pd.DataFrame
            DataFrame containing only major procedures.
        """
        df = df.copy()
        major_procedures = df[code_col_name].str[0].between("A", "N")
        df = df[major_procedures]
        return df
