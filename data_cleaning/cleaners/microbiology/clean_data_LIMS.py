
import pandas as pd
from data_cleaning.cleaners.microbiology.microbiologyCleaner import MicrobiologyCleaner
from data_cleaning.sir import find_sir_mic_variables_df, separate_sir_mic_data
from data_cleaning.transformations import remove_redundant_decimals, reshape_to_long_format


class LIMSCleaner(MicrobiologyCleaner):
    def __init__(self):
        super().__init__()

    def convert_to_timedelta(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """
        Converts a column in a DataFrame from the string format "10d 18h 30m" to timedelta64[ns] format.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the column to convert.
        col_name : str
            Name of the column to convert.

        Returns
        -------
        pd.DataFrame
            DataFrame with the relevant column converted to timedelta64[ns] format.
        """
        df = df.copy()
        df[col_name] = df[col_name].str.replace('d', 'days').str.replace('h', 'hours').str.replace('m', 'minutes')
        df[col_name] = pd.to_timedelta(df[col_name], errors='coerce')

        return df


    def clean_LIMS_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = remove_redundant_decimals(df)
        df = self.convert_to_timedelta(df, 'TTD TTD')
        df, df_sir = separate_sir_mic_data(df, ['Mikrobiologi_Prov_Alias', 'RS_PAT_Alias', 'Provtagningsdatum', 'species'])
        df_sir = self.fill_sir_data(df = df_sir,sir_cols = find_sir_mic_variables_df(df_sir),groupby_cols=['RS_PAT_Alias','Provtagningsdatum','species'])
        df_sir = reshape_to_long_format(df_sir, ['Mikrobiologi_Prov_Alias', 'RS_PAT_Alias', 'Provtagningsdatum','species'],
                                         find_sir_mic_variables_df(df_sir), 'Type of antibiotics', 'SIR')
        # add indicator that the data is from wwBakt
        df["data_source"] = "LIMS"

        return df, df_sir
