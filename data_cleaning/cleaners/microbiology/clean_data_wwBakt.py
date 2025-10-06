import pandas as pd

from data_cleaning.cleaners.microbiology.microbiologyCleaner import MicrobiologyCleaner
from data_cleaning.sir import find_sir_mic_variables_df, separate_sir_mic_data
from data_cleaning.transformations import (
    remove_redundant_decimals,
    reshape_to_long_format,
)


class WWBaktCleaner(MicrobiologyCleaner):
    def __init__(self):
        super().__init__()

    def convert_wwBakt_to_lims(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function for dividing each row into two rows, one for each bottle.
        This is done as LIMS uses one row for each bottle.
        Only rows related to blood cultures are kept. \n

        Parameters:\n
        df: DataFrame that contains the wwBakt data

        Returns: \n
        df: DataFrame in LIMS format

        TODO: bottle_nr should not be set for other types of samples than blood cultures
        """
        df = df.copy()

        # find columns related to bottle 2.
        # Uses the fact that there are a digit in  column names for bottle 2
        bottle_2_cols = [
            col
            for col in df.columns
            if "ttd" in col.lower() and any(c.isdigit() for c in col.lower())
        ]
        # find columns related to bottle 1 by removing the bottle 2 columns from the list of all TTD columns
        bottle_1_cols = list(
            set([col for col in df.columns if "ttd" in col.lower()])
            - set(bottle_2_cols)
        )

        # separate the data
        bottle_1_data = df.drop(columns=bottle_2_cols)
        bottle_2_data = df.drop(columns=bottle_1_cols)

        # rename the columns to match the bottle 1 columns
        bottle_2_data.columns = bottle_1_data.columns

        # add a column to indicate which bottle the data comes from.
        # This is only applicable for blood culture data
        bottle_1_data["bottle_nr"] = "Flaska 1"
        bottle_2_data["bottle_nr"] = "Flaska 2"

        # combine the data
        df = pd.concat([bottle_1_data, bottle_2_data])


        # If result is missing then set positive
        #df["TTD Result"] = df["TTD Result"].fillna("Positive")

        return df

    def add_labnr(
        self,
        df: pd.DataFrame,
        year_col: str = "year",
        section_code: str = "section_code",
        section_number: str = "section_number",
    ) -> pd.DataFrame:
        """
        Computes and adds the labnr to the DataFrame. \n
        The labnr value is not present in the wwBakt data so have to be computed. \n
        SID value contains three parts: first two digits of the year, the section code and the section number. \n

        Parameters: \n
        df: DataFrame \n
        year_col: column where the year data can be found \n
        section_code: column where the section code can be found \n
        section_number: column where the section number can be found \n
        """
        df = df.copy(deep=True)
        # add the last two digits of the year to the DataFrame
        df["labnr"] = df[year_col].map(int).map(str).str[2:4]

        # add the section to the DataFrame
        df["labnr"] = df["labnr"] + df[section_code]

        # add the section number to the DataFrame
        # TODO: For now, the section number is assumed to be an float. This will later be changed to be an integer.
        df["labnr"] = df["labnr"] + df[section_number].map(int).map(str)

        return df

    def convert_hours_to_datetime(self, hours):
        return pd.Timedelta(hours, unit="h")

    def clean_wwBakt_data(self, df: pd.DataFrame) -> tuple:
        df = df.copy()

        df = remove_redundant_decimals(df)

        # seperate the sir data from the rest of the data
        wwbakt_data, sir_data = separate_sir_mic_data(
            df,
            id_variables=[
                "Mikrobiologi_Prov_Alias",
                "RS_PAT_Alias",
                "Prdate",
                "species",
            ],
        )


        # fill in SIR data
        sir_data = self.fill_sir_data(
            df=sir_data,
            sir_cols=find_sir_mic_variables_df(sir_data),
            groupby_cols=["RS_PAT_Alias", "Prdate", "species"],
        )


        sir_data_long_format = reshape_to_long_format(
            sir_data,
            id_vars=["Mikrobiologi_Prov_Alias", "RS_PAT_Alias", "Prdate", "species"],
            value_vars=find_sir_mic_variables_df(sir_data),
            var_name="Type of antibiotics",
            value_name="SIR",
        )

        # add the SID to the data
        wwbakt_data = self.add_labnr(
            wwbakt_data, year_col="År", section_code="Avd", section_number="Avdnr"
        )


        # convert the data to LIMS format
        wwbakt_data_lims = self.convert_wwBakt_to_lims(wwbakt_data)


        # add indicator that the data is from wwBakt
        wwbakt_data_lims["data_source"] = "wwBakt"

        # The TTD column is in hours, convert to datetime
        wwbakt_data_lims["TTD"] = wwbakt_data_lims["TTD"].apply(
            self.convert_hours_to_datetime
        )

        return wwbakt_data_lims, sir_data_long_format
