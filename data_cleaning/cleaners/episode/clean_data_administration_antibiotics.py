import pandas as pd
from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner


class Administraion_antibiotics(EpisodeCleaner):
    def __init__(self):
        super().__init__()

    def clean_antibiotics_name(
        self, df: pd.DataFrame, antibiotics_col_name: str
    ) -> pd.DataFrame:
        """
        Function for cleaning antibiotic names. This is done by removing all characters after the first step.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe containing a column with antibiotic names.
        antibiotics_col_name : str
            Column name of the column containing antibiotic names.

        Returns
        -------
        pd.DataFrame
            DataFrame containing cleaned antibiotic names.
        """
        df = df.copy()
        df[antibiotics_col_name] = df[antibiotics_col_name].str.extract(
            r"^([a-zåäöA-ZÅÄÖ/]+)", expand=False
        )

        return df[antibiotics_col_name]

    def deduplicate_based_test_type(
        self,
        df: pd.DataFrame,
        priority: list,
        test_type_col_name: str,
        sir_value_col_name: str,
    ) -> pd.DataFrame:
        """
        Function for deduplicating SIR testing. The data is deduplicating based on the priority of the test type.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame containing the data to be deduplicated.
        priority : list
            A list of test types sorted by priority. ['Sir','Mic'] mean that Sir have priority over Mic.
        test_type_col_name : str
            The name of the column containing the test type.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the data deduplicated based on the priority of the test type.
        """

        df = df.copy()

        # Set priority
        priority_map = {key: value for value, key in enumerate(priority)}
        df["priority"] = df[test_type_col_name].map(priority_map)

        # Deduplicate
        subset_cols = [
            col
            for col in df.columns
            if col not in [test_type_col_name, sir_value_col_name, "priority"]
        ]
        print(subset_cols)
        df = df.sort_values(by=["priority"]).drop_duplicates(
            subset=subset_cols, keep="first"
        )

        # Drop priority column and return
        return df.drop(columns=["priority"])

    def split_antibiotic_name(
        self, df: pd.DataFrame, antibiotic_testing_col_name: str
    ) -> pd.DataFrame:
        
        df = df.copy()
        df[["resistance_determination_type", "resistance_determination_antibiotic"]] = (
            df[antibiotic_testing_col_name].str.split(" ", n=1, expand=True)
        )
        df = df.drop(columns=[antibiotic_testing_col_name])
        return df

    def add_adequate_antibiotic_usage(
        self,
        df,
        sir_data,
        merge_on_columns: list,
        antibiotics_admin_col: str,
        resistance_antibiotic_col: str,
        episode_id_col: str,
        sir_col: str,
        adequate_values: list = ["S", "I"],
        output_col: str = "adequate_antibiotic_usage",
    ):
        df = df.copy()
        sir_data = sir_data.copy()

        combined = pd.merge(df, sir_data, how="right", on=merge_on_columns)
        combined = combined[
            combined[antibiotics_admin_col] == combined[resistance_antibiotic_col]
        ]

        def determine_adequacy(sir_values):
            return 1 if any(val in adequate_values for val in sir_values) else 0

        combined[output_col] = combined.groupby(episode_id_col)[sir_col].transform(
            determine_adequacy
        )

        return combined[[episode_id_col, output_col]].drop_duplicates()
