import pandas as pd
import numpy as np
from data_cleaning.cleaners.baseCleaner import BaseCleaner
from data_cleaning.renaming import generate_rename_columns_json
from data_cleaning.utils import save_json


class MicrobiologyCleaner(BaseCleaner):
    def __init__(self):
        super().__init__()

    def determine_episode(
        self,
        df: pd.DataFrame,
        columns_to_sort_by: list,
        patient_id_col_name: str,
        sample_date_col_name: str,
        time: int = 30,
    ) -> pd.DataFrame:
        """
        Determines the episode number and ID for each sample.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame containing sample data.
        columns_to_sort_by : list
            List of columns to sort by (e.g., patient ID and sample date).
        patient_id_col_name : str
            Column name for patient ID.
        sample_date_col_name : str
            Column name for sample date.
        time : int, optional
            The maximum time difference in days to be considered part of the same episode, by default 30.

        Returns
        -------
        pd.DataFrame
            The DataFrame with new columns `episode_nr` and `episode_id`.

        Example
        -------
        >>> determine_episode(
                              self,
                              df = microbiology_data,
                              columns_to_sort_by=["patient_id", "date_of_sampling"],
                              patient_id_col_name="patient_id",
                              sample_date_col_name="date_of_sampling",
                              time=30)
        """

        # Create a copy of the DataFrame and convert sample_date_col_name to datetime
        df = df.copy()
        df[sample_date_col_name] = pd.to_datetime(
            df[sample_date_col_name], errors="coerce"
        )

        # Sort the DataFrame based on the specified variables
        df = df.sort_values(by=columns_to_sort_by).reset_index(drop=True)

        # Calculate the time difference (in days) between consecutive samples for each patient
        df["days_diff"] = (
            df.groupby(patient_id_col_name)[sample_date_col_name].diff().dt.days
        )

        # A new episode starts when:
        # 1. The patient ID changes (indicated by a large change in 'days_diff' or NaN in diff)
        # 2. The difference in days exceeds the specified time threshold
        new_episode = (
            df[patient_id_col_name] != df[patient_id_col_name].shift(1, fill_value=0)
        ) | (df["days_diff"] > time)

        # Use cumulative sum of the new episode indicator to assign episode numbers
        df["episode_nr"] = (
            new_episode.groupby(df[patient_id_col_name]).cumsum().astype(int)
        )

        # Generate the 'episode_id' by concatenating patient ID and episode number
        df["episode_id"] = df[patient_id_col_name].astype(str) + df[
            "episode_nr"
        ].astype(int).astype(str)

        return df

    def extract_blood_samples(
        self, df: pd.DataFrame, variable_name: str, keyword: str
    ) -> pd.DataFrame:
        """
        Separates blood samples from other types of samples. Returns a DataFrame with the separated blood samples.
        """

        extracted_df = df.copy()
        extracted_df = extracted_df[
            extracted_df[variable_name]
            .astype(str)
            .str.contains(keyword, case=False, na=False)
        ]
        return extracted_df

    def flag_contaminants(
        self, df: pd.DataFrame, variable_name: str, contaminants: list
    ) -> pd.DataFrame:
        """
        Flags samples that contain contaminants from the provided list. Adds a new a column of booleans.
        """
        df = df.copy()
        df["flag_contaminants"] = df[variable_name].isin(contaminants)
        return df

    def filter_TTP(
        self, df: pd.DataFrame, ttp_col_name: str, limit: pd.Timedelta
    ) -> pd.DataFrame:
        """
        Function for removing rows where the TTP is above `limit`. Rows where TTP is missing is kept.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing TTP values.
        ttp_col_name : str
            Column name for the TTP values.
        limit : pd.Timedelta
            The limit for the TTP values.

        Returns
        -------
        pd.DataFrame
            DataFrame with rows removed where TTP is above `limit`.
        """
        df = df.copy()
        df = df[(df[ttp_col_name].isna()) | (df[ttp_col_name] < limit)]
        return df

    def add_ttp(
        self,
        df: pd.DataFrame,
        result_col_name: str,
        incubation_date_col_name: str,
        ttd_col_name: str,
        result_date_col_name: str,
    ) -> pd.DataFrame:
        """
        Adds a column with calculated TTP values to a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame.
        result_col_name : str
            Column name for the results.
        incubation_date_col_name : str
            Column name for the incubation dates.
        ttd_col_name : str
            Column name for the TTD values.
        result_date_col_name : str
            Column name for the result dates.

        Returns
        -------
        pd.DataFrame
            DataFrame with TTP column added.
        """

        df = df.copy()

        # Case 1: If TTD exists and result is positive or missing, set TTP as TTD.
        condition1 = (
            ~df[result_col_name].str.contains("neg", na=False, case=False)
            & df[ttd_col_name].notna()
        )

        df["TTP"] = np.nan
        df["TTP"] = pd.to_timedelta(df["TTP"])

        # Apply Case 1: Set TTP to TTD for non negative results where TTD exists
        df.loc[condition1, "TTP"] = df.loc[condition1, ttd_col_name]

        # Case 2: Set TTP as the difference between 'result_date' and 'arrival_date'
        # if result is positive or missing, and TDD is missing.
        # condition2 = ~df[result_col_name].str.contains('neg', na=False, case=False) & df[ttd_col_name].isna()

        # Apply Case 2: Set TTP as the date difference for non negative results where TTD is missing
        # df.loc[condition2, 'TTP'] = (df[result_date_col_name] - df[incubation_date_col_name]).dt.days
        # df.loc[condition2, 'TTP'] = pd.to_timedelta(df[result_date_col_name] - df[incubation_date_col_name])
        df["TTP_hours"] = df["TTP"].dt.total_seconds() / 3600

        return df

    def fill_sir_data(self, df, sir_cols, groupby_cols):
        df = df.copy()

        df[sir_cols] = df.groupby(groupby_cols, dropna=False)[sir_cols].ffill()
        df[sir_cols] = df.groupby(groupby_cols, dropna=False)[sir_cols].bfill()
        return df

    def set_contaminant_relevant(
        self,
        df: pd.DataFrame,
        method: str = "bottle",
        patient_id_col: str = "patient_id",
        species_col: str = "species",
        sample_date_col: str = "sample_date",
        labnr_col: str = "sid",
        potential_contaminant_col: str = "potential_contaminant",
    ) -> pd.DataFrame:
        """
        Function for estimating if a potential contaminant is relevant or not. The function have three different methods:
        - `bottle`: If a potential contaminant is found in less than 3 bottles during a sample date, then it's not considered relevant.
        - `labnr`: If a potential contaminant is found in only one bottle set then it's considered not relevant.
        - `potential_contaminant`: Potential contaminant is considered not relevant.

        Parameters
        ----------
        df : pd.DataFrame
        A DataFrame with microbiological findings. It is important that there are only positive findings in the DataFrame.
        method : str, optional
        Method to use for estimating if a potential contaminant is relevant or not, by default "bottle"
        patient_id_col : str, optional
            The column name of the column containing unique identifiers for patients, by default "patient_id"
        species_col : str, optional
            The column name of the column containing a name for the finding, by default "species"
        sample_date_col : str, optional
            The column name of the column containing sample dates, by default "sample_date"
        labnr_col : str, optional
            The column name of the column containing identifiers for bottle sets, by default "sid"
        potential_contaminant_col : str, optional
            The column name of the column indicating if a finding is a potential contaminant or not, by default "potential_contaminant"

        Returns
        -------
        pd.DataFrame
        A DataFrame containing a new column `relevant` indicating whether the finding is relevant or not according to the method used.
        """

        df = df.copy()
        mask = df[potential_contaminant_col] == True

        df["relevant"] = None

        if method == "bottle":
            df.loc[mask, "relevant"] = ~(
                df.loc[mask]
                .groupby([patient_id_col, sample_date_col, species_col])[labnr_col]
                .transform("count")
                < 3
            )
        elif method == "labnr":
            df.loc[mask, "relevant"] = ~(
                df.loc[mask]
                .groupby([patient_id_col, sample_date_col, species_col])[labnr_col]
                .transform("nunique")
                == 1
            )
        elif method == "potential_contaminant":
            df.loc[mask, "relevant"] = False
        else:
            raise ValueError("Choose a method")

        df["relevant"] = df["relevant"].fillna(True)

        return df

    def set_mono_poly_contamination(
        self,
        df: pd.DataFrame,
        patient_id_col: str = "patient_id",
        sample_date_col: str = "sample_date",
        species_col: str = "species",
    ) -> pd.DataFrame:
        """
        Function for classifying if the findings on a sample date are mono, poly or contamination.
        - `mono`: If there is only one relevant finding on a sample date.
        - `poly`: If there are multiple relevant findings on a sample date.
        - `cont`: If finding is not relevant

        Relevant finding will always be classified as mono or poly.
        Non-relevant findings will always be classified as contamination.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame with microbiological findings. It is important that there are only positive findings in the DataFrame.
            The function `set_contaminant_relevant` should be run before this function so that the column `relevant` is present in the DataFrame.
        patient_id_col : str, optional
            The column name of the column containing unique identifiers for patients, by default "patient_id"
        sample_date_col : str, optional
            The column name of the column containing sample dates, by default "sample_date"
        species_col : str, optional
            The column name of the column containing a name for the finding, by default "species"
        relevant_col : str, optional
            The column name of the column indicating if a finding is relevant or not, by default "relevant"

        Returns
        -------
        pd.DataFrame
            A DataFrame containing a new column `mono_poly_contamination` indicating whether the finding is mono, poly or contamination.
        """

        df = df.copy()
        df["is_relevant"] = df["relevant"] == True
        df["is_non_relevant"] = df["relevant"] == False

        # Deduplication data so that the same finding is not counted multiple times
        dedup = df.drop_duplicates(
            subset=[patient_id_col, sample_date_col, species_col]
        )

        # Count relevant and non-relevant findings per patient and sample date
        counts = (
            dedup.groupby([patient_id_col, sample_date_col])[
                ["is_relevant", "is_non_relevant"]
            ]
            .sum()
            .rename(
                columns={
                    "is_relevant": "nr_relevant_findings",
                    "is_non_relevant": "nr_non_relevant_findings",
                }
            )
            .reset_index()
        )
        # Add counts to the dataframe
        df = df.merge(counts, on=[patient_id_col, sample_date_col], how="left")

        # function for classification of findings
        def classify_finding(row):
            if row["nr_relevant_findings"] == 0 and row["nr_non_relevant_findings"] > 0:
                return "cont"
            elif row["nr_relevant_findings"] > 1:
                return "poly"
            elif row["nr_relevant_findings"] == 1:
                return "mono"

            raise ValueError("This should not happen")

        # Create a helper column which stores the classification
        df["mono_poly_contamination"] = df.apply(classify_finding, axis=1)

        # If the finding is not relevant then set the classification to contamination
        df.loc[df["is_non_relevant"], "mono_poly_contamination"] = "cont"

        df = df.drop(columns=["is_relevant", "is_non_relevant"])
        return df

    def flag_polymicrobial(
        self,
        df: pd.DataFrame,
        patient_id_col: str = "patient_id",
        sample_date_col: str = "sample_date",
        species_col: str = "species",
        labnr_col: str = "sid",
    ) -> pd.DataFrame:
        """
        Function for flagging polymicrobial findings. The function will create two new columns:
        - `polymicrobial`: A boolean column indicating if the finding is polymicrobial or not.
        - `which_polymicrobial`: A column containing each of the findings.
        - `which_sample_ids`: A column containing the sample ids of the polymicrobial finding.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame with microbiological findings. Functions `set_mono_poly_contamination` and `set_contaminant_relevant` should be run before this function so that the columns `mono_poly_contamination` and `relevant` are present in the DataFrame.
        patient_id_col : str, optional
            The column name of the column containing unique identifiers for patients, by default "patient_id"
        sample_date_col : str, optional
            The column name of the column containing sample dates, by default "sample_date"
        species_col : str, optional
            The column name of the column containing a name for the finding, by default "species"
        labnr_col : str, optional
            The column name of the column containing identifiers for bottle sets, by default "sid"
        finding_type_col : str, optional
            The column name of the column indicating if a finding is mono, poly or contamination, by default "mono_poly_contamination"

        Returns
        -------
        pd.DataFrame
            A DataFrame containing two new columns `polymicrobial` and `which_polymicrobial` indicating whether the finding is polymicrobial or not and which findings are polymicrobial.
        """

        df = df.copy()
        df["polymicrobial"] = df["mono_poly_contamination"] == "poly"
        df["which_polymicrobial"] = None

        mask = df["polymicrobial"]

        df.loc[mask, "which_polymicrobial"] = (
            df[mask]
            .groupby([patient_id_col, sample_date_col])[species_col]
            .transform(
                lambda x: " | ".join(
                    sorted(
                        set(str(val).replace("&", "") for val in x if pd.notnull(val))
                    )
                )
            )
        )
        # My thought is that these sample ids can be used for accessing SIR data if needed
        # TODO: add parameter for sample_id column
        which_sample_ids_df = (
            df.groupby([patient_id_col, sample_date_col])
            .apply(
                lambda g: " | ".join(
                    sorted(
                        set(
                            f"{row['species']}:{row['sample_id']}"
                            for _, row in g.iterrows()
                            if pd.notnull(row["sample_id"]) and pd.notnull(row["species"])
                        )
                    )
                )
            )
            .reset_index(name="which_sample_ids")
        )

        # make sure all rows contain the column which_sample_ids
        df = df.merge(which_sample_ids_df, on=[patient_id_col, sample_date_col], how="left")


        df["polymicrobial"] = df["polymicrobial"].fillna(False)
        
        return df
    
    def flag_polymicrobial_whole_episode(
        self,
        df: pd.DataFrame,
        episode_id_col: str = "episode_id",
        sample_date_col: str = "sample_date",
        microorganism_id_col: str = "microorganism",
    ) -> pd.DataFrame:
        """
        Flags episodes that contain more than one microorganism within the same episode id.
        Adds a new a column of booleans.

        Parameters
        ----------
        df: pd.DataFrame
            Input DataFrame.
        episode_id_col : str = 'episode_id'
            Column name for the episode ID column.
        sample_date_col : str = 'sample_date'
            Column name for the sample date column.
        microorganism_id_col : str = 'microorganism'
            Column name for the microorganism column.

        Returns
        -------
        pd.DataFrame
            DataFrame with new column added.
        """
        df = df.copy()
        df["polymicrobial"] = df.groupby([episode_id_col])[
            microorganism_id_col
        ].transform(lambda x: x.nunique() > 1)
        return df
        

    def classify_microbiological_findings(
        self,
        df: pd.DataFrame,
        method: str = "bottle",
        outcome_col: str = "bottle_outcome",
        outcome_positive_prefix: str = "pos",
        patient_id_col: str = "patient_id",
        sample_date_col: str = "sample_date",
        species_col: str = "species",
        labnr_col: str = "sid",
        potential_contaminant_col: str = "potential_contaminant",
    ) -> pd.DataFrame:
        """
        Function for running all steps needed to classify microbiological and specify relevant findings.
        The function will run the following steps:
        1. Set contaminant relevant
        2. Set mono/poly contamination
        3. Flag polymicrobial findings
        4. Update the original DataFrame with the new columns



        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame with microbiological findings.
        method : str, optional
            Method to use for estimating if a potential contaminant is relevant or not.\n
            The following methods are available:
            - `bottle`: If a potential contaminant is found in less than 3 bottles during a sample date, then it's not considered relevant.
            - `labnr`: If a potential contaminant is found in only one bottle set then it's considered not relevant.
            - `potential_contaminant`: Potential contaminant is considered not relevant.\n
            Default method is `bottle`.
        outcome_col : str, optional
            The column name of the column containing the outcome of the microbiological finding, by default "bottle_outcome"
        outcome_positive_prefix : str, optional
            The prefix of the outcome indicating a positive finding, by default "pos"
        patient_id_col : str, optional
            The column name of the column containing unique identifiers for patients, by default "patient_id"
        sample_date_col : str, optional
            The column name of the column containing sample dates, by default "sample_date"
        species_col : str, optional
            The column name of the column containing a name for the finding, by default "species"
        labnr_col : str, optional
            The column name of the column containing identifiers for bottle sets, by default "sid"
        potential_contaminant_col : str, optional
            The column name of the column indicating if a finding is a potential contaminant or not, by default "potential_contaminant"

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the new columns `relevant`, `mono_poly_contamination`, `polymicrobial`, `which_polymicrobial` and `which_sample_ids`.
        """
        df = df.copy()

        # Mask for getting the rows with positive findings
        mask = (
            df[outcome_col]
            .str.lower()
            .str.startswith(outcome_positive_prefix, na=False)
        )

        # Subset of the data containing only positive findings
        subset = df.loc[mask].copy()

        subset = self.set_contaminant_relevant(
            subset,
            method=method,
            patient_id_col=patient_id_col,
            species_col=species_col,
            sample_date_col=sample_date_col,
            labnr_col=labnr_col,
            potential_contaminant_col=potential_contaminant_col,
        )

        subset = self.set_mono_poly_contamination(
            subset,
            patient_id_col=patient_id_col,
            sample_date_col=sample_date_col,
            species_col=species_col,
        )

        subset = self.flag_polymicrobial(
            subset,
            patient_id_col=patient_id_col,
            sample_date_col=sample_date_col,
            species_col=species_col,
            labnr_col=labnr_col,
        )

        # Update positive rows
        df.loc[mask, subset.columns] = subset[subset.columns].values

        return df
