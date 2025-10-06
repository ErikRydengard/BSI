from data_cleaning.cleaners.baseCleaner import BaseCleaner
import pandas as pd


class EpisodeCleaner(BaseCleaner):
    def __init__(self):
        pass

    def deduplication_based_on_time_diff(
        self, df: pd.DataFrame, diff_col_name: str, groupby_cols: list
    ) -> pd.DataFrame:
        """
        Deduplicate a DataFrame based on the time difference between measurements and baseline.
        If there are multiple measurments that are of the same type then only keep the measurment that is recorded most recently.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame containing a column with the time difference.
        diff_col_name : str
            Column name of the column containing the time difference.
        groupby_cols : list
            List of columns to group by.

        Returns
        -------
        pd.DataFrame
            A deduplicated DataFrame based on the diff times.
        """
        df = df.copy()
        df_grouped = df.dropna(subset=[diff_col_name]).loc[
            df.groupby(groupby_cols)[diff_col_name].idxmax()
        ]

        return pd.concat([df_grouped, df[df[diff_col_name].isna()]], ignore_index=True)

    def map_data_to_hospitalisation(
        self,
        reference_df: pd.DataFrame,
        df: pd.DataFrame,
        date_col_name: str,
        patient_id_col_name: str = "patient_id",
        hosp_start_col_name: str = "hosp_start",
        hosp_end_col_name: str = "hosp_stop",
    ) -> pd.DataFrame:
        """
        Maps data to hospitalisations based on the start and end dates of the hospitalisation.

        Parameters
        ----------
        reference_df : pd.DataFrame
            A DataFrame containing columns related to `episode_id`, `patient_id`, `hosp_start`, and `hosp_stop` columns.
        df : pd.DataFrame
            A DataFrame containing the data to be mapped to the hospitalisation. Must contain a patient ID column.
        date_col_name : str
            The name of the column containing the date to be used for mapping.
        patient_id_col_name : str, optional
            The name of the column containing patient IDs. Defaults to "patient_id".
        hosp_start_col_name : str, optional
            The name of the column containing the start date of a hospitalisation. Defaults to "hosp_start".
        hosp_end_col_name : str, optional
            The name of the column containing the end date of a hospitalisation. Defaults to "hosp_stop".

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data mapped to the hospitalisation episodes.


        Example
        -------
        >>> mapped_data = map_data_to_hospitalisation(
                           reference_df=reference_data[
                                    ["episode_id", "patient_id", "hosp_start", "hosp_stop"]
                           ],
                            df=administration_with_patient_id,
                            date_col_name="administration_date")
        """

        df = df.copy()
        reference_df = reference_df.copy()

        df = pd.merge(reference_df, df.copy(), on=patient_id_col_name, how="left")

        df = df[df[hosp_start_col_name].notnull() & df[hosp_end_col_name].notnull()]

        df = df[
            (df[date_col_name] >= df[hosp_start_col_name])
            & (df[date_col_name] <= df[hosp_end_col_name])
        ]

        return pd.merge(reference_df, df, on=reference_df.columns.to_list(), how="left")

    def map_data_to_interval(
        self,
        reference_df: pd.DataFrame,
        df: pd.DataFrame,
        date_col_name: str,
        patient_id_col_name: str = "patient_id",
        baseline_col_name: str = "sample_date",
        time_before_baseline: pd.Timedelta = pd.Timedelta(0),
        time_after_baseline: pd.Timedelta = pd.Timedelta(0),
    ) -> pd.DataFrame:
        """
        Maps data to intervals around a baseline date.

        Parameters
        ----------
        reference_df : pd.DataFrame
            A DataFrame containing `episode_id`, `patient_id`, and `sample_date` columns.
        df : pd.DataFrame
            A DataFrame containing the data to be mapped to the intervals. Must contain a patient ID column.
        date_col_name : str
            The column name of the column containing the date to be used for mapping.
        patient_id_col_name : str, optional
            The column name of the column containing patient IDs. Defaults to "patient_id".
        baseline_col_name : str, optional
            The column name of the column containing the baseline date. Defaults to "sample_date".
        time_before_baseline : pd.Timedelta, optional
            The time before the baseline date to include in the interval. Defaults to 0.
        time_after_baseline : pd.Timedelta, optional
            The time after the baseline date to include in the interval. Defaults to 0.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data mapped to the intervals around the baseline
        """

        df = df.copy()
        reference_df = reference_df.copy()

        df = pd.merge(reference_df, df, on=patient_id_col_name, how="left")

        df = df[
            (df[date_col_name] >= df[baseline_col_name] - time_before_baseline)
            & (df[date_col_name] <= df[baseline_col_name] + time_after_baseline)
        ]
        df['diff'] = abs(df[date_col_name] - df[baseline_col_name])

        return pd.merge(reference_df, df, on=reference_df.columns.to_list(), how="left")

    def summarize_data_by_episode(
        self,
        df: pd.DataFrame,
        episode_id_col: str,
        summary_function: callable,
    ) -> pd.DataFrame:
        """
        Function that converts multiple rows of data into a single row of summary data for each episode.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing a column with episode ids.
        episode_id_col : str
            The column name of the episode id.
        summary_function : callable
            A function that takes a dataframe as input and returns a dictionary containing the summary of the episode.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the summary of each episode.
        """

        summery_df = df.copy()

        results = []

        for episode_id, group in summery_df.groupby(episode_id_col):
            result = summary_function(group)
            results.append(result)

        return pd.DataFrame(results)

