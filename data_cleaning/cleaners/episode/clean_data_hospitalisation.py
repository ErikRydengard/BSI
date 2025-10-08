import pandas as pd
from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner


class HospitalisationCleaner(EpisodeCleaner):
    def __init__(self):
        super().__init__()

    def clean_hosp_data(
        self,
        df: pd.DataFrame,
        hosp_stop_col: str = "hosp_stop",
        hosp_type_col: str = "hosp_type",
        remove_hosp_type: bool = False,
        hosp_type_value: str = "Öppenvård",
    ) -> pd.DataFrame:

        df = df.dropna(subset=[hosp_stop_col])

        if remove_hosp_type:
            df = df[
                ~df[hosp_type_col].str.contains(hosp_type_value, case=False, na=False)
            ]

        return df

    def calculate_duration(self, intervals: list) -> float:
        """
        Function used to calculate the duration of a list of intervals. The function merges intervals so that overlapping intervals are not counted multiple times.

        Parameters
        ----------
        intervals : list
            List of intervals to calculate the duration of. The elements of the list should be of type pd.Interval.

        Returns
        -------
        float
            The total duration of the intervals in days.
        """

        intervals = sorted(intervals, key=lambda x: x.left)

        merged_intervals = [intervals[0]]
        for interval in intervals[1:]:
            last = merged_intervals[-1]
            if interval.left > last.right:
                merged_intervals.append(interval)
            else:
                merged_intervals[-1] = pd.Interval(
                    left=last.left, right=max(last.right, interval.right), closed="both"
                )

        total_seconds = sum(
            (interval.right - interval.left).total_seconds() + 1
            for interval in merged_intervals
        )
        return total_seconds / (24 * 60 * 60)

    def calculate_hosp_duration_past(
        self,
        df: pd.DataFrame,
        episode_id_col_name: str = "episode_id",
        patient_id_col_name: str = "patient_id",
        date_col_name: str = "sample_date",
        hosp_start_col_name: str = "hosp_start",
        hosp_stop_col_name: str = "hosp_stop",
        output_col_name: str = "",
        time_before: pd.Timedelta = pd.Timedelta(0),
    ) -> pd.DataFrame:
        """
        Function used to calculate the duration of hospitalisation in the past

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing the data to be processed.
        episode_id_col_name : str, optional
            Column name of the episode ID column, by default "episode_id".
        patient_id_col_name : str, optional
            Column name of the patient ID column, by default "patient_id".
        date_col_name : str, optional
            Column name of the date column, by default "sample_date".
        hosp_start_col_name : str, optional
            Column name of the hospitalisation start date column, by default "hosp_start".
        hosp_stop_col_name : str, optional
            Column name of the hospitalisation stop date column, by default "hosp_stop".
        output_col_name : str, optional
            Column name of the output column, by default "".
        time_before : pd.Timedelta, optional
            Time before the baseline date to include in the interval, by default pd.Timedelta(0).

        Returns
        -------
        pd.DataFrame
            DataFrame containing the episode ID, date, and the calculated duration of hospital
        """

        df = df.copy()
        result = []

        group_columns = [episode_id_col_name, patient_id_col_name, date_col_name]
        for group_keys, group in df.groupby(group_columns):
            episode_id, patient_id, baseline = group_keys

            condition = (group[hosp_start_col_name] < baseline) | (
                group[hosp_stop_col_name] > baseline - time_before
            )

            filtered_df = group[condition].copy()

            filtered_df["constrained_start"] = filtered_df[hosp_start_col_name].clip(
                lower=baseline - time_before
            )
            filtered_df["constrained_stop"] = filtered_df[hosp_stop_col_name].clip(
                upper=baseline
            )

            filtered_df = filtered_df[
                filtered_df["constrained_start"] < filtered_df["constrained_stop"]
            ]

            if filtered_df.empty:
                result.append(
                    {
                        episode_id_col_name: episode_id,
                        date_col_name: baseline,
                        output_col_name: 0,
                    }
                )
                continue

            intervals = [
                pd.Interval(left, right, closed="both")
                for left, right in zip(
                    filtered_df["constrained_start"], filtered_df["constrained_stop"]
                )
            ]

            total_days = self.calculate_duration(intervals)

            result.append(
                {
                    episode_id_col_name: episode_id,
                    date_col_name: baseline,
                    output_col_name: total_days,
                }
            )

        return pd.DataFrame(result)

    def calculate_hosp_duration_future(
        self,
        df: pd.DataFrame,
        episode_id_col_name: str = "episode_id",
        patient_id_col_name: str = "patient_id",
        date_col_name: str = "sample_date",
        hosp_start_col_name: str = "hosp_start",
        hosp_stop_col_name: str = "hosp_stop",
        output_col_name: str = "",
        time_before: pd.Timedelta = pd.Timedelta(0),
    ):
        print("Not implemented")

        pass

    def get_most_recent_hospitalisation_data(
        self,
        df,
        hosp_start_col_name: str = "hosp_start",
        hosp_stop_col_name: str = "hosp_stop",
        episode_id_col_name: str = "episode_id",
        hosp_site_col_name: str = "hosp_site",
        exclude_hosp_site: list = [],
        include_only_hosp_site: list = [],
    ):
        df = df.copy()
        df_original = df.copy()

        if len(exclude_hosp_site) != 0:
            condition = ~df[hosp_site_col_name].str.contains(
                "|".join(exclude_hosp_site), case=False, na=False
            )
            df = df[condition]

        if len(include_only_hosp_site) != 0:
            condition = df[hosp_site_col_name].str.contains(
                "|".join(include_only_hosp_site), case=False, na=False
            )
            df = df[condition]

        df["earliest_hosp_start"] = df.groupby(episode_id_col_name)[
            hosp_start_col_name
        ].transform("min")
        df["latest_hosp_stop"] = df.groupby(episode_id_col_name)[
            hosp_stop_col_name
        ].transform("max")

        df = df[df[hosp_stop_col_name] == df["latest_hosp_stop"]]
        df = df.merge(
            df_original[[episode_id_col_name]].drop_duplicates(),
            on=episode_id_col_name,
            how="right",
        )

        return df[
            [
                episode_id_col_name,
                hosp_site_col_name,
                hosp_start_col_name,
                hosp_stop_col_name,
                "earliest_hosp_start",
                "latest_hosp_stop",
            ]
        ]
    
