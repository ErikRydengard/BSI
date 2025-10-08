import pandas as pd

from data_cleaning.renaming import generate_rename_columns_json
from data_cleaning.transformations import convert_to_datetime, remove_redundant_decimals
from data_cleaning.utils import save_json


class BaseCleaner:
    def __init__(self):
        pass

    def concat_data(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Function that concatenates two DataFrames.
        Parameters
        ----------
        df1 : pd.DataFrame
            Dataframe to concatenate.
        df2 : pd.DataFrame
            Another DataFrame to concatenate.

        Returns
        -------
        pd.DataFrame
            The concatenated DataFrame of df1 and df2.
        """
        return pd.concat([df1, df2], axis=0, ignore_index=True)

    def clean_data(self, df: pd.DataFrame, cols_to_drop: list = []) -> pd.DataFrame:
        """
        A function that cleans a DataFrame by dropping columns, removing duplicates, and converting columns to datetime.
        This function can be used as a inital step in cleaning a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to clean.
        cols_to_drop : optional, list
            Columns to drop from the DataFrame.

        Returns
        -------
        pd.DataFrame
            Cleaned DataFrame.
        """

        df = df.copy()

        df = df.drop(columns=cols_to_drop)
        df = df.drop_duplicates()
        df = remove_redundant_decimals(df)
        df = convert_to_datetime(df)

        return df

    def add_patient_id_col(
        self, df: pd.DataFrame, mapping: dict, hosp_id_col: str
    ) -> pd.DataFrame:
        """
        Add a patient_id column to the DataFrame based on a mapping dictionary.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe containing a column with hospital ids.
        mapping : dict
            Dictionary where the keys are hospital ids and the values are patient ids.
        hosp_id_col : str
            The column name of the column containing the hospital ids.

        Returns
        -------
        pd.DataFrame
            The same DataFrame with a new column containing the patient IDs.
        """
        df = df.copy()
        df["patient_id"] = df[hosp_id_col].astype(str).map(mapping)
        df = df.dropna(subset=["patient_id"])
        df["patient_id"] = df["patient_id"].astype(int)
        return df

    def get_exact_match_mask(self, df: pd.DataFrame, target_cols: list, values: list) -> pd.Series:
        """
        Returns a boolean mask indicating rows where any value in the specified columns matches exactly with any value in the provided list.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to check.
        target_cols : list
            List of column names to check for exact matches.
        values : list
            List of exact values to filter by.

        Returns
        -------
        pd.Series
            Boolean mask where True indicates a row with an exact match in any of the specified columns.
        """
        return df[target_cols].apply(lambda col: col.isin(values)).fillna(False).any(axis=1)

    def filter_rows_by_exact_value(self, df: pd.DataFrame, target_cols: list, values: list) -> pd.DataFrame:
        """
        Filters rows in the DataFrame where any value in the specified columns matches exactly with any value in the provided list.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to filter.
        target_cols : list
            List of column names to check for exact matches.
        values : list
            List of exact values to filter by.

        Returns
        -------
        pd.DataFrame
            DataFrame containing only rows where at least one specified column has an exact match with any of the given values.
        """
        row_mask = self.get_exact_match_mask(df, target_cols, values)
        return df[row_mask].copy()

    def get_prefix_match_mask(self, df: pd.DataFrame, target_cols: list, prefixes: list) -> pd.Series:
        """
        Returns a boolean mask indicating rows where any value in the specified columns starts with any of the provided prefixes.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to check.
        target_cols : list
            List of column names to check for prefix matches.
        prefixes : list
            List of prefixes to filter by.

        Returns
        -------
        pd.Series
            Boolean mask where True indicates a row with a value that starts with any of the specified prefixes.
        """
        prefixes_tuple = tuple(prefixes)
        return df[target_cols].apply(lambda col: col.str.startswith(prefixes_tuple)).fillna(False).any(axis=1)

    def filter_rows_by_value_prefix(self, df: pd.DataFrame, target_cols: list, prefixes: list) -> pd.DataFrame:
        """
        Filters rows in the DataFrame where any value in the specified columns starts with any of the provided prefixes.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to filter.
        target_cols : list
            List of column names to check for prefix matches.
        prefixes : list
            List of prefixes to filter by.

        Returns
        -------
        pd.DataFrame
            DataFrame containing only rows where at least one specified column value starts with any of the given prefixes.
        """
        row_mask = self.get_prefix_match_mask(df, target_cols, prefixes)
        return df[row_mask].copy()
    
    def seperate_data_by_prefix(self, df: pd.DataFrame, common_cols:list, prefix:str, keep_prefix = False) -> pd.DataFrame:
        """
        Function for extracting specific columns in a DataFrame by a prefix. This is useful if there are screening and clinical columns in a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to extract columns from.
        common_cols : list
            List of common columns to keep in the DataFrame.
        prefix : str
            The prefix to filter columns by.

        Returns
        -------
        pd.DataFrame
            DataFrame containing only the common columns and columns with the specified prefix.
        """
        df = df.copy()

        cols = [col for col in df.columns if col.startswith(prefix)]

        df = df[common_cols + cols]

        if not keep_prefix:
            df.columns = [col.replace(prefix, "") for col in df.columns]

        return df, cols
    
    def pivot_data(
        self, df: pd.DataFrame, index_cols: list, pivot_cols: list, value_col: str
    ) -> pd.DataFrame:
        """
        Pivot a DataFrame by specified index and pivot columns, expanding rows to ensure each cell contains only one value.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to pivot.
        index_cols : list
            List of column names to use as the index for grouping.
        pivot_cols : list
            List of column names to pivot. Unique values in these columns will form individual columns
            in the output DataFrame.
        value_col : str
            Column containing values to populate the pivot table. If multiple values exist for the same
            combination of `index_cols` and `pivot_cols`, they are aggregated into lists.

        Returns
        -------
        pd.DataFrame
            A pivoted DataFrame with expanded rows, where each cell contains a single value. For cases with
            multiple values in the same cell, the values are first aggregated into lists and then expanded
            across rows.

        Example
        -------
        >>> df_pivoted = pivot_data(
            df=diagnosis,
            index_cols=["patient_id", "origin", "diagnosis_date"],
            pivot_cols=["diagnosis_type"],
            value_col="diagnosis_code"
        )
        """
        df_pivoted = df.pivot_table(
            index=index_cols,
            columns=pivot_cols,
            values=value_col,
            aggfunc=lambda x: list(x),
        ).reset_index()

        df_pivoted.columns.name = None

        pivot_columns = [col for col in df[pivot_cols].unique()]

        for col in pivot_columns:
            df_pivoted = df_pivoted.explode(col)

        return df_pivoted
    
    def assign_block_id(self, df, patient_id, start, stop, time=0):
        """
        Function for assigning block ids to overlapping time periods for each patient. This is useful for combining overlapping hospitalisations

        Parameters
        ----------
        df : _type_
            Dataframe containing dates
        patient_id : _type_
            column name of the column containing patient ids
        start : _type_
            column name of the column containing the start dates
        stop : _type_
            column name of the column containing the stop dates
        time : int, optional
            Minimum time gap between hospitalisations, by default 0

        Returns
        -------
        Original DataFrame with an additional column "block_id" indicating overlapping periods.
        """
        df = df.copy()
        start_date = start + '_date'
        stop_date = stop + '_date'

        df[start_date] = df[start].dt.normalize()
        df[stop_date] = df[stop].dt.normalize()
        df = df.sort_values([patient_id, start_date, stop_date]).reset_index(drop=True)

        prev_max_end = df.groupby(patient_id)[stop_date].cummax().shift()
        new_block = (df[start_date] > (prev_max_end + pd.Timedelta(days=time))) | prev_max_end.isna()
        df["block_id"] = new_block.groupby(df[patient_id]).cumsum()
        return df
    

    def calculate_hospitalisation_times(self, df, unique_id, baseline, start, stop, time):
        df = df.copy()
        
        for col in [baseline, start, stop]:
            df[col] = pd.to_datetime(df[col])

        start_date = start + '_date'
        stop_date = stop + '_date'
        df[start_date] = df[start]
        df[stop_date] = df[stop]
        df['baseline_date'] = df[baseline]

        
        window_start = df['baseline_date'] + pd.Timedelta(days=1)
        window_end = df['baseline_date'] + pd.Timedelta(days=time)

        # clip dates
        df['start_date_trunc'] = df[start_date].clip(lower=window_start)
        df['stop_date_trunc'] = df[stop_date].clip(upper=window_end)

        # needed so that one day hospitalisations are counted also
        df['diff'] = (df['stop_date_trunc'] - df['start_date_trunc']).dt.days + 1
        
        
        df = df[df['start_date_trunc'] <= df['stop_date_trunc']]
        
        df_hosp_times = df.groupby(unique_id).agg({
            'diff': 'sum'
        }).reset_index().rename(columns={'diff': f'hosp_time_{time}'})

        return df_hosp_times