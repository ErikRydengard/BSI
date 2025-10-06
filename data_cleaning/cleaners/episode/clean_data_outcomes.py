from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner
import pandas as pd
import numpy as np

class OutcomesCleaner(EpisodeCleaner):
    def __init__(self):
        super().__init__()

    def add_mortality(
        self,
        reference_df: pd.DataFrame,
        microbiology_df: pd.DataFrame,
        deceased_df: pd.DataFrame,
        mortality_time: pd.Timedelta,
        mortality_column_name: str
    ) -> pd.DataFrame:
        """
        Calculates the mortality for patients in a DataFrame.

        Parameters
        ----------
        reference_df : pd.DataFrame
            DataFrame with patients and their hospitalisations.
        microbiology_df : pd.DataFrame
            DataFrame with patients and their sample dates.
        deceased_df : pd.DataFrame
            DataFrame containing information and data for deceased and deceased date.
        mortality_time : pd.Timedelta
            The time after baseline for mortality.
        mortality_column_name : str
            The name for the resulting column containing the mortality.

        Returns
        -------
        pd.DataFrame
            DataFrame containing mortality data.
        """
        reference_df = reference_df.copy()
        microbiology_df = microbiology_df.copy()
        deceased_df = deceased_df.copy()

        # Get the latest hosp dates for each patient_id
        reference_df = reference_df.sort_values(['patient_id', 'hosp_start'], ascending=[True, False])[['patient_id', 'hosp_start', 'hosp_stop']]
        reference_df = reference_df.drop_duplicates(subset=['patient_id']).dropna(subset=['hosp_start'])

        # Merge the deceased data with the latest hosp dates
        deceased_latest_hosp = deceased_df.merge(reference_df, how='left', on='patient_id')
        deceased_latest_hosp = deceased_latest_hosp.rename(
            columns={'hosp_start': 'latest_in_date', 'hosp_stop': 'latest_out_date'}
        )
        deceased_latest_hosp['latest_in_date'] = deceased_latest_hosp['latest_in_date'].dt.normalize()
        deceased_latest_hosp['latest_out_date'] = deceased_latest_hosp['latest_out_date'].dt.normalize()

        # Merge the deceased data with episode IDs and sample dates from the microbiology dataframe
        deceased_episodes = deceased_latest_hosp.merge(microbiology_df[['patient_id', 'episode_id', 'sample_date']], how='left', on='patient_id')

        # Condition 1: If `deceased` is False, mortality is False
        cond1 = deceased_episodes['deceased'] == False

        # Condition 2: If both `latest_in_date` and `deceased` are NaN, set to NaN
        cond2 = deceased_episodes['latest_in_date'].isna() & deceased_episodes['deceased'].isna()

        # Condition 3: If `deceased_date` is within mortality_time of `sample_date`, set to True
        cond3 = deceased_episodes['deceased_date'] <= deceased_episodes['sample_date'] + mortality_time

        # Condition 4: If `deceased_date` is NaN and `latest_out_date` is not mortality_time after `sample_date`, set to NaN
        cond4 = deceased_episodes['deceased_date'].isna() & ~(deceased_episodes['latest_out_date'] > deceased_episodes['sample_date'] + mortality_time)

        # Default to False for all other cases
        deceased_episodes[mortality_column_name] = np.select(
            [cond1, cond2, cond3, cond4],
            [False, None, True, None],
            default=False
        )
        
        return deceased_episodes.drop(columns=['latest_in_date', 'latest_out_date'])
    
    def add_readmitted(self, df: pd.DataFrame, hospitalisations_df: pd.DataFrame, date_limit_col_name: str) -> pd.DataFrame:
        """
        Calculates readmittance for patients in a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with patient information.
        hospitalisations_df : pd.DataFrame
            DataFrame with hospitalisations.
        date_limit_col_name : str
            Name of the column to check for readmittance, typically the sample date.

        Returns
        -------
        pd.DataFrame
            DataFrame with readmittance and time to readmittance added.
        """
        df = df.copy()
        hospitalisations_df = hospitalisations_df.copy()

        # Merge episode and hospitalisation data
        merged = df.merge(hospitalisations_df, on='patient_id', how='left')

        # Determine readmitted status
        merged['readmitted'] = merged['in_date'] > merged[date_limit_col_name]

        # Identify the first readmission date for each episode
        merged_readmitted = (
            merged.query('readmitted == True')
            .sort_values(['episode_id', 'in_date'])
            .drop_duplicates(subset=['episode_id'])
            .rename(columns={'in_date': 'first_in_date'})
        )

        # Aggregate readmitted status by episode_id
        readmitted = merged.groupby('episode_id')['readmitted'].any().reset_index()

        # Merge first readmission date back into the readmitted DataFrame
        readmitted = readmitted.merge(
            merged_readmitted[['episode_id', 'first_in_date']],
            how='left',
            on='episode_id'
        )

        # Merge readmittance info back to the original DataFrame
        df = df.merge(readmitted, on='episode_id', how='left')

        # Calculate time to readmittance in days
        df['time_to_readmittance'] = (df['first_in_date'] - df[date_limit_col_name]).dt.days

        return df.drop(columns='first_in_date')
    
    def get_days_of_care_after_baseline(self, hospitalisations: pd.DataFrame, microbiology: pd.DataFrame, days_after_baseline: int) -> pd.DataFrame:
        """
        Calculates days of care after first hospitalisation.

        Parameters
        ----------
        hospitalisations : pd.DataFrame
            DataFrame with hospitalisations.
        microbiology : pd.DataFrame
            DataFrame with episode IDs and sample dates.
        days_after_baseline : int
            Limit of days after baseline/sample date to include.

        Returns
        -------
        pd.DataFrame
            DataFrame with episode IDs and days of care.
        """
        hospitalisations = hospitalisations.copy()
        microbiology = microbiology.copy()
        
        # Adds episode_id to hospitalisations and removes rows where sample_date is after out_date
        hospitalisations = hospitalisations.dropna(subset=['in_date', 'out_date']
                                                ).merge(microbiology[['patient_id', 'episode_id','sample_date']], how='left', on='patient_id')
        hospitalisations = hospitalisations[hospitalisations['sample_date'] <= hospitalisations['out_date']
                                            ].sort_values(['episode_id', 'in_date', 'out_date']
                                                        ).drop(columns='patient_id')
        
        hospitalisations['previous_in_date'] = hospitalisations.groupby('episode_id')['in_date'].shift(1)
        hospitalisations['previous_out_date'] = hospitalisations.groupby('episode_id')['out_date'].shift(1)

        # Calculates the first out date for each episode
        first_out_dates = hospitalisations.drop_duplicates(subset=['episode_id']
                                                                    ).rename(columns={'out_date': 'first_out_date'}
                                                                            )[['episode_id', 'first_out_date']]
        hospitalisations = hospitalisations.merge(first_out_dates, how='left', on='episode_id')

        # Removes first care occasion
        hospitalisations = hospitalisations[hospitalisations['episode_id'].duplicated()]

        # Removes care occasions with in_dates 365 days after first_out_date
        hospitalisations['date_limit'] = hospitalisations['first_out_date'] + pd.Timedelta(days=days_after_baseline)
        hospitalisations = hospitalisations[
            hospitalisations['in_date'] < hospitalisations['date_limit']
        ]

        # Sets out_date to first_out_date + 365 days, if out_date is after this date.
        hospitalisations['out_date'] = np.minimum(hospitalisations['out_date'], hospitalisations['date_limit'])

        # Counts total days and days overlapping with the previous hospitalisation
        hospitalisations['total_days'] = (hospitalisations['out_date'] - hospitalisations['in_date']).dt.days + 1
        hospitalisations['overlapping_days'] = np.maximum(0, (hospitalisations['previous_out_date'] - hospitalisations['in_date']).dt.days + 1)

        # Sets overlapping_days to 0 for the first occurrence of each episode_id
        hospitalisations.loc[hospitalisations.groupby('episode_id').cumcount() == 0, 'overlapping_days'] = 0

        # Calculates the correct days of care for each hospitalisation, taking overlapping days into consideration
        hospitalisations[f'days_of_care_{days_after_baseline}_days_after_baseline'] = hospitalisations['total_days'] - hospitalisations['overlapping_days']

        # Calculates the sum of days for each episode
        days_of_care = hospitalisations.groupby('episode_id')[f'days_of_care_{days_after_baseline}_days_after_baseline'].sum().reset_index()
        
        return days_of_care
    
    def get_days_of_care_before_baseline(self, hospitalisations: pd.DataFrame, microbiology: pd.DataFrame, days_before_baseline: int) -> pd.DataFrame:
        """
        Calculates days of care before baseline.

        Parameters
        ----------
        hospitalisations : pd.DataFrame
            DataFrame with hospitalisations.
        microbiology : pd.DataFrame
            DataFrame with episode IDs and sample dates.
        days_before_baseline : int
            Limit of days before baseline/sample date to include.

        Returns
        -------
        pd.DataFrame
            DataFrame with episode IDs and days of care.
        """
        hospitalisations = hospitalisations.copy()
        microbiology = microbiology.copy()

        # Adds episode_id to hospitalisations and removes rows where sample_date is before out_date
        hospitalisations = hospitalisations.dropna(subset=['in_date', 'out_date']
                                                    ).merge(microbiology[['patient_id', 'episode_id','sample_date']], how='left', on='patient_id')
        hospitalisations = hospitalisations[hospitalisations['sample_date'] > hospitalisations['out_date']
                                                ].sort_values(['episode_id', 'in_date', 'out_date']
                                                            ).drop(columns='patient_id')
        
        # Remove rows where in_date is earlier than 365 days before sample_date
        hospitalisations = hospitalisations[
                hospitalisations['in_date'] > hospitalisations['sample_date'] - pd.Timedelta(days=days_before_baseline)
            ]
        
        hospitalisations['previous_in_date'] = hospitalisations.groupby('episode_id')['in_date'].shift(1)
        hospitalisations['previous_out_date'] = hospitalisations.groupby('episode_id')['out_date'].shift(1)

        # Counts total days and days overlapping with the previous hospitalisation
        hospitalisations['total_days'] = (hospitalisations['out_date'] - hospitalisations['in_date']).dt.days + 1
        hospitalisations['overlapping_days'] = np.maximum(0, (hospitalisations['previous_out_date'] - hospitalisations['in_date']).dt.days + 1)

        # Sets overlapping_days to 0 for the first occurrence of each episode_id
        hospitalisations.loc[hospitalisations.groupby('episode_id').cumcount() == 0, 'overlapping_days'] = 0

        # Calculates the correct days of care for each hospitalisation, taking overlapping days into consideration
        hospitalisations[f'days_of_care_{days_before_baseline}_days_before_baseline'] = hospitalisations['total_days'] - hospitalisations['overlapping_days']

        # Calculates the sum of days for each episode
        days_of_care = hospitalisations.groupby('episode_id')[f'days_of_care_{days_before_baseline}_days_before_baseline'].sum().reset_index()

        return days_of_care