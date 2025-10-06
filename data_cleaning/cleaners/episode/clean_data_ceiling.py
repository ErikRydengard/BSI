from data_cleaning.cleaners.episode.episodeCleaner import EpisodeCleaner
from data_cleaning.transformations import remove_redundant_decimals
import pandas as pd

class CeilingCleaner(EpisodeCleaner):
    def __init__(self):
        super().__init__()


    def map_ceiling_decision(self, ceiling_decision: str) -> int:
        """
        Maps ceiling decision to a numerical value based on keywords. Helper function for filter_and_link_patient_id.

        Parameters
        ----------
        ceiling_decision : str
            Ceiling decision as a string.

        Returns
        -------
        int
            The numerical value of a string ceiling decision.
        """
        ceiling_decision = ceiling_decision.lower()
        if 'pall' in ceiling_decision:
            return 3
        elif 'intens' in ceiling_decision:
            return 2
        elif 'hlr' in ceiling_decision:
            return 1
        else:
            return None


    def filter_and_link_patient_id(
            self,
            df_multiple: pd.DataFrame,
            df_care_data: pd.DataFrame,
            hosp_id_col_name: str,
            ceiling_decision_col_name: str,
            ceiling_date_col_name: str,
            patient_id_col_name: str
    ) -> pd.DataFrame:
        """
        Filters and cleans ceiling data. Additionally, links patient IDs to ceiling data. Helper function for clean_ceiling_data.

        Parameters
        ----------
        df_multiple : pd.DataFrame
            DataFrame with multiple choice ceiling data.
        df_care_data : pd.DataFrame
            DataFrame with patient care data, containing hosp IDs and patient IDs.
        hosp_id_col_name : str
            Column name for hosp IDs.
        ceiling_decision_col_name : str
            Column name for ceiling decisions.
        ceiling_date_col_name : str
            Column name for ceiling dates.
        patient_id_col_name : str
            Column name for patient IDs.

        Returns
        -------
        pd.DataFrame
            Filtered and cleaned DataFrame.
        """
        
        # Extract data from multiple choice DataFrames
        df = pd.DataFrame()
        df['hosp_id'] = df_multiple[hosp_id_col_name]
        df['ceiling_decision'] = df_multiple[ceiling_decision_col_name]
        df['ceiling_date'] = df_multiple[ceiling_date_col_name]

        # Link care hosps with patient IDs
        df_care_data = df_care_data.rename(columns={hosp_id_col_name: 'hosp_id'})
        df = df.merge(
            df_care_data[['hosp_id', patient_id_col_name]].drop_duplicates(subset='hosp_id'),
            how='left',
            on='hosp_id'
        ).rename(columns={patient_id_col_name: 'patient_id'})

        # Create new column with ceiling decision numbers based on mapping
        df['ceiling_decision'] = df['ceiling_decision'].apply(self.map_ceiling_decision)

        # Map the numerical values to the corresponding labels and replace them
        ceiling_decision_labels = {1: "No CPR", 2: "No CPR or ICU", 3: "Palliative care"}
        df['ceiling_decision'] = df['ceiling_decision'].map(ceiling_decision_labels)
        
        # Convert to a categorical dtype
        df['ceiling_decision'] = pd.Categorical(df['ceiling_decision'], categories=["No CPR", "No CPR or ICU", "Palliative care"], ordered=True)
        
        # Drop rows with NaN values
        df = df.dropna(subset='ceiling_decision', ignore_index=True)


        return df


    def clean_ceiling_data(
            self,
            df_outpatient_multiple: pd.DataFrame,
            df_inpatient_multiple: pd.DataFrame,
            df_outpatient_care: pd.DataFrame,
            df_inpatient_care: pd.DataFrame,
            hosp_id_col_name: str,
            ceiling_decision_col_name: str,
            ceiling_date_col_name: str,
            patient_id_col_name: str
    ) -> pd.DataFrame:
        """
        Cleans ceiling data.

        Parameters
        ----------
        df_outpatient_multiple : pd.DataFrame
            DataFrame with outpatient care multiple choice ceiling data.
        df_inpatient_multiple : pd.DataFrame
            DataFrame with inpatient care multiple choice ceiling data.
        df_outpatient_care : pd.DataFrame
            DataFrame with outpatient care data, containing hosp IDs and patient IDs.
        df_inpatient_care : pd.DataFrame
            DataFrame with inpatient care data, containing hosp IDs and patient IDs.
        hosp_id_col_name : str
            Column name for hosp IDs.
        ceiling_decision_col_name : str
            Column name for ceiling decisions.
        ceiling_date_col_name : str
            Column name for ceiling dates.
        patient_id_col_name : str
            Column name for patient IDs.

        Returns
        -------
        pd.DataFrame
            Cleaned ceiling data.
        """
        df_outpatient_filtered = self.filter_and_link_patient_id(
            df_outpatient_multiple,
            df_outpatient_care,
            hosp_id_col_name,
            ceiling_decision_col_name,
            ceiling_date_col_name,
            patient_id_col_name
        )

        df_inpatient_filtered = self.filter_and_link_patient_id(
            df_inpatient_multiple,
            df_inpatient_care,
            hosp_id_col_name,
            ceiling_decision_col_name,
            ceiling_date_col_name,
            patient_id_col_name
        )

        df = pd.concat([df_outpatient_filtered, df_inpatient_filtered])

        # Keep only the row with the 'worst' ceiling_decision for each hosp_id
        # Sort by hosp_id, ceiling_date and ceiling_decision according to ascending boolean list 
        # to have the 'worst' ceiling decision at the top of each group
        df = df.sort_values(['hosp_id', 'ceiling_date', 'ceiling_decision'], ascending=[True, True, False])

        # Drop duplicates, keeping only the first occurrence within each hosp_id group
        df = df.drop_duplicates(subset=['hosp_id'], keep="first", ignore_index=True)

        df = remove_redundant_decimals(df)

        # Remove timezone sensitivity from datetimes
        df[['ceiling_date']] = df[['ceiling_date']].apply(lambda col: col.dt.tz_localize(None))

        return df