import os
import pandas as pd
import pandas as pd

class CSVDataLoader:
    """
    A class for loading and cleaning CSV data from a specified folder path.

    Attributes:
    -----------
    folder_path : str
        The path to the folder containing the CSV files to be loaded.

    data : dict
        A dictionary containing the loaded CSV data, where the keys are the file names and the values are the corresponding dataframes.
    """

    def __init__(self, folder_path):
        """
        Initializes a CSVDataLoader object with the specified folder path.

        Parameters:
        -----------
        folder_path : str
            The path to the folder containing the CSV files to be loaded.
        """
        self.folder_path = folder_path
        self.data = {}

    def load_data(self):
        """
        Loads CSV data from the specified folder path into a dictionary.

        Excludes certain datasets based on their file names.

        Returns:
        --------
        None
        """
        csv_files = [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]
        excluded_datasets = ['ActuacionesBomberos_2022.csv', 'ActuacionesBomberos_2023.csv', 'DatosEstacionesAbril023.csv', 'DatosEstacionesAgosto2022,csv', 'DatosEstacionesDiciembre2022.csv', 'DatosEstacionesEnero2023.csv', 'DatosEstacionesFebrero2023.csv', 'DatosEstacionesJulio2023.csv', 'DatosEstacionesJunio2023.csv', 'DatosEstacionesMarzo2023.csv', 'DatosEstacionesMayo2023.csv', 'DatosEstacionesNoviembre2022.csv', 'DatosEstacionesOctubre2022.csv', 'DatosEstacionesSeptiembre2022.csv']
        for file_name in csv_files:
            if file_name in excluded_datasets:
                print(f"Excluyendo {file_name}")
                continue
            file_path = os.path.join(self.folder_path, file_name)
            try:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1', low_memory=False)
                self.data[str(file_name)] = df
            except Exception as e:
                print(f"Error al leer {file_name}: {str(e)}")
    
    def clean_data(self):
        """
        Cleans the loaded CSV data by renaming columns, removing whitespace, dropping null values and duplicates, and converting date columns to datetime format.

        Returns:
        --------
        None
        """
        for i in self.data.values():
            i = i.rename(columns = lambda x: x.strip().lower().replace(' ', '_'))
            i = i.map(lambda x: x.strip() if isinstance(x, str) else x)
            i = i.dropna()
            i = i.drop_duplicates()
            i = i.loc[:, ~i.columns.duplicated()]
            i.columns = map(str.upper, i.columns)
            if 'FECHA' in i.columns:
                i['FECHA'] = pd.to_datetime(i['FECHA'], format='%d/%m/%Y')

    def get_cleaned_data(self):
        """
        Returns the cleaned CSV data as a dictionary.

        Returns:
        --------
        dict
            A dictionary containing the cleaned CSV data, where the keys are the file names and the values are the corresponding dataframes.
        """
        return self.data

if __name__ == "__main__":
    folder_path = "datasets"
    data_loader = CSVDataLoader(folder_path)
    data_loader.load_data()
    data_loader.clean_data()
    cleaned_data = data_loader.get_cleaned_data()
    print(cleaned_data)
