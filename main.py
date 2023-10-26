import os
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
        self.filename = []
    def load_data(self):
        """
        Loads CSV data from the specified folder path into a dictionary.

        Returns:
        --------
        None
        """
        csv_files = [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]
        folders = ("datasets/actuacionesBomberos", "datasets/estaciones", "datasets/accidentalidad")
        for folder in folders:
            df = None
            for file in os.listdir(folder):
                filepath = folder + "/" + file
                df1 = pd.read_csv(filepath, sep=';', encoding='utf-8', low_memory=False)
                df = pd.concat([df, df1])
            self.data[str(folder)] = df

        for file_name in csv_files:
            file_path = os.path.join(self.folder_path, file_name)
            try:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1', low_memory=False)
                self.data[str(file_name)] = df
                self.filename.append(file_name)
            except Exception as e:
                print(f"Error al leer {file_name}: {str(e)}")
    
    def clean_data(self):
        """
        Cleans the loaded CSV data by renaming columns, removing whitespace, dropping null values and duplicates, and converting date columns to datetime format.

        Returns:
        --------
        None
        """
        for i in self.data:
            num_columnas = self.data[i].shape[1] -1
            columna_borrar = "Unnamed: " + str(num_columnas)

            if columna_borrar in self.data[i].columns:
                print("im sad")
                self.data[i] = self.data[i].drop(columna_borrar, axis=1)
                self.data[i] = self.data[i].dropna()
            
            self.data[i] = self.data[i].rename(columns = lambda x: x.strip().lower().replace(' ', '_'))
            self.data[i] = self.data[i].map(lambda x: x.strip() if isinstance(x, str) else x)
            self.data[i] = self.data[i].dropna()
            self.data[i] = self.data[i].drop_duplicates()
            self.data[i] = self.data[i].loc[:, ~self.data[i].columns.duplicated()]
            self.data[i].columns = map(str.upper, self.data[i].columns)
            if 'FECHA' in self.data[i].columns:
                self.data[i]['FECHA'] = pd.to_datetime(self.data[i]['FECHA'], format='%d/%m/%Y')


    def get_nan_columns(self):
        for i in self.data:
            print(self.data[i].isnull().sum())
            print(self.data[i].info())
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
    data = data_loader.get_cleaned_data()
    print(data.values())
    data_loader.get_nan_columns()