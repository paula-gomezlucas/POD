import os
import pandas as pd

class CSVDataLoader:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.data = []

    def load_data(self):
        csv_files = [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]
        excluded_datasets = ['ActuacionesBomberos_2022.csv', 'ActuacionesBomberos_2023.csv', 'DatosEstacionesAbril023.csv', 'DatosEstacionesAgosto2022,csv', 'DatosEstacionesDiciembre2022.csv', 'DatosEstacionesEnero2023.csv', 'DatosEstacionesFebrero2023.csv', 'DatosEstacionesJulio2023.csv', 'DatosEstacionesJunio2023.csv', 'DatosEstacionesMarzo2023.csv', 'DatosEstacionesMayo2023.csv', 'DatosEstacionesNoviembre2022.csv', 'DatosEstacionesOctubre2022.csv', 'DatosEstacionesSeptiembre2022.csv']
        for file_name in csv_files:
            if file_name in excluded_datasets:
                print(f"Excluyendo {file_name}")
                continue
            file_path = os.path.join(self.folder_path, file_name)
            try:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1', low_memory=False)
                self.data.append(df)
            except Exception as e:
                print(f"Error al leer {file_name}: {str(e)}")
    
    def clean_data(self):
        for i in range(len(self.data)):
            self.data[i] = self.data[i].rename(columns = lambda x: x.strip().lower().replace(' ', '_'))
            self.data[i] = self.data[i].map(lambda x: x.strip() if isinstance(x, str) else x)
            self.data[i] = self.data[i].dropna()
            self.data[i] = self.data[i].drop_duplicates()
            self.data[i] = self.data[i].loc[:, ~self.data[i].columns.duplicated()]
            self.data[i].columns = map(str.upper, self.data[i].columns)
            if 'FECHA' in self.data[i].columns:
                self.data[i]['FECHA'] = pd.to_datetime(self.data[i]['FECHA'], format='%d/%m/%Y')
    def get_cleaned_data(self):
        return self.data

if __name__ == "__main__":
    folder_path = "datasets"
    data_loader = CSVDataLoader(folder_path)
    data_loader.load_data()
    data_loader.clean_data()
    cleaned_data = data_loader.get_cleaned_data()
    print(cleaned_data)
