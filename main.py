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
            num_columnas = df.shape[1] - 1
            columna_borrar = "Unnamed: " + str(num_columnas)
            df = df.drop(columna_borrar, axis=1)

        for file_name in csv_files:
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
    def get_nan_columns(self):
        for i in self.data:
            print(i)
            print("aaaaaaaaaaaaaaa")
            print(self.data[i].columns.values.tolist())
            print("aaaaaaaaaaaaaaaaaa")
            print(self.data[i].isnull().sum())
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
    print(data_loader.get_nan_columns())


"""
DireccionesVigentes_20231004.csv
COD_VIA                    0
VIA_CLASE                  0
VIA_PAR                 3381
VIA_NOMBRE                 0
VIA_NOMBRE_ACENTOS         0
CLASE_APP                  0
NUMERO                     0
CALIFICADOR           179040
TIPO_NDP                   0
COD_NDP                    0
DISTRITO                   0
BARRIO                     0
COD_POSTAL                 0
UTMX_ED                    0
UTMY_ED                    0
UTMX_ETRS                  0
UTMY_ETRS                  0
LATITUD                    0
LONGITUD                   0
ANGULO_ROTULACION          0
dtype: int64
RADARES FIJOS_vDTT.csv
Nº\nRADAR               0
Ubicacion               0
Carretara o vial        1
UBICACIÓN\nCalle 30     8
PK                      1
Sentido                 3
Tipo                    3
X (WGS84)              13
Y (WGS84)              13
Longitud                0
Latitud                 0
Coordenadas             0
dtype: int64
DireccionesEvolucionHistorica_20231004.csv
COD_VIA                    0
VIA_SQC               371584
VIA_CLASE                  0
VIA_PAR                17997
VIA_NOMBRE                 0
VIA_NOMBRE_ACENTOS         0
COD_NDP                    0
CLASE_NDP                  0
NÚMERO                     0
CALIFICADOR           322826
FECHA_DE_ALTA              0
FECHA_DE_BAJA         211657
TIPO_NDP               16248
UTMX_ED                    0
UTMY_ED                    0
UTMX_ETRS                  0
UTMY_ETRS                  0
LATITUD                    0
LONGITUD                   0
ANGULO_ROTULACION          0
dtype: int64
VialesVigentesDistritosBarrios_20231004.csv
COD_VIA                  0
VIA_CLASE                0
VIA_PAR                601
VIA_NOMBRE               0
VIA_NOMBRE_ACENTOS       0
DISTRITO                24
BARRIO                  24
IMPAR_MIN             2719
IMPAR_MAX             2719
PAR_MIN               2558
PAR_MAX               2558
dtype: int64
Direcciones_vigentes2016.csv
COD_VIA             0
CLASE               0
PARTICULA        3310
NOMBRE              0
CLASE_APP           0
NUMERO_TX           0
CALIFICADOR    178959
TIPO_NDP          479
COD_NDP             0
DISTRITO            0
UTMX_ETRS           0
UTMY_ETRS           0
dtype: int64
VialesEvolucionHistorica_20231004.csv
COD_VIA                  0
VIA_SQC                  0
VIA_CLASE                0
VIA_PAR               1724
VIA_NOMBRE               0
VIA_NOMBRE_ACENTOS       0
FECHA_DE_ALTA            0
FECHA_DE_BAJA         9354
VIA_ESTADO               0
dtype: int64
VialesVigentesDistritos_20231004.csv
COD_VIA                  0
VIA_CLASE                0
VIA_PAR                601
VIA_NOMBRE               0
VIA_NOMBRE_ACENTOS       0
DISTRITO                24
IMPAR_MIN             2081
IMPAR_MAX             2081
PAR_MIN               1879
PAR_MAX               1879
dtype: int64
Viales_vigentes2016.csv
COD_VIA                  0
VIA_CLASE                0
VIA_PAR                272
VIA_NOMBRE               0
COD_VIA_COMIENZA         0
CLASE_COMIENZA           0
PARTICULA_COMIENZA     428
VIA_NOMBRE_COMIENZA      0
COD_VIA_TERMINA          0
CLASE_TERMINA            0
PARTICULA_TERMINA      791
VIA_NOMBRE_TERMINA       0
dtype: int64
VialesVigentes_20231004.csv
COD_VIA                      0
VIA_CLASE                    0
VIA_PAR                    402
VIA_NOMBRE                   0
VIA_NOMBRE_ACENTOS           0
COD_VIA_COMIENZA             0
CLASE_COMIENZA               0
PARTICULA_COMIENZA         545
NOMBRE_COMIENZA              0
NOMBRE_ACENTOS_COMIENZA      0
COD_VIA_TERMINA              0
CLASE_TERMINA                0
PARTICULA_TERMINA          917
NOMBRE_TERMINA               0
NOMBRE_ACENTOS_TERMINA       0
dtype: int64
None
"""