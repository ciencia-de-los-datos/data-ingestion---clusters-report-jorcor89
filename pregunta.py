"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    
    #
    # Inserte su código aquí
    #

    df = pd.read_fwf('clusters_report.txt',
                     skiprows = 4,
                     skipfooter = 0,
                     names = ['Cluster', 'Cantidad de palabras clave',
                            'Porcentaje de palabras clave', 'Principales palabras clave'],
                     keep_default_na = False,
                     )
    
    for index, element in enumerate(df['Cluster']):
        if element == '':
            df['Cluster'].iloc[index] = df['Cluster'].iloc[index-1]
            df['Cantidad de palabras clave'].iloc[index] = df['Cantidad de palabras clave'].iloc[index-1]
            df['Porcentaje de palabras clave'].iloc[index] = df['Porcentaje de palabras clave'].iloc[index-1]
    
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()

    for name in df.columns:
        df[name] = df[name].apply(
            lambda value: " ".join(str(value).strip().split()))

    df['porcentaje_de_palabras_clave']= df['porcentaje_de_palabras_clave'].str[:-2]
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(',', '.')
    
    df = df.astype({'cluster': int})
    df = df.astype({'cantidad_de_palabras_clave': int})
    df = df.astype({'porcentaje_de_palabras_clave': float})
    df = df.astype({'principales_palabras_clave': 'string'})
    
    df['principales_palabras_clave'] = df['principales_palabras_clave'].apply(lambda x: x.strip('.'))
    
    df = df.groupby(['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave'])[
        'principales_palabras_clave'].apply(' '.join).reset_index()
    
    return df