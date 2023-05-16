import pandas as pd
from tienda_amiga.cfg import DATA_DIR

def get_hora(hora: str):

    t_hora = hora.split(' ')
    if len(t_hora[1]) >= 8:
        res = t_hora[1][0:8]
    else: 
        res='0'
    
    return res

def solo_hora(hora):
    return hora[0:2]


def get_fecha(fecha: str):
    if len(fecha) >= 10:
        res = fecha[0:10]
    else: 
        res='0'    
    
    return res

def quitar_corchetes(cadena: str):
    
    if len(cadena) > 0: 
        res = cadena.replace("{", "").replace("}","").replace("[", "").replace("]","").replace("'", "")        
    return res


def transform(fecha: str):
    # TODO: Apply transformations
    df = pd.read_csv(DATA_DIR / "data_bitcoin.csv",usecols=['source_url','url','title','text','tags','authors','publish_date'])    
    df = df.dropna()    #eliminamos los nulos     
    df['publish_date_f'] = df['publish_date'].apply(get_fecha)
    df['publish_time'] = df['publish_date'].apply(get_hora)
    df['publish_hour'] = df['publish_time'].apply(solo_hora)
    df['fecha']=fecha  #agregamos la columna de la fecha de ejecucion del dag
    df['tags2'] = df['tags'].apply(quitar_corchetes)    
    df['authors2'] = df['authors'].apply(quitar_corchetes)    

    #eliminamos columnas antiguas   
    df.drop(["publish_date", "authors","tags"], axis = 1, inplace=True)
    #renombramos las columnas 
    df2=df.rename(columns = {'publish_date_f':'publish_date','authors2':'authors','tags2':'tags'})

    df2.to_csv(DATA_DIR / "data_bitcoin_transformed.csv", index=False)
