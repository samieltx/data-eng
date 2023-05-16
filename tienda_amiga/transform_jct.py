import pandas as pd
from tienda_amiga.cfg import DATA_DIR

def transform(fecha: str):
    # TODO: Apply transformations
    df = pd.read_csv(DATA_DIR / "data_bitcoin.csv",usecols=['source_url','url','title','text','tags','authors','publish_date'])    
    df['fecha']=fecha
    df.to_csv(DATA_DIR / "data_bitcoin_transformed.csv", index=False)
