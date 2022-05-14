import boto3
import pandas as pd


# criar um cliente para interagir com o AWS s3 

s3_client = boto3.client('s3')

s3_client.download_file("datalake-augusto-igti-edc",
                        "data/Produtos sem gatilho atacado.xlsx",
                        "data/Produtos sem gatilho atacado.xlsx")

df = pd.read_excel("data/Produtos sem gatilho atacado.xlsx")
print(df)


s3_client.upload_file("MICRODADOS_ENEM_2020.xlsx",
                    "datalake-augusto-igti-edc",
                    "raw-data/enem/MICRODADOS_ENEM_2020.xlsx")