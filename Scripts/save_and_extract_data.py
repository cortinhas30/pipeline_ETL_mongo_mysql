from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os
from dotenv import load_dotenv


def connect_mongo(uri):

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    return client

def create_connect_db(client, nomedb):
    db = client[nomedb] #criando banco de dados
    return db

def create_collection_db(client, colname):
    collection = client[colname] #criando a coleção do banco de dados
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":

    client = os.getenv("MONGODB_URI")

    client = connect_mongo(client)
    db = create_collection_db(client, "db_produtos_desafio")
    col = create_collection_db(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\n Quantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\n Quantidade de documentos inseridos: {n_docs}")

    client.close()