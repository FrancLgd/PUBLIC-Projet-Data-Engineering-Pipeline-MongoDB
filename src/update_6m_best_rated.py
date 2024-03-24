#### ---- DESCRIPTION DU SCRIPT

#### ---- LIBRAIRIES
import argparse
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pymongo
import configparser
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

#### ---- PARAMETRES

date_du_jour = datetime.now()

# Arguments en ligne de commandes
parser = argparse.ArgumentParser()
# parser.add_argument('--DATE_RUN', help = "Date de run", type = str, default = "2015-02-15")
parser.add_argument('--DATE_RUN', help = "Date de run", type = str, default = date_du_jour.strftime("%Y-%m-%d"))
args = parser.parse_args("")

# Fichier de configuration
config = configparser.ConfigParser()
config.read('./config/cfg.ini')

# Fichier des secrets
secrets = configparser.ConfigParser()
secrets.read('./config/secrets.ini')

#### ---- TRAITEMENTS

# Formater la date de run
date_run = datetime.strptime(args.DATE_RUN,"%Y-%m-%d")

# Se connecter à la base MongoDB
uri = config['mongodb']['uri']\
    .format(usr = secrets['mongodb']['usr'],
            pwd = secrets['mongodb']['pwd'])
client = pymongo.MongoClient(uri)
db = client.videogame
collection = db.video_games

# Définir le pipeline

# ... date de run laggée de 6 mois
lagged_date_run = date_run - relativedelta(months=6)

pipeline = [

    # ... filtrer sur les dates et champs non-manquants
    {
        '$match': {
            'unixReviewTime': {
                '$gt': int(lagged_date_run.timestamp()), 
                '$lt': int(date_run.timestamp())
            }, 
            'overall': {
                '$exists': True, 
                '$ne': None
            }
        }
    }, 
    
    # ... pour chaque jeu, agréger les notes
    {
        '$group': {
            '_id': '$asin', 
            'mean_rate': {
                '$avg': '$overall'
            }, 
            'nb_users': {
                '$addToSet': '$reviewerID'
            },
            "reviews" : {
                "$push": {
                    "overall" : "$overall",
                    "unixReviewTime" : "$unixReviewTime"
                }
            }
        }
    }, 
    {
        '$addFields': {
            'nb_users': {
                '$size': '$nb_users'
            }
        }
    }
]

# Exécuter le pipeline
resultat_pipeline = collection.aggregate(pipeline)

# Stocker le résultat dans une liste
list_docs = list(resultat_pipeline)

# Calculer les champs "most_recent_rate" et "oldest_rate"
def reviews_processing(doc):
    
    doc_cop = doc.copy()
    df_reviews = pd.DataFrame(doc['reviews'])

    # ... pour "most_recent_rate"
    doc_cop['most_recent_rate'] = df_reviews\
        .sort_values(by = 'unixReviewTime',ascending = False)\
        .head(1)["overall"].iloc[0]

    # ... pour "oldest_rate"
    doc_cop['oldest_rate'] = df_reviews\
        .sort_values(by = 'unixReviewTime',ascending = True)\
        .head(1)["overall"].iloc[0]
    
    # ... plus besoin de conserver les reviews
    doc_cop.pop("reviews",None)

    return doc_cop

list_docs = list(map(reviews_processing,list_docs))

# Convertir en dataframe
df = pd.DataFrame(list_docs)

# Renommer une colonne dans le DataFrame df
df.rename(columns={'_id': 'asin'}, inplace=True)

# S'assurer que le type soit str
df["asin"] = df["asin"].astype(str)

# Ajouter la colonne date_run
df['date_run'] = args.DATE_RUN

# Ne garder que les 15 jeux les mieux notés
# ... si les notes sont égales on prend le jeu
# ... avec le plus grand nombre de votes
df_best = df\
    .sort_values(by=['mean_rate','nb_users'],ascending=False)\
    .head(15)

try:
    
    # Se connecter à la base de données Postgresql
    engine = sqlalchemy.create_engine(
    "postgresql+psycopg2://{usr}:{pwd}@{hst}:{port}/{db}"
        .format(usr = secrets["videogame_ratings"]["usr"],
                pwd = secrets["videogame_ratings"]["pwd"],
                hst = config["videogame_ratings"]["hst"],
                port = config["videogame_ratings"]["port"],
                db = config["videogame_ratings"]["db"]))
        
    # Insérer les records
    df_best.to_sql(
        name="6m_best_rated",
        con=engine, 
        if_exists='append',
        index=False)

except SQLAlchemyError as error:

    print("Erreur lors de l'insertion des données")