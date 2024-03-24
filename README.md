
<h2 align="center">Mise en place d'un pipeline ETL depuis une base MongoDB</h2>


<div align="center"><img src="https://cdn.static-media.blent.ai/images/projects/badge_mongodb.svg" width="120" alt="Badge du projet" /></div>

<h2 align="center">François Legland</h2>

## À propos du projet

### Description

Une enseigne de jeux vidéos cherche à améliorer son catalogue de vente en ligne. Pour cela, elle veut proposer sur sa page d’accueil et dans ses campagnes de communication (newsletter, réseaux sociaux) une  **liste des jeux les mieux notés et les plus appréciés**  de la communauté sur les derniers jours.

Afin de refléter au mieux l’avis des internautes, elle souhaite  **récupérer les avis les plus récents**  de ses propres clients en ligne pour déterminer les jeux les mieux notés. Les développeurs Web de l’entreprise souhaitent pouvoir requêter ces informations sur une base de données SQL qui va historiser au jour le jour les jeux les mieux notés.

Les données brutes stockées dans une base MongoDB, et il est supposé que celles-ci sont ajoutées au fur et à mesure par d’autres programmes (API backend). L’objectif est de construire un pipeline de données qui va alimenter automatiquement un Data Warehouse (représenté par une base de données SQL) tous les jours en utilisant les données depuis la base MongoDB.

### Contraintes

* Les informations des jeux les mieux notées vont êtres disponibles sur le site Web, ainsi que dans les campagnes de communication. Elles doivent donc être à la fois **exactes et faciles à manipuler** pour les différents métiers qui vont l’utiliser.

* Le Data Warehouse doit être une base de données **compatible SQL** : MySQL, PostgreSQL ou MariaDB.
  
* Le pipeline doit être capable de gérer les situations où les données sont déjà présentes et **éviter les doublons** (les valeurs existantes doivent être remplacées).
  
* **Chaque jour**, les 15 jeux les mieux notés sur **les 6 derniers mois** seront ajoutés dans le Data Warehouse. Il ne faut donc pas prendre en compte les avis de plus de 6 mois d'antériorité.

## Étapes de réalisation

### 0. Clôner le dépôt git

```shell
git clone https://github.com/FrancLgd/Projet-Data-Engineering-Pipeline-MongoDB.git
cd Projet-Data-Engineering-Pipeline-MongoDB
```

### 1. Installer les dépendances

#### 1.1 Pour les librairies Python

```shell
python3 -m venv venv
source venv/bin/activate
# pip install pandas==2.1.* psycopg2-binary sqlalchemy pymongo apache-airflow
pip install -r requirements.txt
```

#### 1.2. Pour MongoDB Shell

```shell
wget https://downloads.mongodb.com/compass/mongosh-2.1.5-linux-x64.tgz
tar -zxvf mongosh-2.1.5-linux-x64.tgz
mv mongosh-2.1.5-linux-x64 mongosh
sudo cp mongosh/bin/mongosh /usr/local/bin/
sudo cp mongosh/bin/mongosh_crypt_v1.so /usr/local/lib/
rm mongosh-2.1.5-linux-x64.tgz
```

#### 1.3. Pour MongoDB Database Tools

```shell
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.9.4.tgz
tar -xzvf mongodb-database-tools-ubuntu2204-x86_64-100.9.4.tgz
mv mongodb-database-tools-ubuntu2204-x86_64-100.9.4 mongodb-database-tools
sudo cp mongodb-database-tools/bin/* /usr/local/bin/
rm mongodb-database-tools-ubuntu2204-x86_64-100.9.4.tgz
```

### 2. Ajouter les données brutes dans une base MongoDB

Premièrement, nous dézipons le jeu de données `games_ratings.zip` fourni via l'instruction ci-dessous :

```shell
unzip data/games_ratings.zip -d data
```

Pour insérer en base les données du fichier `data/Video_Games_5.json` ainsi obtenu, nous utilisons ensuite la commande `mongoimport` qui suit : 

```shell
mongoimport \
    --uri  mongodb+srv://lgdfrancoisdocs:nedNBi2zKbRX2CdO@videogame.6yuwuvp.mongodb.net/videogame \
    --collection  video_games \
    --type JSON \
    --file data/Video_Games_5.json
```

Dans notre configuration (machine locale et Atlas en version d'essai), le transfert a mis **au total 2 heures** à se réaliser. 

### 3. Créer la base de données SQL avec le schéma associé

Nous choisissons d'installer une base PostgreSQL en local. Pour cela, nous installons `postgresql` puis démarrons le service dédié.

```shell
sudo apt-get install postgresql
sudo service postgresql start
```

La création de la base se fait ensuite via les commandes suivantes.

```shell
sudo -u postgres psql
```

```sql
CREATE DATABASE videogame_ratings;
```

Enfin, la table destinée à stocker les jeux les mieux notés sur les 6 derniers mois, que nous nommerons `6m_best_rated`, est créée via

```sql
CREATE TABLE "6m_best_rated" (
    date_run DATE,
    asin VARCHAR(20),
    mean_rate DECIMAL(3, 2),
    nb_users INTEGER,
    most_recent_rate DECIMAL(3, 2),
    oldest_rate DECIMAL(3, 2),
    CONSTRAINT pk_best_rated PRIMARY KEY (date_run, asin)
);
```
, où les champs définissent :
- pour **date_run** : la date à laquelle le ranking sur les 6 derniers mois a été calculé
- pour **asin** : l'identifiant du jeu vidéo
- pour **mean_rate** : la note moyenne
- pour **nb_users** : le nombre d'utilisateurs ayant noté le jeu
- pour **most_recent_rate** : la note la plus récente
- pour **oldest_rate** : la note la plus ancienne (sur les 6 derniers mois)

Un même jeu vidéo ne peut au maximum être ranké qu'une seule fois pour un même jour, **d'où la contrainte de clé primaire** définie dans la table. 


### 4. Développer le script Python du pipeline ETL

Le script du pipeline ETL est `./src/update_6m_best_rated.py`. Il est paramétré par l'argument `--DATE_RUN` qui définit la date pour laquelle l'exécution doit être faite. C'est à partir de cette date que sont récupérés les 6 mois qui précendent. Sa valeur par défaut est la date du jour du système lors de l'exécution. 

On notera que ce script utilise deux fichiers annexes, à personnaliser en fonction de sa situation :
* `./config/cfg.ini` pour la configuration des accès à la base MongoDB et la base Postgresql de destination (hôte, port, etc.)
* `./config/secrets.ini` pour les noms d'utilisateur et les mots de passe.

### 5. Automatiser le pipeline avec un outil de planification

Airflow ayant été installé section 1.1 avec `./venv`, il nous reste à définir la variable d'environnement associée,

```shell
export AIRFLOW_HOME=~/Devs/Projet-Data-Engineering-Pipeline-MongoDB/airflow
echo '~/Devs/Projet-Data-Engineering-Pipeline-MongoDB/airflow' >> ~/.bashrc
```

puis, à l'initialiser dans sa version standalone (ici par simplicité, cette version n'étant en effet pas recommendée en opérationnel).

```shell
airflow db reset
airflow db init
airflow standalone
```

Le dag qui définit la tâche quotidienne est `./airflow/dags/daily_update_dag.py`. Elle exécute le script `./src/update_6m_best_rated.py` via un BashOperator.

Si on fait un backfilling d'une date passée quelconque en entrant

```shell
airflow dags backfill -s 2015-04-22 -e 2015-04-23 daily_update
```

, on peut ensuite constater que le job Airflow a bien fonctionné

![alt text](./docs/capture_airflow.png) 

, et que les enregistrements ont bien été créé dans la table `"6m_best_rated"`

![alt text](./docs/capture_database.png) 


Tout semble donc **prêt pour que la tâche quotidienne serve en opérationnel** à alimenter la page d'accueil et les campagnes de commmunication.