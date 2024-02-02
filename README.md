# Large Scale Distributed Systems
## Projet Hadoop Map Reduce

---
Antoine BUQUET

---

## Objectif
Obtenir une liste triée (de manière croissante) des films qui ont été le plus de fois le film préféré d'un utilisateur.

## Étapes préliminaires
1. Télécharger le zip `ml-25m` sur le site de MovieLens : https://grouplens.org/datasets/movielens/
2. Extraire le zip dans le dossier `data` à la racine du projet
3. Modifier le chemin du projet dans le service `namenode` et `datanode` du fichier `docker-compose.yml` pour y avoir accès depuis les containers :
```yaml
services:
  namenode:
    ...
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
      - C:\Users\Antoine\Desktop\Programmation\Ecole\5A_Polytech\Hadoop-MR-Project:/hadoop/labs # Cette ligne
    ...

  datanode:
    ...
    volumes:
      - hadoop_datanode:/hadoop/dfs/data
      - C:\Users\Antoine\Desktop\Programmation\Ecole\5A_Polytech\Hadoop-MR-Project:/hadoop/labs # Et cette ligne
    ...
```


## Lancement du projet

### À la racine du projet
Compilation du projet et lancement des containers
```bash
mvn clean package
docker compose up -d
```

### Dans le container Datanode
Création du dossier `input` et copie des fichiers `movies.csv` et `ratings.csv` dans HDFS
```bash
hdfs dfs -mkdir /input
hdfs dfs -copyFromLocal hadoop/labs/data/ml-25m/movies.csv /input
hdfs dfs -copyFromLocal hadoop/labs/data/ml-25m/ratings.csv /input
```

### Dans le container Namenode
Éxecution du projet et récupération des résultats
```bash
## Étape 1
hadoop jar hadoop/labs/target/Hadoop_MR_Project-1.0-SNAPSHOT.jar fr.polytech.hadoop.step1.BestMoviePerUserId
hdfs dfs -copyToLocal /output/bestMovieIdPerUserId/part-r-00000 hadoop/labs/data/output/bestMovieIdPerUserId.txt
hdfs dfs -copyToLocal /output/bestMoviePerUserId/part-r-00000 hadoop/labs/data/output/bestMoviePerUserId.txt

## Étape 2
hadoop jar hadoop/labs/target/Hadoop_MR_Project-1.0-SNAPSHOT.jar fr.polytech.hadoop.step2.MostLikedMovies
hdfs dfs -copyToLocal /output/movieCount/part-r-00000 hadoop/labs/data/output/movieCount.txt
hdfs dfs -copyToLocal /output/mostLikedMovies/part-r-00000 hadoop/labs/data/output/mostLikedMovies.txt
```

Les résultats sont disponibles dans le dossier `data/output` du projet.