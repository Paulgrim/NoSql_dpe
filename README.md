# NF26 ― P2021 ― Projet

Le csv est disponible [ici](https://data.ademe.fr/datasets/dpe-france) :

Définition des différentes variables [ici](https://koumoul.com/s/data-fair/api/v1/datasets/dpe-france/metadata-attachments/ADEME%20-%20DPE%20-%20Dictionnaire%20de%20donn%C3%A9es%20-%202020-06-08.pdf)


## Structuration du dépôt

 - un dossier `project` pour votre code.
 - un `Pipfile` et son `Pipfile.lock`, contenant une liste des dépendances pour créer un environnement virtuel grâce à `make init`.
 - un `Makefile` simple avec quelques cibles pour le style de votre code.
 - un setup de `pre-commit` simple (`.pre-commit-config.yaml`).
- un dossier `data` où les différents csv et pdf seront générés

## Documentation pour reproductibilité de votre projet

Pour reproduire mes travaux vous pouvez réaliser les étapes ci-dessous :

 - Se connecter au serveur `nf26-1.leger.tf`.
 - Se rendre dans le dossier `/home/grimalpa/td-nf26-project-grimalpa`.
 - Vérifier qu'un dossier `data` avec le csv dpe-68.csv téléchargeable au lien suivant https://files.data.gouv.fr/ademe/dpe-departements/68.zip est bien présent
 - Lancer `make init` pour setup l'environnement de travail
 - Lancer `make init-keyspace` pour créer les tables dans cassandra si vous voulez supprimer les tables et les recréer
 - Lancer `make insert` pour insérer les données dans les tables si vous avez fait la commande précédente
-  Lancer `make corr` pour visualiser la corrélation des différents attributs en fonction de la consommation d'énergie et les 5 logements les plus économes en énergie.
- Lancer `make densite` pour générer différents csv avec la densite et les geoJson associé. De plus vous pourrez observer la corrélation entre production d'énergie et densité.
- Lancer `make econome` pour afficher les 5 logements les plus économes et génerer le csvs pour visualiser les logements les plus économes sur la carte interactive du notebook et pour générer différents csv utile pour générer les graphiques avec la commande `make plot`(voir plus bas).
- Une fois les différents csv générés, vous pourrez utiliser le notebook `visualisation.py` pour visualiser les cartes interactives et ainsi observer la densité de logement sur deux cartes (pour 25km² et pour 100km²).
- Lancer `make plot` pour créer des pdf des distributions des logements selon type et classe énergétique

### Exemple d'affichage disponible avec `visualisation.py`:

![C25](data\C25.PNG?raw=true "Densité de logements pour 25km2")
![alt text](https://github.com/Paulgrim/NoSql_dpe/blob/main/data/C25.PNG?raw=true)

![C100](data\C100.PNG?raw=true ""Densité de logements pour 100km2")


