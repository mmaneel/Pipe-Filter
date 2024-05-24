import pandas as pd
from Pipe_filter import Pipeline,FiltreValidation,FiltreNormalisation,FiltreTransformation

# Lire les données CSV
df = pd.read_csv('Pipe-Filter/dataset_consommation_energie_algerie.csv')

# Convertir le DataFrame en une liste de dictionnaires
donnees_brutes = df.to_dict(orient='records')

# Diviser les données en deux parties
mid_index = len(donnees_brutes) // 2
donnees_part1 = donnees_brutes[:mid_index]
donnees_part2 = donnees_brutes[mid_index:]

# Créer deux instances du pipeline
pipeline1 = Pipeline([
    FiltreValidation(),
    FiltreNormalisation(),
    FiltreTransformation()
])

pipeline2 = Pipeline([
    FiltreValidation(),
    FiltreNormalisation(),
    FiltreTransformation()
])

# Traiter chaque partie des données brutes avec un pipeline distinct
print("part1",donnees_part1)
donnees_traitees_part1 = [pipeline1.traiter(donnees) for donnees in donnees_part1]

# print("part2",donnees_part2)
donnees_traitees_part2 = [pipeline2.traiter(donnees) for donnees in donnees_part2]

# Combiner les deux parties traitées
donnees_traitees = donnees_traitees_part1 + donnees_traitees_part2

# Filtrer les résultats pour enlever les entrées traitées sans erreurs (None)
donnees_traitees = [donnees for donnees in donnees_traitees if donnees is not None]

# Convertir les données traitées en DataFrame
df_traite = pd.DataFrame(donnees_traitees)

# Sauvegarder le DataFrame traité en CSV
df_traite.to_csv('Pipe-Filter/dataset_consommation_energie_algerie_traite.csv', index=False)
