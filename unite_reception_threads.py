import pandas as pd
import numpy as np
from threading import Thread
from queue import Queue
from Pipe_filter import Pipeline, FiltreValidation, FiltreNormalisation, FiltreTransformation

# Fonction pour traiter une partie des données avec un pipeline
def traiter_partie(pipeline, donnees_part, output_queue):
    output_queue.put([pipeline.traiter(donnees) for donnees in donnees_part])

# Lire les données CSV
df = pd.read_csv('dataset_consommation_energie_algerie.csv')

# Convertir le DataFrame en une liste de dictionnaires
donnees_brutes = df.to_dict(orient='records')

# Diviser les données en deux parties de taille égale
donnees_parts = np.array_split(donnees_brutes, 2)
donnees_part1, donnees_part2 = donnees_parts

# Créer deux instances du pipeline
pipeline1 = Pipeline([FiltreValidation(), FiltreNormalisation(), FiltreTransformation()])
pipeline2 = Pipeline([FiltreValidation(), FiltreNormalisation(), FiltreTransformation()])

# Créer les queues pour stocker les résultats
queue1 = Queue()
queue2 = Queue()

# Créer les threads pour traiter chaque partie des données

thread1 = Thread(target=traiter_partie, args=(pipeline1, donnees_part1, queue1))
thread2 = Thread(target=traiter_partie, args=(pipeline2, donnees_part2, queue2))

# Démarrer les threads
thread1.start()
thread2.start()

# Attendre la fin de l'exécution des threads
thread1.join()
thread2.join()

# Récupérer les résultats des queues
donnees_traitees_part1 = queue1.get()
donnees_traitees_part2 = queue2.get()

# Combiner les deux parties traitées
donnees_traitees = donnees_traitees_part1 + donnees_traitees_part2

# Filtrer les résultats pour enlever les entrées traitées sans erreurs (None)
donnees_traitees = [donnees for donnees in donnees_traitees if donnees is not None]

# Convertir les données traitées en DataFrame
df_traite = pd.DataFrame(donnees_traitees)

# Sauvegarder le DataFrame traité en CSV
df_traite.to_csv('dataset_consommation_energie_algerie_traite.csv', index=False)
