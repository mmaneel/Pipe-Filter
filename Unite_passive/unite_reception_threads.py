import os
import pandas as pd
import numpy as np
from threading import Thread
from queue import Queue
from Pipe_filter import Pipeline, FiltreValidation, FiltreNormalisation, FiltreTransformation
import time

# Fonction pour traiter une partie des données avec un pipeline
def traiter_partie(pipeline, donnees_part, output_queue):
    output_queue.put([pipeline.traiter(donnees) for donnees in donnees_part])


# Check if the file 'donnees_apres_etape_1_FiltreValidation.csv' exists and run 'split.py' if it does



i = 0
# Vérifier si le fichier dataset.csv existe dans le répertoire courant
while True : 
    if os.path.exists('dataset.csv'):
        print("test")
        # Lire les données CSV
        df = pd.read_csv('dataset.csv')

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

        # Test
        # Sauvegarder le DataFrame traité en CSV
        df_traite.to_csv('dataset_traite.csv', index=False)
        print("saved")
        

        if os.path.exists('donnees_apres_etape_1_FiltreValidation.csv') :
            if os.path.exists('donnees_apres_etape_1_FiltreValidation.csv'):
                print("split")
                exit_code = os.system('python split.py')
                if exit_code != 0:
                    print("Failed to run split.py")
                else:
                    print("split.py executed successfully")

                    # Check for the second file with a timeout of 10 seconds
                    timeout = 0.1
                    file_found = False
                    start_time = time.time()

                    while time.time() - start_time < timeout:
                        if os.path.exists('donnees_apres_etape_2_FiltreNormalisation.csv'):
                            file_found = True
                            break
                        time.sleep(1)  # Sleep for 1 second before checking again

                    # start from second filter 
                    # Lire les données CSV
                    if file_found:
                        print("Second file 'donnees_apres_etape_2_FiltreNormalisation.csv' found.")
                        i = 1
                    else:
                        print("Second file 'donnees_apres_etape_2_FiltreNormalisation.csv' not found within 10 seconds == panne")
                        df = pd.read_csv('donnees_apres_etape_1_FiltreValidation.csv')

                        # Convertir le DataFrame en une liste de dictionnaires
                        donnees_brutes = df.to_dict(orient='records')

                        # Diviser les données en deux parties de taille égale
                        donnees_parts = np.array_split(donnees_brutes, 2)
                        donnees_part1, donnees_part2 = donnees_parts

                        # Créer deux instances du pipeline
                        pipeline1 = Pipeline([FiltreNormalisation(), FiltreTransformation()])
                        pipeline2 = Pipeline([FiltreNormalisation(), FiltreTransformation()])

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

                        # Test
                        # Sauvegarder le DataFrame traité en CSV
                        df_traite.to_csv('dataset_traite_active.csv', index=False)
                    
                        # os.remove('donnees_apres_etape_1_FiltreValidation.csv')
                        break 

        
        if os.path.exists('donnees_apres_etape_2_FiltreNormalisation.csv'):
            # start from third filter 
            # Check for the second file with a timeout of 10 seconds
            timeout = 0.1
            file_found = False
            start_time = time.time()

            while time.time() - start_time < timeout:
                if os.path.exists('donnees_apres_etape_3_FiltreTransformation.csv'):
                    file_found = True
                    break
                time.sleep(1)  # Sleep for 1 second before checking again

            # start from second filter 
            # Lire les données CSV
            if file_found:
                print("Second file 'donnees_apres_etape_3_FiltreTransformation.csv' found.")
                i = 2
            else:
                print("Second file 'donnees_apres_etape_3_FiltreTransformation.csv' not found within 10 seconds == panne")
                # Lire les données CSV
                df = pd.read_csv('donnees_apres_etape_2_FiltreNormalisation.csv')

                # Convertir le DataFrame en une liste de dictionnaires
                donnees_brutes = df.to_dict(orient='records')

                # Diviser les données en deux parties de taille égale
                donnees_parts = np.array_split(donnees_brutes, 2)
                donnees_part1, donnees_part2 = donnees_parts

                # Créer deux instances du pipeline
                pipeline1 = Pipeline([FiltreTransformation()])
                pipeline2 = Pipeline([FiltreTransformation()])

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

                # Test
                # Sauvegarder le DataFrame traité en CSV
                df_traite.to_csv('dataset_traite_active.csv', index=False)
            
                # os.remove('donnees_apres_etape_2_FiltreNormalisation.csv')
                break 

        if os.path.exists('donnees_apres_etape_3_FiltreTransformation.csv'):
            # send it to unite de stockage 
            # Check for the second file with a timeout of 10 seconds
            timeout = 0.1
            file_found = False
            start_time = time.time()

            while time.time() - start_time < timeout:
                if os.path.exists('donnees_apres_etape_1_FiltreValidation.csv'):
                    file_found = True
                    break
                time.sleep(1)  # Sleep for 1 second before checking again

            # start from second filter 
            # Lire les données CSV
            if file_found:
                print("Second file 'donnees_apres_etape_1_FiltreValidation.csv' found.")
                i = 3 
            else:
                print("Second file 'donnees_apres_etape_1_FiltreValidation.csv' not found within 10 seconds == panne")
                print("save the output to 'dataset_traite_active.csv'")
                df = pd.read_csv('donnees_apres_etape_3_FiltreTransformation.csv')
                df.to_csv('dataset_traite_active.csv', index=False)
            
                # os.remove('donnees_apres_etape_3_FiltreTransformation.csv')
                break 

    # if(i==3): 
    #     time.sleep(10)
    #     os.remove('donnees_apres_etape_1_FiltreValidation.csv')
    #     os.remove('donnees_apres_etape_2_FiltreNormalisation.csv')
    #     os.remove('donnees_apres_etape_3_FiltreTransformation.csv')

        


        
