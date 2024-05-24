from abc import ABC, abstractmethod
from datetime import datetime
import re
import pandas as pd

class Filtre(ABC):
    """Interface de base pour les filtres"""
    @abstractmethod
    def traiter(self, donnees):
        """Méthode à implémenter pour traiter les données"""
        pass

class FiltreValidation(Filtre):
    """Filtre de validation des données"""
    def traiter(self, donnees):
        # Validation de l'ID du compteur
        if not re.match(r'^\d{6}$', str(donnees['compteur_id'])):
            raise ValueError("ID de compteur invalide")

        # Validation de l'horodatage
        try:
            datetime.fromisoformat(donnees['timestamp'])
        except ValueError:
            raise ValueError("Horodatage invalide")

        # Validation de la consommation
        consommation = float(donnees['consommation'].split()[0])
        if consommation < 0 or consommation > 10000:
            raise ValueError("Valeur de consommation invalide")

        # Validation du type de client
        type_client = donnees['type_client'].lower()
        if type_client not in ['residentiel', 'commercial', 'industriel']:
            raise ValueError("Type de client invalide")

        # Validation du code postal
        if not re.match(r'^\d{5}$', str(donnees['code_postal'])):
            raise ValueError("Code postal invalide")

        # Validation de la puissance souscrite
        puissance_souscrite = float(donnees['puissance_souscrite'].split()[0])
        if puissance_souscrite <= 0:
            raise ValueError("Puissance souscrite invalide")

        return donnees

class FiltreNormalisation(Filtre):
    """Filtre de normalisation des données"""
    def traiter(self, donnees):
        # Conversion de l'unité de consommation en kWh
        consommation, unite = donnees['consommation'].split()
        consommation = float(consommation)
        if unite == 'Wh':
            consommation /= 1000
        donnees['consommation'] = f"{consommation:.5f} kWh"

        # Normalisation du type de client
        donnees['type_client'] = donnees['type_client'].capitalize()

        # Normalisation de la wilaya et de la ville
        donnees['wilaya'] = donnees['wilaya'].upper()
        donnees['ville'] = donnees['ville'].upper()

        # Normalisation du fournisseur
        donnees['fournisseur'] = donnees['fournisseur'].replace(' ', '_').upper()

        # Conversion de la puissance souscrite en float
        puissance_souscrite = float(donnees['puissance_souscrite'].split()[0])
        donnees['puissance_souscrite'] = puissance_souscrite

        # Conversion du code postal en int
        donnees['code_postal'] = int(donnees['code_postal'])

        # Conversion du tarif en float
        donnees['tarif'] = float(donnees['tarif'])

        return donnees


"""
Puisque les données sont reçues 3 fois par jour, 
il serait plus pertinent de calculer la consommation quotidienne en multipliant la consommation par 8 
(8 heures entre chaque lecture) au lieu de 24. 
"""


class FiltreTransformation(Filtre):
    """Filtre de transformation des données"""
    def traiter(self, donnees):
        # Calcul de la consommation pour 8 heures
        consommation_kwh = float(donnees['consommation'].split()[0])
        consommation_8h = consommation_kwh * 8
        donnees['consommation_8h'] = f"{consommation_8h:.5f} kWh"

        # Catégorisation des clients selon la consommation
        if consommation_kwh < 1.67:
            donnees['categorie_client'] = 'Faible consommation'
        elif consommation_kwh < 6.67:
            donnees['categorie_client'] = 'Consommation moyenne'
        else:
            donnees['categorie_client'] = 'Forte consommation'

        # Ajout du ratio consommation/puissance souscrite
        puissance_souscrite = donnees['puissance_souscrite']
        ratio = consommation_kwh / puissance_souscrite
        donnees['ratio_consommation'] = ratio

        return donnees

class Pipeline:
    """Pipeline de filtres"""
    def __init__(self, filtres):
        self.filtres = filtres

    def traiter(self, donnees):
        for filtre in self.filtres:
            donnees = filtre.traiter(donnees)
        return donnees


# Lire les données CSV
df = pd.read_csv('Pipe-Filter/dataset_consommation_energie_algerie.csv')

# Convertir le DataFrame en une liste de dictionnaires
donnees_brutes = df.to_dict(orient='records')

# Créer une instance du pipeline
pipeline = Pipeline([
    FiltreValidation(),
    FiltreNormalisation(),
    FiltreTransformation()
])

# Traiter chaque donnée brute
donnees_traitees = [pipeline.traiter(donnees) for donnees in donnees_brutes]

# Convertir les données traitées en DataFrame
df_traite = pd.DataFrame(donnees_traitees)

# Sauvegarder le DataFrame traité en CSV
df_traite.to_csv('Pipe-Filter/dataset_consommation_energie_algerie_traite.csv', index=False)
