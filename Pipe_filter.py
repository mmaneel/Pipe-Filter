from abc import ABC, abstractmethod
from datetime import datetime
import re

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
        if not re.match(r'^\d{6}$', donnees['compteur_id']):
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
        if not re.match(r'^\d{5}$', donnees['code_postal']):
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

        return donnees

class FiltreTransformation(Filtre):
    """Filtre de transformation des données"""
    def traiter(self, donnees):
        # Calcul de la consommation horaire
        consommation = float(donnees['consommation'].split()[0])
        # ... Autres transformations si nécessaire
        return donnees

class Pipeline:
    """Pipeline de filtres"""
    def __init__(self, filtres):
        self.filtres = filtres

    def traiter(self, donnees):
        for filtre in self.filtres:
            donnees = filtre.traiter(donnees)
        return donnees

# Exemple d'utilisation
donnees_brutes = {
    'compteur_id': '123456',
    'timestamp': '2023-05-24 08:00:00',
    'consommation': '1.234 kWh',
    'type_client': 'Residentiel',
    'wilaya': 'Alger',
    'ville': 'Alger',
    'localisation': '36.7538,3.0588',
    'region': 'Nord',
    'code_postal': '16000',
    'fournisseur': 'Énergie Plus',
    'tarif': '0.12',
    'puissance_souscrite': '5 kW',
    'type_compteur': 'Smart Meter Gen 3'
}

pipeline = Pipeline([
    FiltreValidation(),
    FiltreNormalisation(),
    FiltreTransformation()
])

donnees_traitees = pipeline.traiter(donnees_brutes)
print(donnees_traitees)