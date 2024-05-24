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
        erreurs = []

        # Validation de l'ID du compteur
        if not re.match(r'^\d{6}$', donnees['compteur_id']):
            erreurs.append("ID de compteur invalide")

        # Validation de l'horodatage
        try:
            datetime.fromisoformat(donnees['timestamp'])
        except ValueError:
            erreurs.append("Horodatage invalide")

        # Validation de la consommation
        consommation = float(donnees['consommation'].split()[0])
        if consommation < 0 or consommation > 10000:
            erreurs.append("Valeur de consommation invalide")

        # Validation du type de client
        type_client = donnees['type_client'].lower()
        if type_client not in ['residentiel', 'commercial', 'industriel']:
            erreurs.append("Type de client invalide")

        # Validation de la wilaya
        if not donnees['wilaya'].isalpha():
            erreurs.append("Wilaya invalide")

        # Validation de la ville
        if not donnees['ville'].isalpha():
            erreurs.append("Ville invalide")

        # Validation de la localisation (latitude, longitude)
        localisation_pattern = r'^\d{1,3}\.\d{4,},\d{1,3}\.\d{4,}$'
        if not re.match(localisation_pattern, donnees['localisation']):
            erreurs.append("Localisation invalide")

        # Validation de la région
        if not donnees['region'].isalpha():
            erreurs.append("Région invalide")

        # Validation du code postal
        if not re.match(r'^\d{5}$', donnees['code_postal']):
            erreurs.append("Code postal invalide")

        # Validation du fournisseur
        if not all(x.isalnum() or x.isspace() for x in donnees['fournisseur']):
            erreurs.append("Fournisseur invalide")

        # Validation du tarif
        try:
            tarif = float(donnees['tarif'])
            if tarif <= 0:
                erreurs.append("Tarif invalide")
        except ValueError:
            erreurs.append("Tarif invalide")

        # Validation de la puissance souscrite
        puissance_souscrite = float(donnees['puissance_souscrite'].split()[0])
        if puissance_souscrite <= 0:
            erreurs.append("Puissance souscrite invalide")

        # Validation du type de compteur
        if not all(x.isalnum() or x.isspace() for x in donnees['type_compteur']):
            erreurs.append("Type de compteur invalide")

        return erreurs

class Pipeline:
    """Pipeline de filtres"""
    def __init__(self, filtres):
        self.filtres = filtres

    def traiter(self, donnees):
        erreurs = []
        for filtre in self.filtres:
            result = filtre.traiter(donnees)
            if isinstance(result, list):  # Si le résultat est une liste d'erreurs
                erreurs.extend(result)  # Ajouter les erreurs à la liste
        return erreurs

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
])

erreurs = pipeline.traiter(donnees_brutes)
if erreurs:
    print("Erreurs de validation:")
    for erreur in erreurs:
        print("-", erreur)
else:
    print("Aucune erreur de validation détectée.")
