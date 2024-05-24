from abc import ABC, abstractmethod
from datetime import datetime
import re
import logging
import time

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
        anomalies = []

        # Validation de l'ID du compteur
        if not re.match(r'^\d{6}$', str(donnees['compteur_id'])):
            erreurs.append("ID de compteur invalide")
            anomalies.append("Tentative d'injection avec ID de compteur")

        # Validation de l'horodatage
        try:
            datetime.fromisoformat(donnees['timestamp'])
        except ValueError:
            erreurs.append("Horodatage invalide")
            anomalies.append("Tentative d'injection avec horodatage")

        # Validation de la consommation
        try:
            consommation = float(donnees['consommation'].split()[0])
            if consommation < 0 or consommation > 10000:
                erreurs.append("Valeur de consommation invalide")
        except ValueError:
            erreurs.append("Valeur de consommation invalide")
            anomalies.append("Tentative d'injection avec consommation")

        # Validation du type de client
        type_client = donnees['type_client'].lower()
        if type_client not in ['residentiel', 'commercial', 'industriel']:
            erreurs.append("Type de client invalide")
            anomalies.append("Tentative d'injection avec type de client")

        # Validation de la wilaya
        if not all(x.isalpha() or x.isspace() for x in donnees['wilaya']):
            erreurs.append("Wilaya invalide")
            anomalies.append("Tentative d'injection avec wilaya")

        # Validation de la ville
        if not all(x.isalpha() or x.isspace() or x in ['"', "'"] for x in donnees['ville']):
            erreurs.append("Ville invalide")
            anomalies.append("Tentative d'injection avec ville")

        # Validation de la localisation (latitude, longitude)
        localisation_pattern = r'^-?\d+\.\d+,-?\d+\.\d+$'
        if not re.match(localisation_pattern, donnees['localisation']):
            erreurs.append("Localisation invalide")
            anomalies.append("Tentative d'injection avec localisation")

        # Validation de la région
        if not donnees['region'].isalpha():
            erreurs.append("Région invalide")
            anomalies.append("Tentative d'injection avec région")

        # Validation du code postal
        if not re.match(r'^\d{5}$', str(donnees['code_postal'])):
            erreurs.append("Code postal invalide")
            anomalies.append("Tentative d'injection avec code postal")

        # Validation du fournisseur
        if not all(x.isalnum() or x.isspace() for x in donnees['fournisseur']):
            erreurs.append("Fournisseur invalide")
            anomalies.append("Tentative d'injection avec fournisseur")

        # Validation du tarif
        try:
            tarif = float(donnees['tarif'])
            if tarif <= 0:
                erreurs.append("Tarif invalide")
        except ValueError:
            erreurs.append("Tarif invalide")
            anomalies.append("Tentative d'injection avec tarif")

        # Validation de la puissance souscrite
        try:
            puissance_souscrite = float(donnees['puissance_souscrite'].split()[0])
            if puissance_souscrite <= 0:
                erreurs.append("Puissance souscrite invalide")
        except ValueError:
            erreurs.append("Puissance souscrite invalide")
            anomalies.append("Tentative d'injection avec puissance souscrite")

        # Validation du type de compteur
        if not all(x.isalnum() or x.isspace() for x in donnees['type_compteur']):
            erreurs.append("Type de compteur invalide")
            anomalies.append("Tentative d'injection avec type de compteur")

        return erreurs, anomalies


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


class FiltreSecurite(Filtre):
    """Filtre combiné pour la sécurité"""
    def __init__(self):
        self.traffic_count = 0
        self.start_time = time.time()

    def traiter(self, donnees):
        # Détection d'attaques DoS
        self.traffic_count += 1
        current_time = time.time()
        elapsed_time = current_time - self.start_time

        if elapsed_time < 60:  # Surveillance sur une période de 60 secondes
            if self.traffic_count > 1000:  # Seuil de trafic suspect
                logging.warning("Attaque DoS détectée : trafic anormalement élevé")
        else:
            self.traffic_count = 0
            self.start_time = current_time

        # Détection de retard de message
        timestamp_envoi = datetime.fromisoformat(donnees['timestamp'])
        timestamp_reception = datetime.now()
        delai_transmission = (timestamp_reception - timestamp_envoi).total_seconds()

        if delai_transmission > 10:  # Seuil de retard suspect
            logging.warning(f"Retard de message détecté : {delai_transmission} secondes")

        # Limitation de l'exposition
        donnees['compteur_id'] = 'XXXXXX'
        donnees['fournisseur'] = 'XXXXXX'
        donnees['type_compteur'] = 'XXXXXX'

        return donnees


class Pipeline:
    """Pipeline de filtres"""
    def __init__(self, filtres):
        self.filtres = filtres

    def traiter(self, donnees):
        erreurs = []
        anomalies = []
        for filtre in self.filtres:
            if isinstance(filtre, FiltreValidation):
                result, anomaly = filtre.traiter(donnees)
                if isinstance(result, list):  # Si le résultat est une liste d'erreurs
                    erreurs.extend(result)
                if isinstance(anomaly, list):  # Si le résultat est une liste d'anomalies
                    anomalies.extend(anomaly)
            else:
                donnees = filtre.traiter(donnees)

        if anomalies:
            for anomalie in anomalies:
                logging.warning(f"Anomalie détectée : {anomalie}")

        if erreurs:
            for erreur in erreurs:
                print("-", erreur)
            return None

        return donnees


# Configuration du logging pour afficher les avertissements dans la console
logging.basicConfig(level=logging.WARNING)

# Création des filtres
filtre_validation = FiltreValidation()
filtre_normalisation = FiltreNormalisation()
filtre_transformation = FiltreTransformation()
filtre_securite = FiltreSecurite()

# Création du pipeline avec les filtres
pipeline = Pipeline([filtre_validation, filtre_normalisation, filtre_transformation, filtre_securite])

# Exemple de données
exemple_donnees = {
    'compteur_id': '123456',
    'timestamp': '2024-05-24T12:00:00',
    'consommation': '5000 Wh',
    'type_client': 'residentiel',
    'wilaya': 'Alger',
    'ville': 'Alger',
    'localisation': '36.7538,3.0588',
    'region': 'Centre',
    'code_postal': '16000',
    'fournisseur': 'Electricite Algerie',
    'tarif': '0.20',
    'puissance_souscrite': '10 kW',
    'type_compteur': 'Electronique'
}

# Traitement des données par le pipeline
resultat = pipeline.traiter(exemple_donnees)

# Affichage du résultat final
if resultat is not None:
    print("Données traitées avec succès :")
    for cle, valeur in resultat.items():
        print(f"{cle}: {valeur}")
else:
    print("Échec du traitement des données.")
