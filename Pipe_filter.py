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

        # Validation de l'ID du compteur
        if not re.match(r'^\d{6}$', str(donnees['compteur_id'])):
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
        if not all(x.isalpha() or x.isspace() or x in ['"', "'"] for x in donnees['ville']):
            erreurs.append("Ville invalide")

        # Validation de la localisation (latitude, longitude)
        localisation_pattern = r'^-?\d+\.\d+,-?\d+\.\d+$'
        if not re.match(localisation_pattern, donnees['localisation']):
            erreurs.append("Localisation invalide")

        # Validation de la région
        if not donnees['region'].isalpha():
            erreurs.append("Région invalide")

        # Validation du code postal
        if not re.match(r'^\d{5}$', str(donnees['code_postal'])):
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
            if self.traffic_count > 100:  # Seuil de trafic suspect
                logging.warning("Attaque DoS detectee : trafic anormalement eleve")
        else:
            self.traffic_count = 0
            self.start_time = current_time

        # Détection de retard de message
        timestamp_envoi = datetime.fromisoformat(donnees['timestamp'])
        timestamp_reception = datetime.now()
        delai_transmission = (timestamp_reception - timestamp_envoi).total_seconds()

        if delai_transmission > 1000:  # Seuil de retard suspect
            logging.warning(f"Retard de message detecte : {delai_transmission} secondes")

        # Limitation de l'exposition
        # donnees['compteur_id'] = 'XXXXXX'
        # donnees['fournisseur'] = 'XXXXXX'
        # donnees['type_compteur'] = 'XXXXXX'

        return donnees
    

class Pipeline:
    """Pipeline de filtres"""
    def __init__(self, filtres):
        self.filtres = filtres

    def traiter(self, donnees):
        erreurs = []
        for filtre in self.filtres:
            if filtre.__class__.__name__ == "FiltreValidation":
                result = filtre.traiter(donnees)
                if isinstance(result, list):  # Si le résultat est une liste d'erreurs
                    if result == [] : 
                        pass 
                    else : 
                        erreurs.extend(result)
                        for erreur in erreurs:
                            print("-", erreur)
                        return None
            else:
                donnees= filtre.traiter(donnees)
        return donnees
    
        
# Configuration du logging pour afficher les avertissements dans la console
logging.basicConfig(level=logging.WARNING)

