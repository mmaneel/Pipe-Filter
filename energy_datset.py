import pandas as pd
import random
from datetime import datetime, timedelta

# Définir les attributs
compteur_ids = [123456, 789012, 345678, 654321, 987654]
types_client = ['Residentiel', 'Commercial', 'Industriel']
localisations = [
    {'wilaya': 'Alger', 'villes': [
        {'ville': 'Oued Smar', 'coord': (36.7000, 3.1833), 'code_postal': 16270},
        {'ville': 'Bab Ezzouar', 'coord': (36.7268, 3.1829), 'code_postal': 16024},
        {'ville': 'El Harrach', 'coord': (36.7146, 3.1319), 'code_postal': 16200}
    ]},
    {'wilaya': 'Oran', 'villes': [
        {'ville': 'Bir El Djir', 'coord': (35.7106, -0.5625), 'code_postal': 31130},
        {'ville': 'Es Sénia', 'coord': (35.6475, -0.6231), 'code_postal': 31100},
        {'ville': 'Aïn Turk', 'coord': (35.7435, -0.7695), 'code_postal': 31300}
    ]},
    {'wilaya': 'Biskra', 'villes': [
        {'ville': 'El Hadjeb', 'coord': (34.8511, 5.7416), 'code_postal': 75006},
        {'ville': 'Sidi Khaled', 'coord': (34.4264, 4.9431), 'code_postal': 75300},
        {'ville': 'M’Chouneche', 'coord': (34.9639, 6.0822), 'code_postal': 75008}
    ]},
    # Ajouter d'autres wilayas et villes si nécessaire
]
regions = ['Nord', 'Ouest', 'Sud', 'Est', 'Centre']
fournisseurs = ['Sonelgaz', 'Power Algeria', 'Energy Plus']
tarifs = [0.12, 0.14, 0.16]
puissances_souscrites = [3, 6, 9, 12, 15]
types_compteur = ['Smart Meter Gen 1', 'Smart Meter Gen 2', 'Smart Meter Gen 3']

# Fonction pour générer des données aléatoires
def generate_data(num_entries):
    data = []
    timestamp = datetime.now()
    for _ in range(num_entries):
        compteur_id = random.choice(compteur_ids)
        timestamp += timedelta(minutes=15)
        consommation = round(random.uniform(0.1, 100), 3)
        type_client = random.choice(types_client)
        localisation = random.choice(localisations)
        ville_info = random.choice(localisation['villes'])
        wilaya = localisation['wilaya']
        ville = ville_info['ville']
        coord = ville_info['coord']
        code_postal = ville_info['code_postal']
        region = random.choice(regions)
        fournisseur = random.choice(fournisseurs)
        tarif = random.choice(tarifs)
        puissance = f"{random.choice(puissances_souscrites)} kW"
        type_compteur = random.choice(types_compteur)

        if random.random() < 0.1:  # 10% de chance pour chaque entrée
            data.append({
                'compteur_id': 999999,  # ID spécifique pour l'exemple
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'consommation': f"500 Wh",  # Exemple nécessitant une conversion en kWh
                'type_client': 'residentiel',  # Exemple nécessitant une capitalisation
                'wilaya': 'alger',  # Exemple nécessitant une capitalisation
                'ville': 'alger',  # Exemple nécessitant une capitalisation
                'localisation': f"{36.7538},{3.0588}",
                'region': 'Nord',
                'code_postal': '16000',
                'fournisseur': 'Énergie Plus ',  # Exemple nécessitant une suppression d'espace
                'tarif': 0.12,
                'puissance_souscrite': '5 kW',
                'type_compteur': 'Smart Meter Gen 3'
            })
        
        data.append({
            'compteur_id': compteur_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'consommation': f"{consommation} kWh",
            'type_client': type_client,
            'wilaya': wilaya,
            'ville': ville,
            'localisation': f"{coord[0]},{coord[1]}",
            'region': region,
            'code_postal': code_postal,
            'fournisseur': fournisseur,
            'tarif': tarif,
            'puissance_souscrite': puissance,
            'type_compteur': type_compteur
        })
    return data

# Générer le dataset
num_entries = 100  # Nombre de lignes de données
data = generate_data(num_entries)

# Créer un DataFrame
df = pd.DataFrame(data)

# Sauvegarder le dataset en CSV
df.to_csv('Pipe-Filter/dataset_consommation_energie_algerie.csv', index=False)

print("Dataset généré et sauvegardé sous 'dataset_consommation_energie_algerie.csv'")
