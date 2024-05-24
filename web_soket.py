import socket

# Définir l'adresse et le port du serveur
HOST = '192.168.43.215'  # Remplacer par l'adresse IP du serveur si nécessaire
PORT = 5000

# Créer un socket TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    # Se connecter au serveur
    client_socket.connect((HOST, PORT))

    # Recevoir la taille du fichier du serveur en premier
    taille_fichier_bytes = client_socket.recv(4)
    taille_fichier = int.from_bytes(taille_fichier_bytes, byteorder='big')
    # Recevoir le contenu du fichier depuis le serveur
    contenu = client_socket.recv(taille_fichier)

    # Nom du fichier de destination
    nom_fichier_destination = 'Classeur1Wail_recu.csv'

    # Écrire le contenu reçu dans un fichier
    with open(nom_fichier_destination, 'wb') as fichier:
        fichier.write(contenu)

    print(f"Fichier {nom_fichier_destination} reçu avec succès.")