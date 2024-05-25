import socket

# Définir l'adresse et le port du serveur
HOST = '192.168.43.46'  # Adresse IP du serveur
PORT = 5000              # Port d'écoute du serveur

# Créer un socket TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    try:
        # Se connecter au serveur
        client_socket.connect((HOST, PORT))
        print("Connexion établie avec le serveur.")

        # Demander à l'utilisateur s'il veut simuler une panne
        simuler_panne = input("Voulez-vous simuler une panne ? (o/n) ").lower()

        if simuler_panne == "o":
            # Envoyer un message de panne au serveur
            message_panne = "enpanne"
            client_socket.sendall(message_panne.encode())
            print("Message de panne envoyé au serveur.")
        else:
            # Recevoir les données du serveur
            taille_fichier_bytes = client_socket.recv(4)
            taille_fichier = int.from_bytes(taille_fichier_bytes, byteorder='big')

            donnees_recues = b''
            while len(donnees_recues) < taille_fichier:
                paquet = client_socket.recv(4096)
                if not paquet:
                    break
                donnees_recues += paquet
            print("Message recu du  serveur.")
            # Traiter les données reçues
             


    except socket.error as e:
        print(f"Erreur de socket : {e}")

    finally:
        # Fermer la connexion
        client_socket.close()