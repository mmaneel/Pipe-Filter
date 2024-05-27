import socket

# Adresse IP locale de cette machine
host_ip = "192.168.43.87"
host_port = 5000

# Créer un socket TCP/IP
receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
receiver_socket.bind((host_ip, host_port))
# Écouter les connexions entrantes
receiver_socket.listen(1)
print(f"En écoute sur {host_ip}:{host_port}")

# Accepter la connexion entrante
conn, addr = receiver_socket.accept()
print(f"Connexion établie avec {addr}")

while True:
    # Recevoir le nom du fichier
    nom_fichier = conn.recv(1024).decode().strip()
    if not nom_fichier:
        break

    # Ouvrir un nouveau fichier en mode binaire pour écrire
    with open(nom_fichier, 'wb') as fichier:
        while True:
            data = conn.recv(1024)
            if not data:
                # Signal de fin de données reçu
                break
            fichier.write(data)
        print(f"Fichier {nom_fichier} reçu du client expéditeur.")

    # Envoyer une réponse
    response = "Fichier reçu avec succès."
    conn.sendall(response.encode())

# Fermer la connexion
conn.close()
receiver_socket.close()