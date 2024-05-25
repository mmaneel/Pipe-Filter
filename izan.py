# Client 1
import socket

# Créer un socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Obtenir l'adresse IP locale de l'hôte
host = socket.gethostbyname(socket.gethostname())
port = 5000  # Choisissez un port libre

# Connecter le socket au port
client_socket.bind((host, port))

# Écouter les connexions entrantes
client_socket.listen(1)
print(f"En attente de connexion sur {host}:{port}...")

# Accepter la connexion
conn, addr = client_socket.accept()
print(f"Connecté à {addr}")

# Envoyer un message
message = "Salut, je suis le client 1!"
conn.send(message.encode())

# Fermer la connexion
conn.close()