import socket

# Définir l'adresse et le port du serveur
HOST = '192.168.43.46'
PORT = 5000

# Créer un socket TCP
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    # Se connecter au serveur
    client_socket.connect((HOST, PORT))
    print(f"Connecté au serveur {HOST}:{PORT}")
    # Recevoir les données du serveur
    data_length = int.from_bytes(client_socket.recv(4), byteorder='big')
    data = bytearray()
    while len(data) < data_length:
        packet = client_socket.recv(data_length - len(data))
        if not packet:
            break
        data.extend(packet)
    # Enregistrer les données reçues dans un fichier
    with open('Pipe-Filter/dataset.csv', 'wb') as fichier:
        fichier.write(data)
    print("Données reçues et enregistrées dans 'data_recues.csv'")