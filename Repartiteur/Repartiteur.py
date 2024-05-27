import socket
import pandas as pd
import threading
import time

# Définir l'adresse et le port du serveur
HOST = '192.168.43.46'
PORT = 5000

def diviser_fichier(fichier_origine):
    # Charger le fichier d'origine
    df = pd.read_csv(fichier_origine)
    # Calculer le nombre de lignes à inclure dans chaque fichier
    nb_lignes_total = len(df)
    nb_lignes_30 = nb_lignes_total // 3
    nb_lignes_70 = nb_lignes_total - nb_lignes_30
    # Diviser les données en deux DataFrames
    df_30 = df.head(nb_lignes_30)  # df_30 contiendra les 30% premières lignes de df
    df_70 = df.tail(nb_lignes_70)  # df_70 contiendra les 70% dernières lignes de df
    return df_30, df_70

def envoyer_dataframe(client_socket, df):
    try:
        # Convertir le DataFrame en format CSV
        csv_data = df.to_csv(index=False).encode()
        # Envoyer la longueur des données au client en premier
        client_socket.sendall(len(csv_data).to_bytes(4, byteorder='big'))
        # Envoyer les données au client
        client_socket.sendall(csv_data)
        print(f"{len(df)} lignes envoyées avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'envoi des données : {e}")

def gerer_client(client_socket1,client_socket2, df_30, df_70):
    try:
        if client_socket2 == None : 
            panne = False
            sent = False
            client_socket1.settimeout(10)  # Définir un délai d'attente de 10 secondes
            while True:
                try:
                    data = client_socket1.recv(1024)
                    if data:
                        message = data.decode()
                        if message == "panne":
                            panne = True
                            print("Aucune unite est connectée") 
                            break
                except socket.timeout:
                    print("Délai d'attente de 10 secondes dépassé")
                    if (not panne) and (not sent):
                        print("not panne and not sent")
                        envoyer_dataframe(client_socket1, pd.concat([df_30, df_70]))  # Envoyer 70% des données au client
                        sent = True
                    break
        else : 
            panne = False
            sent = False
            client_socket1.settimeout(10)  # Définir un délai d'attente de 10 secondes
            while True:
                try:
                    data = client_socket1.recv(1024)
                    if data:
                        message = data.decode()
                        if message == "panne":
                            panne = True
                            envoyer_dataframe(client_socket2, pd.concat([df_30, df_70]))  # Envoyer toutes les données au client
                            client_socket2.close()
                            break
                except socket.timeout:
                    print("Délai d'attente de 10 secondes dépassé")
                    if (not panne) and (not sent):
                        print("not panne and not sent")
                        envoyer_dataframe(client_socket1, df_70)  # Envoyer 70% des données au client
                        sent = True
                    break
    except Exception as e:
        print(f"Erreur avec le client : {e}")
    finally:
        client_socket1.close()



def main():
    try:
        # Créer un socket TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((HOST, PORT))
            server_socket.listen(2)  # Accepter jusqu'à 2 connexions simultanées
            print(f"Serveur en écoute sur {HOST}:{PORT}")
            # Diviser le fichier CSV en deux parties
            df_30, df_70 = diviser_fichier('dataset_consommation_energie_algerie.csv')
            server_socket.settimeout(10) 

            while True:
                client_socket1, address1 = server_socket.accept()
                 
                if '192.168.43.215' != address1[0] : 
                    client_socket2, address2 = client_socket1, address1
                    print(f"Nouvelle connexion de passive {address2}")
                    try : 
                        client_socket1, address1 = server_socket.accept()
                        print(f"Nouvelle connexion de active {address1}")
                    except socket.timeout:
                        client_socket1 = None

                else :
                    print(f"Nouvelle connexion de active {address1}")
                    try : 
                        client_socket2, address2 = server_socket.accept()
                        print(f"Nouvelle connexion de passive {address2}")
                    except socket.timeout:
                        client_socket2 = None
                

                if client_socket1 is None:
                    t1 = threading.Thread(target=gerer_client, args=(client_socket2,client_socket1, df_70, df_30))
                    t1.start()
                    t1.join()
                elif client_socket2 is None:
                    t1 = threading.Thread(target=gerer_client, args=(client_socket1,client_socket2, df_30, df_70)) 
                    t1.start()
                    t1.join()
                else:
                    t1 = threading.Thread(target=gerer_client, args=(client_socket1,client_socket2, df_30, df_70)) 
                    t2 = threading.Thread(target=gerer_client, args=(client_socket2,client_socket1, df_70, df_30))
                    t1.start()
                    t2.start()
                    t1.join()
                    t2.join()
                break
    except Exception as e:
        print(f"Erreur de serveur : {e}")

if __name__ == "__main__":
    main()