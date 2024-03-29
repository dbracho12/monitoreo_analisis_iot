import time
import json
import random
import threading
import signal
from queue import Queue
import paho.mqtt.client as paho
from dotenv import dotenv_values
import os
import logging
import logging.config

config = dotenv_values()

# ----------------------
# Aquí manipular las funciones de uso general
THRESHOLD_INICIO = 1
THRESHOLD_FIN = 4

ESTADO_INICIO = 0
ESTADO_PRESENCIA_FLANCO = 1
ESTADO_FLANCO_CONFIRMADO = 2

def accel_analysis(accel, client):
    logging.debug(f"Accel {accel}")
    estado_sistema = client_local._userdata["estado_sistema"]
    estado_luz = client_local._userdata["estado_luz"]

    # Máquina de estados
    if estado_sistema == ESTADO_INICIO:
        if accel > THRESHOLD_INICIO:
            # Se ha detectado una aceleración
            # mayor al threshold de inicio, se pasa
            # al estado "en presencia de posible flanco"
            estado_sistema = ESTADO_PRESENCIA_FLANCO
            logging.debug(f"THRESHOLD_INICIO superado {accel}")

    elif estado_sistema == ESTADO_PRESENCIA_FLANCO:
        if accel < THRESHOLD_INICIO:
            # La aceleración a disminuido,
            # se pasa al estado de inicio
            estado_sistema = ESTADO_INICIO
            logging.debug(f"RESET ESTADO_INICIO {accel}")

        elif accel > THRESHOLD_FIN:
            # Se ha detectado una aceleración
            # mayor al threshold de fin de flanco, se pasa
            # al estado "flanco confirmado" y se prende la luz
            estado_sistema = ESTADO_FLANCO_CONFIRMADO

            # Invertir el estado de la luz
            estado_luz = 1 if estado_luz == 0 else 0
            
            topico = "actuadores/luces/1"            
            client.publish(topico, estado_luz)
            logging.debug(f"THRESHOLD_FIN superado {accel}")

    elif estado_sistema == ESTADO_FLANCO_CONFIRMADO:
        # Se espera a que la aceleración disminuya
        # y se termine el flanco ascendente
        if accel < THRESHOLD_INICIO:
            # La aceleración a disminuido,
            # se pasa al estado de inicio
            estado_sistema = ESTADO_INICIO
            logging.debug(f"RESET ESTADO_INICIO {accel}")
    
    # Almacenar el nuevo valor de estado
    client_local._userdata["estado_sistema"] = estado_sistema
    client_local._userdata["estado_luz"] = estado_luz

# ----------------------
# Aquí crear los callbacks de MQTT Local
# Aquí crear el callback on_connect_local
def on_connect_local(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Mqtt Local conectado")

        # Aquí Suscribirse a los topicos locales deseados
        client.subscribe("actuadores/volar")
        client.subscribe("actuadores/luces/1")
        client.subscribe("actuadores/motores/#")
        client.subscribe("actuadores/joystick")
        client.subscribe("sensores/gps")
        client.subscribe("sensores/inerciales")
        client.subscribe("sensores/monitoreo")
    else:
        logging.error(f"Mqtt Local connection faild, error code={rc}")


# Aquí crear el callback on_message_local
def on_message_local(client, userdata, message):
    queue = userdata["queue"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))

    queue.put({"topico": topico, "mensaje": mensaje})


# Aquí crear el callback procesamiento_local
def procesamiento_local(name, flags, client_local, client_remoto):
    logging.info(f"Comienza thread {name}")
    queue = client_local._userdata["queue"]

    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de consumidor con "get".
        # En este caso el sistema esperará (block=True) hasta que haya
        # al menos un item disponible para leer        
        msg = queue.get(block=True)

        # Sino hay nada por leer, vuelvo a esperar por otro mensaje
        if msg is None:
            continue

        # Hay datos para leer y consumir
        topico = msg['topico']
        mensaje = msg['mensaje']

        # Consultar si el tópico es de los sensores inerciales
        if topico == "sensores/inerciales":
            data = json.loads(mensaje)
            accel = float(data["accel"])
            accel_analysis(accel, client_local)
        
        topico_remoto = config["DASHBOARD_TOPICO_BASE"] + topico
        client_remoto.publish(topico_remoto, mensaje)

    logging.info(f"Termina thread {name}")

# ----------------------
# ----------------------
# Aquí crear los callbacks de MQTT Remoto

def on_connect_remoto(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Mqtt Remoto conectado")

        # Aquí Suscribirse a los topicos remotos deseados
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/volar")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/luces/1")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/motores/#")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "keepalive/request")
    else:
        logging.error(f"Mqtt Remoto connection faild, error code={rc}")


# Aquí crear el callback on_message_remoto
def on_message_remoto(client, userdata, message):
    queue = userdata["queue"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))
    queue.put({"topico": topico, "mensaje": mensaje})


# Aquí crear el callback procesamiento_remoto
def procesamiento_remoto(name, flags, client_local, client_remoto):
    logging.info(f"Comienza thread {name}")
    queue = client_remoto._userdata["queue"]

    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de consumidor con "get".
        # En este caso el sistema esperará (block=True) hasta que haya
        # al menos un item disponible para leer        
        msg = queue.get(block=True)

        # Sino hay nada por leer, vuelvo a esperar por otro mensaje
        if msg is None:
            continue

        # Hay datos para leer y consumir
        topico_completo = msg['topico']
        mensaje = msg['mensaje']
        
        # Quitar la parte del tópico que corresponde al dashboard y el usuario
        topico = topico_completo.replace(config["DASHBOARD_TOPICO_BASE"], '')

        # Analizar topico recibido
        if topico == "keepalive/request":
            # Enviar respuesta de keepalive
            topico_remoto = config["DASHBOARD_TOPICO_BASE"] + "keepalive/ack"
            client_remoto.publish(topico_remoto, "1")

        # Agregar el destintivo de que el mensaje viene del dashboard
        topico_local = "dashboardiot/" + topico
        # Enviar el mensaje al cliente MQTT local para que otros
        # acutadores o sensores estén al tanto de lo recibido
        client_local.publish(topico_local, mensaje)

    logging.info(f"Termina thread {name}")

# ----------------------

# Flags que almacenaremos para todos los threads en comun
# como por ejemplo el flag que indica que debe terminarse el thread
# Recordar que no se debe pasar al threads variables tipo bool, int o string,
# siempre usar un objeto (como en este caso un diccionario)
flags = {"thread_continue": True}
def finalizar_programa(sig, frame):
    global flags
    logging.info("Señal de terminar programa")    
    flags["thread_continue"] = False


if __name__ == "__main__":
    # crear la carepta de logs sino existe
    os.makedirs("./logs", exist_ok=True)

    # Configurar el logger
    with open("logging.json", 'rt') as f:
        logging_config = json.load(f)
        logging.config.dictConfig(logging_config)

  
    # Definir estado del sistema inicial
    estado_sistema = ESTADO_INICIO
  
    # ----------------------
    # Aquí conectarse a MQTT remoto
    queue_remoto = Queue()

    random_id = random.randint(1, 999)
    client_remoto = paho.Client(f"remoto_{random_id}")
    client_remoto.on_connect = on_connect_remoto
    client_remoto.on_message = on_message_remoto
    # Configurar las credenciales del broker remoto
    client_remoto.username_pw_set(config["DASHBOARD_MQTT_USER"], config["DASHBOARD_MQTT_PASSWORD"])
    client_remoto.user_data_set(
        {
            "queue": queue_remoto,
        }
    )

    client_remoto.connect(config["DASHBOARD_MQTT_BROKER"], int(config["DASHBOARD_MQTT_PORT"]))
    client_remoto.loop_start()

    # Aquí conectarse a MQTT local
    queue_local = Queue()

    client_local = paho.Client("local")
    client_local.on_connect = on_connect_local
    client_local.on_message = on_message_local
    client_local.user_data_set( 
        {
            "queue": queue_local,
            "estado_sistema": estado_sistema,
            "estado_luz": 0
        }
    )

    client_local.connect(config["BROKER"], int(config["PORT"]))
    client_local.loop_start()


    # Capturar el finalizar programa forzado
    signal.signal(signal.SIGINT, finalizar_programa)

    # El programa principal solo armará e invocará threads
    logging.info("Lanzar thread de procesamiento de MQTT local")
    thread_procesamiento_local = threading.Thread(target=procesamiento_local, args=("procesamiento_local", flags, client_local, client_remoto), daemon=True)
    thread_procesamiento_local.start()

    logging.info("Lanzar thread de procesamiento de MQTT remoto")
    thread_procesamiento_remoto = threading.Thread(target=procesamiento_remoto, args=("procesamiento_remoto", flags, client_local, client_remoto), daemon=True)
    thread_procesamiento_remoto.start()

    # ----------------------
    # El programa principal queda a la espera de que se desee
    # finalizar el programa
    while flags["thread_continue"]:
        # busy loop
        time.sleep(0.5)
    
    # ----------------------
    logging.info("Comenzando la finalización de los threads...")
    # Se desea terminar el programa, desbloqueamos los threads
    # con un mensaje vacio
    queue_local.put(None)
    queue_remoto.put(None)
    
    # No puedo finalizar el programa sin que hayan terminado los threads
    # el "join" espera por la conclusion de cada thread, debo lanzar el join
    # por cada uno
    thread_procesamiento_local.join()
    thread_procesamiento_remoto.join()

    client_local.disconnect()
    client_local.loop_stop()

    client_remoto.disconnect()
    client_remoto.loop_stop()