# Parte 1 invocar la funsiones y la variable de entrono
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

#Parte 2 thresholds y estados:
THRESHOLD_INICIO = 60
THRESHOLD_FIN = 80

ESTADO_INICIO = 0
ESTADO_PRESENCIA_FLANCO = 1
ESTADO_FLANCO_CONFIRMADO = 2


def heading_analysis(heading, client,):
    #print(f"Accel {accel}")
    estado_sistema = client_local._userdata["estado_sistema"]
    estado_luz = client_local._userdata["estado_luz"]
    logging.info(f"Accel {heading}, estado_sistema {estado_sistema}")

    #Máquina de estados
    if estado_sistema == ESTADO_INICIO:
        topico = "actuadores/luces/1"
        client_local.publish(topico, 0)
        #Completar el código del estado ESTADO_INICIO
        if heading > THRESHOLD_INICIO:
            estado_sistema = ESTADO_PRESENCIA_FLANCO 

    elif estado_sistema == ESTADO_PRESENCIA_FLANCO:
        # completar el código del estado ESTADO_PRESENCIA_FLANCO
        if heading < THRESHOLD_INICIO:
            estado_sistema = ESTADO_INICIO
            
        elif heading > THRESHOLD_FIN:
            estado_sistema = ESTADO_FLANCO_CONFIRMADO
            #preder la luz enviar un mensaje MQTT
            #topico = "sensores/inerciales"
            #topico = "actuadores/luces/1"
            #if estado_luz == 1:
                #estado_luz = 0
            #elif estado_luz == 0:
                #estado_luz = 1
            #client_local.publish(topico, estado_luz)
           

    elif estado_sistema == ESTADO_FLANCO_CONFIRMADO:
        topico = "actuadores/luces/1"
        client_local.publish(topico, 1)
        #completar el código del estado ESTADO_FLANCO_CONFIRMADO
        if heading< THRESHOLD_INICIO:
            estado_sistema = ESTADO_INICIO
            logging.info(f"RESET ESTADO_INICIO {heading}")

     # Almacenar el nuevo valor de estado
    client_local._userdata["estado_sistema"] = estado_sistema
    client_local._userdata["estado_luz"] = estado_luz



 #Parte 2 MQTT LOCAL
 # 2.1) Establecer el callback para la conexion LOCAl, fusion on_connect_local
def on_connect_local(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Mqtt_Local esta conectado")

        #Aquí Suscribirse a los topicos locales deseados
        client.subscribe("actuadores/volar")
        client.subscribe("actuadores/luces/1")
        client.subscribe("actuadores/motores/#")
        client.subscribe("actuadores/joystick")
        client.subscribe("sensores/gps")
        client.subscribe("sensores/inerciales")
        client.subscribe("sensores/monitoreo")
    else:
        logging.error(f"Mqtt_Local No se pudo establecer conectar, error code={rc}")

 #2.2) Recibir los mensaje y topicos que llegan del on_connect_cocal me dioante la funcion 
 #on_message_local (Productor)
def on_message_local(client, userdata, message):
    queue_local = userdata["queue_local"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))
    #Agregar dato de la cola 
    queue_local.put({"topico": topico, "mensaje": mensaje})


 #2.3) Aquí crear el callback procesamiento_local (Consumidor)
def procesamiento_local(name, flags, client_local, client_remoto):
    logging.info(f"{name}: Comienza thread de MQTT local")
    queue_local = client_local._userdata["queue_local"]
    
    while flags["thread_continue"]:
        msg = queue_local.get(block=True)

        # Sino hay nada por leer, vuelvo a esperar por otro mensaje
        if msg is None:
            continue

        #Hay datos para leer, los consumo e imprimo en consola
        print(f"mensaje recibido en thread {name}:")
        print(f"{msg['topico']}: {msg['mensaje']}")
        topico = msg['topico']
        mensaje = msg['mensaje']

        # Consultar si el tópico es de los sensores inerciales
        if topico == "sensores/inerciales":
            data = json.loads(mensaje)
            heading = float(data["heading"])
            heading_analysis(heading, client_local)

        #Enviando mensaje y topico a la nuve desde el thread prosamiento_local
        topico_remoto = config["DASHBOARD_TOPICO_BASE"] + topico
        client_remoto.publish(topico_remoto, mensaje)

    logging.info(f"{name}termina thread")

    #Parte 3 MQTT REMOTO
    # 3.1) Establecer el callback para la conexion LOCAl, fusion on_connect_local
def on_connect_remoto(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Mqtt_Remoto Esta conectado")
        # Aquí Suscribirse a los topicos remotos deseados
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/volar")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/luces/1")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/motores/#")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "sensores/inerciales")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "keepalive/request")
    else:
        logging.error(f"Mqtt_Remoto No se pudo establecer conectar , error code={rc}")

    #3.2) Aquí crear el callback on_message_remoto
def on_message_remoto(client, userdata, message):
    queue_remoto= userdata["queue_remoto"]
    topico = message.topic
    mensaje= str(message.payload.decode("utf-8"))
    queue_remoto.put({"topico": topico, "mensaje": mensaje})

    #3.3 Crear la funcion procesamiento_remoto#
def procesamiento_remoto(name,queue_remoto, client_local, flags):
    logging.info(f"{name}: Comienza thread MQTT remoto")
    queue_remoto= client_remoto._userdata["queue_remoto"]

    while flags["thread_continue"]:
        datos = queue_remoto.get(block=True)
        topico = datos["topico"]
        mensaje = datos["mensaje"]

        topico_local = topico.replace(config["DASHBOARD_TOPICO_BASE"], "")

        # Analisis topico recibido
        if topico == "keepalive/request":
            logging.debug("responde al keepalive")
            #topico_remoto = config["DASHBOARD_TOPICO_BASE"] + "keepalive/ack"
            client_remoto.publish(config["DASHBOARD_TOPICO_BASE"] + "keepalive/ack",1)
        #Agregar el destintivo de que el mensaje viene del dashboard
        topico_local = "dashboardiot/" + topico_local
        logging.debug("Dato recibido de espacio -topico:", topico_local)
        logging.debug("Dato recibido del espacio -mensaje:",mensaje)
        client_local.publish(topico,mensaje)
       
    logging.info(f"{name} Termina thread")

   #Fusion de finalizar programa
flags = {"thread_continue": True}
def finalizar_programa(sig, frame):
    global flags
    logging.info("Señal de terminar programa")    
    flags["thread_continue"] = False

   # Invocar el programa principal

if __name__ == "__main__":
    # crear la carepta de logs sino existe
    os.makedirs("./logs", exist_ok=True)
    # Configurar el logger
    with open("logging.json", 'rt') as f:
        logging_config = json.load(f)
        logging.config.dictConfig(logging_config)

  
    # Definir estado del sistema inicial
    estado_sistema = ESTADO_INICIO

    #Crear la queue, y para poder recibir la informacion del cliente local para enviarla al cliente remoto
    queue_remoto = Queue()


    #Conectarse al MQTT_REMOTO
    random_id = random.randint(1, 999)
    client_remoto = paho.Client(f"cliente_remoto_danny{random_id}")
    client_remoto.on_message = on_message_remoto
    #Creadenciales
    client_remoto.username_pw_set(config["DASHBOARD_MQTT_USER"], config["DASHBOARD_MQTT_PASSWORD"])
    client_remoto.user_data_set(
        {
            "queue_remoto":queue_remoto,
        }
    )
    client_remoto.connect(config["DASHBOARD_MQTT_BROKER"], int(config["DASHBOARD_MQTT_PORT"]))
    client_remoto.loop_start()

    #Conectarse al MQTT_LOCAL
    #Crear la queue
    queue_local = Queue()

    client_local = paho.Client("cliente_local_danny")
    client_local.on_connect = on_connect_local
    client_local.on_message = on_message_local
    #dejar disponible la queue dentro de "user_data" #
    client_local.user_data_set(
        {
            "queue_local": queue_local,
            "estado_sistema": estado_sistema,
            "estado_luz": 0
        }
    )
    #Para compartir variable entre dos funsiones distintas se usa el userdata para compartir 
    #como parametros locales de la funsion y no usar parametros globales

    client_local.connect(config["BROKER"], int(config["PORT"]))
    client_local.loop_start()

    #Capturar el finalizar programa forzado con la funsion finalizar_programa
    signal.signal(signal.SIGINT, finalizar_programa)

    # Crear los Threads del procesamiento_remoto y del procesamiento_local
    #thread_procesamiento_local

    thread_procesamiento_local = threading.Thread(
        target=procesamiento_local, args=("procesamiento_local", flags, client_local, client_remoto),
        daemon=True
    )
    thread_procesamiento_local.start()
    #Thread_procesamiento_remoto

    thread_procesamiento_remoto = threading.Thread(
        target=procesamiento_remoto,args=("procesamiento_remoto",
        queue_remoto,client_local,flags),
        daemon =True
    )
    thread_procesamiento_remoto.start()
    
    while flags["thread_continue"]:
        time.sleep(0.5)

    print("Comenzando la finalización de los threads...")
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
    
    