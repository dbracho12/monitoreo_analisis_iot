import os
import json
import logging
import logging.config

# CONFIG:
# 1 - primero se define el formato
# 2 - luego se crea cada handler, en el cual definidmos:
#   --> tipo de handler (strea consola, stream archivo)
#   --> level (filtro, se escucha de aquí par aabajo)
#   --> formato (se elije alguno de los definos)
#   --> ubicacion, bytes, output, etc
# 3 --> al final de todo se define el LEVEL general y los handlers a usar

if __name__ == "__main__":
    # crear la carepta de logs sino existe
    os.makedirs("./logs", exist_ok=True)

    # Configurar el logger
    with open("logging.json", 'rt') as f:
       config = json.load(f)
       logging.config.dictConfig(config)

    # Realizar algunas pruebas
    logging.info('¡Hola Mundo!')
    logging.error('Mqtt Local connection faild')
    logging.warning('No se pudo enviar el paquete MQTT')
    logging.debug('THRESHOLD_FIN 15.9')
