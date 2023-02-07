from kafka import KafkaConsumer
import logging
import logging.handlers as handlers
import ast
from datetime import datetime, timedelta
from huron_process import HuronProcess

TOPIC = 'NP_refex_api'
data_received_plant_list = []


def main(logger):
    consumer = KafkaConsumer(TOPIC, bootstrap_servers='172.16.0.29:9092')
    for msg in consumer:
        try:
            msg_data = msg.value.decode("utf-8")
            json_data = ast.literal_eval(msg_data)
            try:
                plant_id = str(json_data['network_id'])
                packet_dt = str(
                    (datetime.utcfromtimestamp(json_data['packet_timestamp']) + timedelta(minutes=330)).strftime(
                        '%Y-%m-%d %H:%M'))
                logger.info(f'{plant_id} \t {packet_dt} \t - Data Received from Client.')
            except Exception as e:
                logger.error('Error Reading Json Data')
                logger.error(msg.value.decode("utf-8"))
                logger.error(e)
            try:
                HuronProcess(logger).generate_csv_from_json(json_data)
            except Exception as e:
                logger.error(e)
        except Exception as e:
            logger.error('Exception in reading msg from Consumer')
            logger.error(e)


if __name__ == '__main__':
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('refex_logger_huron')
    logger.setLevel(logging.INFO)

    logHandler = handlers.TimedRotatingFileHandler('Logs/log.log', when='H', interval=1)
    logHandler.setLevel(logging.INFO)
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    logger.info('Starting Service Huron Consumer v1.6')
    main(logger)

    logger.info('Completed Service Huron Consumer v1.6')
