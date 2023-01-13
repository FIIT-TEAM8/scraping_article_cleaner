from dotenv import load_dotenv
from lxml import html
import os
import re
import logging
import subprocess
import json
import pika
if os.path.exists("./.env"):
    load_dotenv()

# extract text from specified tags. Keeps the order of the text.
def extract_from_tags(raw_html: str):
    root = html.fromstring(raw_html)
    elements = root.xpath("//p | //h1 | //h2 | //h3 | //h4 | //h5 | //h6")
    text = ""
    for el in elements:
        text += " " + str(el.text)
    return text

# returns a STRING (not a list) with every match delimited by a space. Keeps the original order
def extract_names(clean_data: str):
    pattern = r'\b([A-Z][a-z]+)\b'
    names = " ".join(re.findall(pattern, clean_data))
    return names


# code should be split to phases:
# 1. Startup - this runs only once, on the container start
# 2. Init - this is ran every time a new article is processed.
# 3. Work - actual work with the article
# 4. Cleanup - cleaning up after the article, if needed.
# Phases 1 and 2 should be in try block, phase 4 should be in finally block so the cleanup is ran every single time, even if exeption occurs
# all phases, except the 1st one are executed inside main_callback function.


def main_callback(ch, method, properties, body):
    try:
        # INIT phase - handle new article. Create a temp file in shared volume that indicates that a process is running
        # create temp file by combining the hostname of the container
        output = subprocess.run(['hostname'], capture_output=True, text=True)
        temp_file_name = os.environ.get("PROGRESS_DIR") + "/" + "cleaning-" + output.stdout.strip()
        temp_file = open(temp_file_name, "w")
        # WORK phase - work on the article. Extract relevant tags and then extract anything that looks like a human name.
        message = json.loads(body.decode())
        clean = extract_from_tags(message["html"])
        names = extract_names(clean)
        message["names"] = names
        
        # now post the cleaned message further along
        serialized_message = json.dumps(message)
        ch.basic_publish(exchange='', routing_key=os.environ.get("RABBITMQ_PRODUCE_QUEUE"), body=serialized_message)
        logging.info("OK")
    except Exception as e:
        logging.error(e)
    finally:
        # CLEANUP phase - delete the temp file created in INIT phase to indicate that work is done.
        if os.path.exists(temp_file_name):
            temp_file.close()
            os.remove(temp_file_name)


# STARTUP phase - connect to message broker, dbs, elastic
logging.basicConfig(level=os.environ.get("LOGGING_LEVEL"))
# connect to rabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ.get("RABBITMQ_HOST"),
                                                               port=int(os.environ.get("RABBITMQ_PORT")),
                                                               virtual_host=os.environ.get("RABBITMQ_VHOST"),
                                                               credentials=pika.PlainCredentials(username=os.environ.get("RABBITMQ_USER"), 
                                                                                                password=os.environ.get("RABBITMQ_PASSWORD"))))
channel = connection.channel()
channel.queue_declare(queue=os.environ.get("RABBITMQ_CONSUME_QUEUE"))
channel.queue_declare(queue=os.environ.get("RABBITMQ_PRODUCE_QUEUE"))
channel.basic_consume(queue=os.environ.get("RABBITMQ_CONSUME_QUEUE"), on_message_callback=main_callback, auto_ack=True)

# start the consuming loop
channel.start_consuming()
