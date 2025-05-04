import json
from urllib.parse import unquote_plus
import boto3
import os
import logging
print('Loading function')
logger = logging.getLogger()
logger.setLevel("INFO")
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
reckognition = boto3.client('rekognition')

table = dynamodb.Table(os.getenv("table"))

def lambda_handler(event, context):
    logger.info(json.dumps(event, indent=2))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key']) # <- A modifier !!!!
    logger.info(f"Bucket: {bucket}")
    logger.info(f"Key: {key}")
    # Récupération de l'utilisateur et de l'UUID de la tâche
    try:
        user, post_id, _ = key.split('/')
        user = f"USER#{user}"
        post_id = f"POST#{post_id}"
    except ValueError:
        logger.error("Clé S3 invalide, attendu format : USER#hiba/POST#uuid/image.jpg")
        return {
            "statusCode": 400,
            "body": "Invalid S3 key format"
        }
    # Ajout des tags user et task_uuid
    
    s3.put_object_tagging(
    Bucket=bucket,
    Key=key,
    Tagging={
        'TagSet': [
            {'Key': 'user', 'Value': user},
            {'Key': 'task_uuid', 'Value': post_id}
        ]
    }
)
    logger.info(f"Tags user={user}, task_uuid={post_id} ajoutés à {key}")


    # Appel à reckognition
    label_data = reckognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MaxLabels=5,
        MinConfidence=75
    )
    logger.info(f"Labels data : {label_data}")

    # Récupération des résultats des labels
    labels = [label['Name'] for label in label_data['Labels']]
    logger.info(f"Labels detected : {labels}")

    # Mise à jour de la table dynamodb
    table.update_item(
    Key={
        'user': user,
        'id': post_id
    },
    UpdateExpression="SET image = :img, labels = :lbl",
    ExpressionAttributeValues={
        ':img': key,
        ':lbl': labels
    }
    )