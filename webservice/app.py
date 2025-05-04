#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
import boto3
from botocore.config import Config
import os
import uuid
from dotenv import load_dotenv
from typing import Union
import logging
from fastapi import FastAPI, Request, status, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from getSignedUrl import getSignedUrl

load_dotenv()

app = FastAPI()
logger = logging.getLogger("uvicorn")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
	exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
	logger.error(f"{request}: {exc_str}")
	content = {'status_code': 10422, 'message': exc_str, 'data': None}
	return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class Post(BaseModel):
    title: str
    body: str

my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
)

dynamodb = boto3.resource('dynamodb', config=my_config)
table = dynamodb.Table(os.getenv("DYNAMO_TABLE"))
s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))
bucket = os.getenv("BUCKET")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################



from botocore.exceptions import ClientError

def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response

@app.post("/posts")
async def post_a_post(post: Post, authorization: str | None = Header(default=None)):
    """
    Poste un post ! Les informations du poste sont dans post.title, post.body et le user dans authorization
    """
    logger.info(f"title : {post.title}")
    logger.info(f"body : {post.body}")
    logger.info(f"user : {authorization}")


    if authorization is None:
        return JSONResponse(
            status_code=401,
            content={"message": "Missing authorization header"},
        )

    post_id = f"POST#{uuid.uuid4()}"
    user_key = f"USER#{authorization}"

    item = {
        "user": user_key,
        "id": post_id,
        "title": post.title,
        "body": post.body
    }

    table.put_item(Item=item)

    return item

@app.get("/posts")
async def get_all_posts(user: Union[str, None] = None):
    """
    Récupère tout les postes. 
    - Si un user est présent dans le requête, récupère uniquement les siens
    - Si aucun user n'est présent, récupère TOUS les postes de la table !!
    """
    if user:
        logger.info(f"Récupération des postes de : {user}")
        response = table.scan()
        items = [item for item in response.get("Items", []) if item.get("user") == f"USER#{user}"]
    else:
        logger.info("Récupération de tous les postes")
        response = table.scan()
        items = response.get("Items", [])

    # Ajout de l’URL pré-signée et du champ 'label' pour chaque post
    for post in items:
        if post.get("image"):
            try:
                url = create_presigned_url(bucket, post.get("image"))
                post["image"] = url
            except Exception as e:
                logger.error(f"Erreur génération URL signée : {e}")
                post["image"] = None
        else:
            post["image"] = None

        # Ajout d’un champ label vide (peut être rempli plus tard)
        if "label" not in post:
            post["label"] = []
        logger.info(f"Item après génération URL : {post}")

    return items

    
@app.delete("/posts/{post_id}")
async def delete_post(post_id: str, authorization: str | None = Header(default=None)):
    # Doit retourner le résultat de la requête la table dynamodb
    logger.info(f"post id : {post_id}")
    logger.info(f"user: {authorization}")
    # Récupération des infos du poste

    # S'il y a une image on la supprime de S3

    # Suppression de la ligne dans la base dynamodb

    # Retourne le résultat de la requête de suppression
    return item



#################################################################################################
##                                                                                             ##
##                                 NE PAS TOUCHER CETTE PARTIE                                 ##
##                                                                                             ##
## 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 👇 ##
@app.get("/signedUrlPut")
async def get_signed_url_put(filename: str,filetype: str, postId: str,authorization: str | None = Header(default=None)):
    return getSignedUrl(filename, filetype, postId, authorization)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="debug")

## ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ☝️ ##
##                                                                                                ##
####################################################################################################