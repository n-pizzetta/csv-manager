#!/bin/bash

# Définir les variables
DOCKER_IMAGE_NAME=csv-manager
PROJECT_DIRECTORY=$(pwd)
NAME=csv-manager
TAG=latest

# Construire l'image Docker
docker build -t ${NAME}:${TAG} -f Dockerfile ..

# Exécuter le conteneur en arrière-plan
docker run -it \
    --name ${DOCKER_IMAGE_NAME} \
    -v ${PROJECT_DIRECTORY}:/app \
    -d --restart=always \
    -p 8501:8501 \
    --memory=3.5g \
    --cpus=4 \
    --log-opt max-size=10m \
    --log-opt max-file=3 \
    ${NAME}:${TAG}

echo "L'application est en cours d'exécution sur http://localhost:8501"
