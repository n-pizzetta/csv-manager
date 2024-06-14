# Variables
DOCKER_IMAGE_NAME = csv-manager
DOCKERFILE_PATH = Dockerfile
PROJECT_DIRECTORY = $(shell pwd)
NAME = csv-manager
TAG = latest

# Cibles
build:
	docker build -t ${NAME}:${TAG} -f ${DOCKERFILE_PATH} ..

run:
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


stop:
	docker stop ${DOCKER_IMAGE_NAME}
	docker rm ${DOCKER_IMAGE_NAME}

clean:
	docker rmi ${NAME}:${TAG}

restart: stop clean build run
