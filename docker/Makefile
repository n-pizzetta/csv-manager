# Variables
DOCKER_IMAGE_NAME = my-streamlit-app

# Cibles
build:
	docker build -t $(DOCKER_IMAGE_NAME) .

run:
	docker run -p 8501:8501 $(DOCKER_IMAGE_NAME)

stop:
	docker stop $(shell docker ps -q --filter ancestor=$(DOCKER_IMAGE_NAME))

clean:
	docker rmi $(DOCKER_IMAGE_NAME)
