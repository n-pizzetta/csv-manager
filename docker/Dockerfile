# Utiliser une image de base avec OpenJDK 24 préinstallé
FROM openjdk:24-jdk-slim

# Installer les dépendances Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    apt-get clean

# Définir le répertoire de travail
WORKDIR /resources

# Copier le fichier requirements.txt dans le conteneur
COPY ../requirements.txt .

# Créer un environnement virtuel Python et installer les dépendances
RUN python3 -m venv /resources/venv
RUN /resources/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copier le reste des fichiers de l'application
COPY ../ .

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Démarrer l'application Streamlit en utilisant l'environnement virtuel
CMD ["/bin/bash", "-c", "source /resources/venv/bin/activate && streamlit run app.py --server.port=8501"]