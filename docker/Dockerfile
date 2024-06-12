# Utiliser une image de base avec OpenJDK 11 préinstallé
FROM openjdk:11-jdk-slim

# Installer les dépendances Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Définir le répertoire de travail
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY ../requirements.txt .

# Installer les dépendances Python
RUN pip3 install --no-cache-dir -r requirements.txt

# Copier le reste des fichiers de l'application
COPY ../ .

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Démarrer l'application Streamlit
CMD ["streamlit", "run", "app.py"]
