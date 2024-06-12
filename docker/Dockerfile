FROM python:3.9-slim

# Définir le répertoire de travail
WORKDIR /app

COPY docker/requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Installer Java pour UCanAccess
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Définir les variables d'environnement Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Démarrer l'application Streamlit
CMD ["streamlit", "run", "app.py"]
