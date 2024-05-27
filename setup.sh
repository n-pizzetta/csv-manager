#!/bin/bash

# Installer OpenJDK
apt-get update
apt-get install -y openjdk-11-jdk

# Définir JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Écrire JAVA_HOME dans un fichier pour que Python puisse y accéder
echo "JAVA_HOME=$JAVA_HOME" > ~/.streamlit/env_vars.sh