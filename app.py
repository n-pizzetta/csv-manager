import streamlit as st
import pandas as pd
import jaydebeapi
from io import BytesIO
import os
import jpype
import tempfile
import zipfile
import warnings
import psutil
import objgraph
import sys
import gc
import io
import contextlib


###########################
## Fonctions utilitaires ##
###########################


def check_memory():
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_used_mb = memory_info.rss / (1024 * 1024)
    virtual_mem = psutil.virtual_memory()
    memory_free_mb = virtual_mem.available / (1024 * 1024)

    st.write("Mémoire utilisée :", memory_used_mb, "MB")
    st.write("Mémoire disponible :", memory_free_mb, "MB")

    # Capture de la taille totale de tous les objets en mémoire
    total_size = sum(sys.getsizeof(x) for x in gc.get_objects())
    st.write("Taille totale de tous les objets en mémoire :", total_size, "octets")




# Fonction pour lire un fichier Access et récupérer les données spécifiques
def read_access_file(db_path, ucanaccess_jars, progress_callback=None, status_text=st.empty()):
    conn = None
    cursor = None
    data_frame = pd.DataFrame()

        # Charger les fichiers JAR nécessaires
    ucanaccess_jars = [
        'ucanaccess-5.0.1.jar',
        os.path.join('loader', 'ucanload.jar'),
        os.path.join('lib', 'commons-lang3-3.8.1.jar'),
        os.path.join('lib', 'commons-logging-1.2.jar'),
        os.path.join('lib', 'hsqldb-2.5.0.jar'),
        os.path.join('lib', 'jackcess-3.0.1.jar')
    ]

    # Message initial
    status_text.text("Converting...")

    try:
        # Définition de la chaine de connexion
        conn_string = f"jdbc:ucanaccess://{db_path}"

        if progress_callback:
            progress_callback(0.1)  # 10% du processus terminé (connexion définie)

        # Connexion à la base de données
        conn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver", 
                                  conn_string, 
                                  [], 
                                  ucanaccess_jars[0])
        
        if progress_callback:
            progress_callback(0.3)  # 30% du processus terminé (connexion établie)

        cursor = conn.cursor()
        
        # Exécuter une requête SQL et stocker les résultats dans un DataFrame pandas
        sql = "select Date_Jour_H_M_d, PDM, TV_corrige, TV_brut from Trafic_Minute"

        cursor.execute(sql)
        results = cursor.fetchall()
        data_frame = pd.DataFrame(results, columns=["Date_Jour_H_M_d", "PDM", "TV_corrige", "TV_brut"])

        if progress_callback:
            progress_callback(0.8)  # 80% du processus terminé (requête exécutée)
    
    except Exception as e:
        st.error(f"Error processing {db_path}: {str(e)}")

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                st.error(f"Failed to close cursor for {db_path}: {str(e)}")
        if conn:
            try:
                conn.close()
            except Exception as e:
                st.error(f"Failed to close connection for {db_path}: {str(e)}")
    
    return data_frame


# Fonction pour sauvegarder en CSV
def save_to_csv(data, file_name):
    output = BytesIO()
    data.to_csv(output, index=False)
    return output.getvalue(), file_name


# Fonction pour créer un fichier ZIP
def create_zip_file(files_dict):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_name, file_data in files_dict.items():
            zip_file.writestr(f"{file_name}.csv", file_data)
    zip_buffer.seek(0)

    # Créer un bouton pour télécharger le fichier ZIP
    st.download_button(
        label="Télécharger tous les fichiers convertis",
        data=zip_buffer,
        file_name="converted_files.zip",
        mime="application/zip",
        on_click=update_key
    )

# Fonction pour démarrer la JVM
@st.cache
def start_jvm():

    # Obtenir le répertoire de travail courant
    current_dir = os.getcwd()

    # Charger les fichiers JAR nécessaires
    ucanaccess_jars = [
        'ucanaccess-5.0.1.jar',
        os.path.join('loader', 'ucanload.jar'),
        os.path.join('lib', 'commons-lang3-3.8.1.jar'),
        os.path.join('lib', 'commons-logging-1.2.jar'),
        os.path.join('lib', 'hsqldb-2.5.0.jar'),
        os.path.join('lib', 'jackcess-3.0.1.jar')
    ]

    # Concaténer les chemins des fichiers JAR correctement
    classpath = ":".join([os.path.join(current_dir, "UCanAccess-5.0.1.bin", jar) for jar in ucanaccess_jars])

    # Afficher le classpath pour le débogage
    #st.write(f"Classpath: {classpath}")

    # Démarrer la JVM avec les fichiers JAR
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        *['-Xms512m', '-Xmx2048m'],     # Augmentation de la mémoire allouée à la JVM
        classpath = classpath
    )

    # Supprimer les variables une fois que la JVM est démarrée
    del current_dir, classpath, ucanaccess_jars


def convert_files(uploaded_files):

    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(uploaded_files)

    for i, uploaded_file in enumerate(uploaded_files):
        
        file_name = uploaded_file.name.split('.')[0]

        # Créer un fichier temporaire pour l'upload
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(uploaded_file.getbuffer())
            tmp_file_path = tmp_file.name
        
        # Fonction pour mettre à jour la barre de progression
        def update_progress(progress):
            current_progress = (i + progress) / total_files
            progress_bar.progress(current_progress)

        # Lire les données du fichier Access
        data = read_access_file(tmp_file_path, update_progress, status_text)
        # Sauvegarder les résultats intermédiaires
        csv_data, file_name = save_to_csv(data, f"{uploaded_file.name.split('.')[0]}.csv")
        st.session_state.converted_files[file_name] = csv_data

        check_memory()

        # Supprimer le fichier temporaire après traitement
        os.remove(tmp_file_path)

        # Effacer l'élément traité de uploaded_files
        uploaded_files = [file for file in uploaded_files if file != uploaded_file]  # Reconstruit la liste sans le fichier traité

    # Mise à jour de la barre de progression et du message
    progress_bar.progress(1.0)
    status_text.text("Converting complete!")

    progress_bar.empty()
    status_text.empty()

# Fonction pour lire des fichiers CSV/Excel et les concaténter
@st.cache
def read_and_concat_files(uploaded_files):
    combined_data = pd.DataFrame()
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(uploaded_files)

    # Message initial
    status_text.text("Processing...")

    for i, uploaded_file in enumerate(uploaded_files):
        current_progress = (i+1) / total_files
        progress_bar.progress(current_progress)

        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            data = pd.read_excel(uploaded_file)
        else:
            st.error(f"Unsupported file format: {uploaded_file.name}")
            continue
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    
    # Mise à jour du message après le traitement
    status_text.text("Processing complete!")
    progress_bar.empty()

    return combined_data





#########################
## Interface Streamlit ##
#########################


if not jpype.isJVMStarted():
    ucanaccess_jars = start_jvm()

# Interface utilisateur Streamlit
st.title("Application de conversion et concaténation de fichiers")

# Choix du mode d'utilisation
mode = st.selectbox("Choisissez une option", ["Conversion de fichiers Access en CSV", "Concaténation de fichiers CSV/Excel"])


if mode == "Conversion de fichiers Access en CSV":
    st.header("Conversion de fichiers Access en CSV")

    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0

    if 'converted_files' not in st.session_state:
        st.session_state.converted_files = {}
    
    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False

    def update_key():
        st.session_state.uploader_key += 1

    uploaded_files = st.file_uploader("Choisissez des fichiers .accdb en ne dépassant pas les 400MB", type="accdb", accept_multiple_files=True, key=f"uploader_{st.session_state.uploader_key}")
    
    check_memory()

    # Créer un bouton pour télécharger le fichier ZIP
    if (st.session_state.converted_files != {}) and (st.session_state.button_clicked is True):
        st.session_state.button_clicked = False             # Réinitialiser le bouton cliqué
        create_zip_file(st.session_state.converted_files)
        st.session_state.converted_files = {}

    elif uploaded_files and (st.session_state.button_clicked is False):
        if st.button("Convertir les fichiers"):
            st.session_state.button_clicked = True
            convert_files(uploaded_files)
            st.rerun()

        


            

elif mode == "Concaténation de fichiers CSV/Excel":
    st.header("Concaténation de fichiers CSV/Excel")
    uploaded_files = st.file_uploader("Choisissez des fichiers CSV ou Excel", type=["csv", "xlsx"], accept_multiple_files=True)

    if uploaded_files:

        combined_data = read_and_concat_files(uploaded_files)
        st.write("Données concaténées")
        st.write(combined_data)

        # Sauvegarder le fichier CSV final
        csv_data, final_file_name = save_to_csv(combined_data, "final_output.csv")
        st.download_button(label="Télécharger le fichier CSV final", data=csv_data, file_name=final_file_name, mime="text/csv")
