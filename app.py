import streamlit as st
import pandas as pd
import jaydebeapi
from io import BytesIO
import os
import jpype
import tempfile
import zipfile
import psutil
import dask.dataframe as dd
import logging

logging.basicConfig(level=logging.INFO)

###########################
## Fonctions utilitaires ##
###########################


# Fonction pour configurer Java
def setup_java():
    
    # Ajouter des chemins communs pour OpenJDK 11
    java_home_paths = [
        '/usr/lib/jvm/java-11-openjdk-amd64',  # Chemin commun pour OpenJDK 11
        '/usr/lib/jvm/default-java'  # Chemin alternatif pour default-java
    ]
    
    # Vérifier et définir JAVA_HOME
    for path in java_home_paths:
        if os.path.exists(path):
            os.environ['JAVA_HOME'] = path
            break
    
    if 'JAVA_HOME' in os.environ:
        jvm_path = os.path.join(os.environ['JAVA_HOME'], 'lib', 'server', 'libjvm.so')
        if os.path.exists(jvm_path):
            os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']
            return jvm_path
        else:
            st.error(f"libjvm.so not found in {jvm_path}.")
            return None
    else:
        st.error("JAVA_HOME is not set.")
        return None


# Fonction pour lire un fichier Access et récupérer les données spécifiques
def read_access_file(db_path, ucanaccess_jars, chunk_size=10000):
    conn = None
    cursor = None
    data_frames = []

    try:
        # Définition de la chaine de connexion
        conn_string = f"jdbc:ucanaccess://{db_path}"

        # Connexion à la base de données
        conn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver", 
                                  conn_string, 
                                  [], 
                                  ucanaccess_jars[0])

        cursor = conn.cursor()
        
        # Exécuter une requête SQL et stocker les résultats dans un DataFrame pandas
        sql = "select Date_Jour_H_M_d, PDM, TV_corrige, TV_brut from Trafic_Minute"

        cursor.execute(sql)

        while True:
            results = cursor.fetchmany(chunk_size)
            if not results:
                break
            chunk_df = pd.DataFrame(results, columns=["Date_Jour_H_M_d", "PDM", "TV_corrige", "TV_brut"])
            data_frames.append(chunk_df)
        #results = cursor.fetchall()
        data_frame = pd.concat(data_frames, ignore_index=True)

    
    except Exception as e:
        st.error(f"Error processing {db_path}: {str(e)}")

    finally:
        if cursor:
            try:
                cursor.close()
                del cursor
            except Exception as e:
                st.error(f"Failed to close cursor for {db_path}: {str(e)}")
                logging.error(f"Failed to close cursor for {db_path}: {str(e)}")
        if conn:
            try:
                conn.close()
                del conn
            except Exception as e:
                st.error(f"Failed to close connection for {db_path}: {str(e)}")
                logging.error(f"Failed to close connection for {db_path}: {str(e)}")
    
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
            zip_file.writestr(file_name, file_data)
    zip_buffer.seek(0)

    # Créer un bouton pour télécharger le fichier ZIP
    if st.download_button(
        label="Télécharger tous les fichiers convertis",
        data=zip_buffer,
        file_name="converted_files.zip",
        mime="application/zip"
    ):
        update_key()
        


def convert_files(uploaded_file):

    # Obtenir le répertoire de travail courant
    current_dir = os.getcwd()

    # Afficher le répertoire courant pour le débogage
    #st.write(f"Current directory: {current_dir}")
    #st.write(f"Fichiers présents dans le répertoire : {os.listdir(current_dir)}")

    # Liste des fichiers JAR nécessaires pour UCanAccess
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

    if not jpype.isJVMStarted():
        #st.write("Starting JVM...")
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            *['-Xms512m', '-Xmx2048m'],     # Augmentation de la mémoire allouée à la JVM
            #classpath = "-Djava.class.path=" + classpath
            classpath = classpath
            )
        #st.write(f"JVM started successfully")
    
    del classpath

    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(uploaded_files)

    # Message initial
    status_text.text("Converting...")

    i = 0

    while uploaded_files:
        uploaded_file = uploaded_files.pop(0)
        file_name = uploaded_file.name.split('.')[0]

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(uploaded_file.getbuffer())
            tmp_file_path = tmp_file.name

        try:
            data = read_access_file(tmp_file_path, ucanaccess_jars)
            ddf = dd.from_pandas(data, npartitions=4)
            csv_data, file_name = save_to_csv(ddf.compute(), f"{uploaded_file.name.split('.')[0]}.csv")
            st.session_state.converted_files[file_name] = csv_data
            del csv_data
            del file_name
        except Exception as e:
            st.error(f"Failed to process {uploaded_file.name}: {str(e)}")
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            
            current_progress = i + 1
            status_text.text(f"Converting... ({current_progress}/{total_files})")
            progress_bar.progress(current_progress/total_files)
            i += 1

    
    del ucanaccess_jars

    # Mise à jour de la barre de progression et du message
    progress_bar.progress(1.0)
    status_text.text("Converting complete!")

    progress_bar.empty()
    status_text.empty()

# Fonction pour lire des fichiers CSV/Excel et les concaténter
def read_and_concat_files(uploaded_files):
    combined_data = pd.DataFrame()
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(uploaded_files)

    # Message initial
    status_text.text("Processing...")

    for i, uploaded_file in enumerate(uploaded_files):

        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            data = pd.read_excel(uploaded_file)
        else:
            st.error(f"Unsupported file format: {uploaded_file.name}")
            continue
        combined_data = pd.concat([combined_data, data], ignore_index=True)

        current_progress = (i+1) / total_files
        status_text.text(f"Processing... {current_progress*100:.2f}%")
        progress_bar.progress(current_progress)
    
    # Mise à jour du message après le traitement
    progress_bar.progress(1.0)
    status_text.text("Processing complete!")

    progress_bar.empty()
    status_text.empty()

    return combined_data





#########################
## Interface Streamlit ##
#########################

# Appeler la fonction pour s'assurer que Java est configuré
#jvm_path = setup_java()

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

    uploaded_files = st.file_uploader("Choisissez des fichiers .accdb", type="accdb", accept_multiple_files=True, key=f"uploader_{st.session_state.uploader_key}")

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
        st.write(combined_data.head())

        # Sauvegarder le fichier CSV final
        csv_data, final_file_name = save_to_csv(combined_data, "final_output.csv")
        st.download_button(label="Télécharger le fichier CSV final", data=csv_data, file_name=final_file_name, mime="text/csv")
