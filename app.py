import streamlit as st
import pandas as pd
import jaydebeapi
from io import BytesIO
import os
import jpype
import tempfile
import zipfile
import gc


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
def read_access_file(db_path, ucanaccess_jars):
    conn = None
    cursor = None
    data_frame = pd.DataFrame()

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
        cursor.execute("SELECT Date_Jour_H_M_d, PDM, TV_corrige, TV_brut FROM Trafic_Minute")
        results = cursor.fetchall()
        data_frame = pd.DataFrame(results, columns=[column[0] for column in cursor.description])
    
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


# Fonction pour créer un fichier ZIP et ajouter des fichiers
def add_files_to_zip(zip_obj, files):
    for file_name, file_data in files.items():
        zip_obj.writestr(file_name, file_data)

@st.experimental_fragment
def download_zip_file(zip_buffer, file_name):
    st.download_button(
        label="Télécharger tous les fichiers convertis",
        data=zip_buffer,
        file_name=file_name,
        mime="application/zip"
        )


# Fonction pour lire des fichiers CSV/Excel et les concaténter
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


#########################
## Interface Streamlit ##
#########################

# Appeler la fonction pour s'assurer que Java est configuré
# jvm_path = setup_java()

# Interface utilisateur Streamlit
st.title("Application de conversion et concaténation de fichiers")

# Choix du mode d'utilisation
mode = st.selectbox("Choisissez une option", ["Conversion de fichiers Access en CSV", "Concaténation de fichiers CSV/Excel"])

if mode == "Conversion de fichiers Access en CSV":
    st.header("Conversion de fichiers Access en CSV")
    uploaded_files = st.file_uploader("Choisissez des fichiers .accdb", type="accdb", accept_multiple_files=True)

    if uploaded_files:


        # Obtenir le répertoire de travail courant
        current_dir = os.getcwd()

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

        if not jpype.isJVMStarted():
            jpype.startJVM(
                jpype.getDefaultJVMPath(),
                *['-Xms512m', '-Xmx2048m'],
                classpath=classpath
            )

        progress_bar = st.progress(0)
        status_text = st.empty()
        total_files = len(uploaded_files)

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            batch_size = 3
            for i in range(0, total_files, batch_size):
                batch = uploaded_files[i:i + batch_size]
                temp_files_batch = {}

                for uploaded_file in batch:

                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        tmp_file.write(uploaded_file.getbuffer())
                        tmp_file_path = tmp_file.name

                    data = read_access_file(tmp_file_path, ucanaccess_jars)
                    csv_data, csv_file_name = save_to_csv(data, uploaded_file.name)
                    temp_files_batch[csv_file_name] = csv_data

                    os.remove(tmp_file_path)

                add_files_to_zip(zipf, temp_files_batch)

                for temp_file_path in temp_files_batch.values():
                    del temp_file_path

                gc.collect()

                progress_bar.progress((i + batch_size) / total_files)
        
        status_text.text("Conversion terminée!")
        zip_buffer.seek(0)

        # Proposer le téléchargement du fichier ZIP final

        download_zip_file(zip_buffer, "converted_files.zip")
        
        progress_bar.empty()
        status_text.empty()

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
