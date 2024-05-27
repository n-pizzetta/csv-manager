import streamlit as st
import pandas as pd
import jaydebeapi
from io import BytesIO
import os
import jpype
import tempfile

# Charger les variables d'environnement depuis ~/.profile
if os.path.exists(os.path.expanduser("~/.profile")):
    with open(os.path.expanduser("~/.profile")) as f:
        env_vars = f.readlines()
    for var in env_vars:
        key, value = var.strip().split("=")
        os.environ[key] = value

# Vérifier si JAVA_HOME est défini
if 'JAVA_HOME' not in os.environ:
    st.error("JAVA_HOME is not set. Make sure setup.sh is executed correctly.")

# Fonction pour lire un fichier Access et récupérer les données spécifiques
def read_access_file(db_path, classpath, progress_callback=None):
    conn = None
    cursor = None
    data_frame = pd.DataFrame()

    try:
        # Définition de la chaine de connexion
        conn_string = f"jdbc:ucanaccess://{db_path};newDatabaseVersion=V2010"

        if progress_callback:
            progress_callback(0.1)  # 10% du processus terminé (connexion définie)

        # Connexion à la base de données
        conn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver", 
                                  conn_string, 
                                  [], 
                                  classpath)
        
        if progress_callback:
            progress_callback(0.3)  # 30% du processus terminé (connexion établie)

        cursor = conn.cursor()
        
        # Exécuter une requête SQL et stocker les résultats dans un DataFrame pandas
        cursor.execute("SELECT Date_Jour_H_M_d, PDM, TV_corrige, TV_brut FROM Trafic_Minute")
        results = cursor.fetchall()
        data_frame = pd.DataFrame(results, columns=[column[0] for column in cursor.description])

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

# Fonction pour lire des fichiers CSV/Excel et les concaténer
def read_and_concat_files(uploaded_files):
    combined_data = pd.DataFrame()
    progress_bar = st.progress(0)
    total_files = len(uploaded_files)

    for i, uploaded_file in enumerate(uploaded_files):
        current_progress = i / total_files
        progress_bar.progress(current_progress)

        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            data = pd.read_excel(uploaded_file)
        else:
            st.error(f"Unsupported file format: {uploaded_file.name}")
            continue
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    
    # Mise à jour de la barre de progression pour terminer l'itération
    progress_bar.progress(1.0)

    return combined_data

# Interface utilisateur Streamlit
st.title("Application de conversion et concaténation de fichiers")

# Choix du mode d'utilisation
mode = st.selectbox("Choisissez une option", ["Conversion de fichiers Access en CSV", "Concaténation de fichiers CSV/Excel"])

if mode == "Conversion de fichiers Access en CSV":
    st.header("Conversion de fichiers Access en CSV")
    uploaded_files = st.file_uploader("Choisissez des fichiers .accdb", type="accdb", accept_multiple_files=True)

    if uploaded_files:
        # Liste des fichiers JAR nécessaires pour UCanAccess
        ucanaccess_jars = [
            'UCanAccess-5.0.1.bin/ucanaccess-5.0.1.jar',
            'UCanAccess-5.0.1.bin/loader/ucanload.jar',
            'UCanAccess-5.0.1.bin/lib/commons-lang3-3.8.1.jar',
            'UCanAccess-5.0.1.bin/lib/commons-logging-1.2.jar',
            'UCanAccess-5.0.1.bin/lib/hsqldb-2.5.0.jar',
            'UCanAccess-5.0.1.bin/lib/jackcess-3.0.1.jar'
        ]

        # Concaténer les chemins des fichiers JAR correctement
        classpath = ":".join(ucanaccess_jars)

        # Vérifier si la JVM est déjà démarrée
        if not jpype.isJVMStarted():
            jvm_path = jpype.getDefaultJVMPath()
            if not jvm_path:
                st.error("JVM path not found. Make sure JAVA_HOME is set correctly.")
            else:
                jpype.startJVM(jvm_path, f"-Djava.class.path={classpath}")
        
        progress_bar = st.progress(0)
        total_files = len(uploaded_files)

        for i, uploaded_file in enumerate(uploaded_files):
            # Créer un fichier temporaire pour l'upload
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(uploaded_file.getbuffer())
                tmp_file_path = tmp_file.name
            
            # Fonction pour mettre à jour la barre de progression
            def update_progress(progress):
                current_progress = (i + progress) / total_files
                progress_bar.progress(current_progress)

            # Lire les données du fichier Access
            data = read_access_file(tmp_file_path, classpath, update_progress)
            
            # Sauvegarder les résultats intermédiaires
            csv_data, file_name = save_to_csv(data, f"{uploaded_file.name.split('.')[0]}.csv")
            st.download_button(label=f"Télécharger le fichier CSV pour {uploaded_file.name}", data=csv_data, file_name=file_name, mime="text/csv")
            
            # Supprimer le fichier temporaire après traitement
            os.remove(tmp_file_path)

            # Mise à jour de la barre de progression pour terminer l'itération
            update_progress(1.0)

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
