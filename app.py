import streamlit as st
import pandas as pd
import jaydebeapi
from io import BytesIO
import os
import jpype
import tempfile
import zipfile
import logging
import gc

logging.basicConfig(level=logging.INFO)

###########################
## Utility Functions ##
###########################

def get_metadata(db_path, ucanaccess_jars):
    conn = None
    metadata = {}
    progress_bar = st.progress(0)
    try:
        with st.status("Retrieving metadata...", expanded=True) as status:
            st.write("Connecting to database...")
            conn_string = f"jdbc:ucanaccess://{db_path}"
            conn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver", 
                                    conn_string, 
                                    [], 
                                    ucanaccess_jars[0])
            meta = conn.jconn.getMetaData()
            rs = meta.getTables(None, None, "%", ["TABLE"])

            while rs.next():
                table_name = str(rs.getString("TABLE_NAME"))
                columns_rs = meta.getColumns(None, None, table_name, None)
                column_names = []
                while columns_rs.next():
                    column_name = str(columns_rs.getString("COLUMN_NAME"))
                    column_names.append(column_name)
                metadata[table_name] = column_names
            
            status.update(label="Metadata retrieval complete!", state="complete", expanded=False)

        del meta
        del rs
        del table_name
        del columns_rs
        del column_name
        del column_names

    except Exception as e:
        st.error(f"Error processing {db_path}: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
                del conn
            except Exception as e:
                st.error(f"Failed to close connection for {db_path}: {str(e)}")
                logging.error(f"Failed to close connection for {db_path}: {str(e)}")
    
    progress_bar.progress(1.0)
    
    return metadata


def read_access_file(db_path, ucanaccess_jars, table_name, selected_columns, progress_callback=None):
    conn = None
    cursor = None
    data_frame = pd.DataFrame()
    try:
        conn_string = f"jdbc:ucanaccess://{db_path}"
        if progress_callback:
            progress_callback(0.1)

        conn = jaydebeapi.connect("net.ucanaccess.jdbc.UcanaccessDriver", conn_string, [], ucanaccess_jars[0])
        if progress_callback:
            progress_callback(0.3)
        
        cursor = conn.cursor()

        columns_str = ", ".join(selected_columns)
        sql = f"select {columns_str} from {table_name}"

        cursor.execute(sql)
        if progress_callback:
            progress_callback(0.5)
        
        results = cursor.fetchall()
        data_frame = pd.DataFrame(results, columns=selected_columns)

        if progress_callback:
            progress_callback(0.8)
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

def save_to_csv(data, file_name):
    output = BytesIO()
    data.to_csv(output, index=False)
    return output.getvalue(), file_name

def update_key():
    st.session_state.uploader_key += 1

def create_zip_file(files_dict):
    zip_buffer = BytesIO()
    try:
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for file_name, file_data in files_dict.items():
                zip_file.writestr(file_name, file_data)
        zip_buffer.seek(0)
        if st.download_button(
            label="Download all converted files",
            data=zip_buffer,
            file_name="converted_files.zip",
            mime="application/zip"
            ):
            update_key()
            st.session_state.button_download_clicked = True
            st.session_state.uploaded_files = None
            st.session_state.converted_files = {}
            st.session_state.meta_data = {}
            st.session_state.button_download_clicked = True
            gc.collect()
            st.rerun()

    except Exception as e:
        st.error(f"Failed to create zip file: {str(e)}")

def convert_files(uploaded_files, table_name, selected_columns, ucanaccess_jars):
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(uploaded_files)
    status_text.text("Converting...")

    for i, uploaded_file in enumerate(uploaded_files):
        file_name = uploaded_file.name.split('.')[0]
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(uploaded_file.getbuffer())
            tmp_file_path = tmp_file.name

        try:
            def update_progress(progress):
                current_progress = (i + progress) / total_files
                progress_bar.progress(current_progress)
            data = read_access_file(tmp_file_path, ucanaccess_jars, table_name, selected_columns, update_progress)
            csv_data, file_name = save_to_csv(data, f"{uploaded_file.name.split('.')[0]}_{table_name}.csv")
            st.session_state.converted_files[file_name] = csv_data

            del data
            del csv_data
            del file_name
 
        except Exception as e:
            st.error(f"Failed to process {uploaded_file.name}: {str(e)}")
        finally:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

        status_text.text(f"Converting... ({i + 1}/{total_files})")

    progress_bar.progress(1.0)
    status_text.text("Converting complete!")
    progress_bar.empty()
    status_text.empty()


#########################
## Streamlit Interface ##
#########################

st.title("Access File Conversion to CSV")


ucanaccess_jars = [
        'ucanaccess-5.0.1.jar',
        os.path.join('loader', 'ucanload.jar'),
        os.path.join('lib', 'commons-lang3-3.8.1.jar'),
        os.path.join('lib', 'commons-logging-1.2.jar'),
        os.path.join('lib', 'hsqldb-2.5.0.jar'),
        os.path.join('lib', 'jackcess-3.0.1.jar')
    ]

if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = None

if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0

if 'converted_files' not in st.session_state:
    st.session_state.converted_files = {}

if 'button_convert_clicked' not in st.session_state:
    st.session_state.button_convert_clicked = False

if 'meta_data' not in st.session_state:
    st.session_state.meta_data = {}

if 'button_download_clicked' not in st.session_state:
    st.session_state.button_download_clicked = False



st.session_state.uploaded_files = st.file_uploader("Choose .accdb files", type="accdb", accept_multiple_files=True, key=f"uploader_{st.session_state.uploader_key}")

if st.session_state.uploaded_files and not st.session_state.meta_data and (st.session_state.button_download_clicked is False):
    
    if not jpype.isJVMStarted():

        current_dir = os.getcwd()
        classpath = ":".join([os.path.join(current_dir, "UCanAccess-5.0.1.bin", jar) for jar in ucanaccess_jars])

        # Start the JVM
        jpype.startJVM(jpype.getDefaultJVMPath(), *['-Xms512m', '-Xmx2048m'], classpath=classpath)
        
        del current_dir
        del classpath

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(st.session_state.uploaded_files[0].getbuffer())
        tmp_file_path = tmp_file.name

    st.session_state.meta_data = get_metadata(tmp_file_path, ucanaccess_jars)
    os.remove(tmp_file_path)


if st.session_state.meta_data and (st.session_state.button_convert_clicked is False):

    st.write("Select the table and columns you want to extract:")

    table_name = st.selectbox("Table", list(st.session_state.meta_data.keys()))

    if table_name in st.session_state.meta_data:
        selected_columns = st.multiselect("Columns", st.session_state.meta_data[table_name], default=st.session_state.meta_data[table_name])
    else:
        st.error(f"Table {table_name} not found in the database metadata.")
    
    if st.button("Convert Files"):
        st.session_state.button_convert_clicked = True
        convert_files(st.session_state.uploaded_files, table_name, selected_columns, ucanaccess_jars)
    
if st.session_state.converted_files and (st.session_state.button_convert_clicked is True):
    st.session_state.button_convert_clicked = False
    create_zip_file(st.session_state.converted_files)
