import platform
import subprocess
import os
import sys
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime
import pydicom
import urllib3
import shutil
import logging
from pathlib import Path
from collections import defaultdict
from threading import Timer
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import stat
import zipfile

logging.basicConfig(
    filename="scp.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

requests.urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

print(platform.system())


class FileHandler(FileSystemEventHandler):
    def __init__(
        self, output_dir, api_url, token, client_id, branch_id, user_id, max_workers=4
    ):
        self.output_dir = output_dir
        self.api_url = api_url
        self.token = token
        self.client_id = client_id
        self.branch_id = branch_id
        self.user_id = user_id
        self.study_files = defaultdict(list)
        self.upload_timer = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".dcm"):
            self.handle_file(event.src_path)

    def handle_file(self, file_path):
        try:
            # self.set_write_access(self.output_dir)
            self.set_write_access(file_path)
            dicom_data = pydicom.dcmread(file_path)
            study_instance_uid = dicom_data.StudyInstanceUID
            logging.info(
                f"Read: {file_path} from pydicom and this is the instance id: {study_instance_uid}\n"
            )

            self.study_files[study_instance_uid].append(file_path)
            logging.info(f"study_files is: {self.study_files}\n")

            if study_instance_uid in self.upload_timer:
                logging.info(f"study_instance_id already present in upload timer\n")
                self.upload_timer[study_instance_uid].cancel()

            self.upload_timer[study_instance_uid] = Timer(
                10.0, self.executor.submit, [self.process_study, study_instance_uid]
            )

            self.upload_timer[study_instance_uid].start()

        except Exception as e:
            logging.error(f"Error handling file: {file_path}, Error: {e}")

    def process_study(self, study_instance_uid):
        try:
            study_files = self.study_files.pop(study_instance_uid, [])
            if not study_files:
                return

            study_dir = os.path.join(self.output_dir, study_instance_uid)
            os.makedirs(study_dir, exist_ok=True)
            logging.info(f"created directory {study_dir}\n")

            for file_path in study_files:
                logging.info(f"file inside study files is {file_path}\n")
                new_file_path = os.path.join(study_dir, os.path.basename(file_path))
                logging.info(f"new file from file is {new_file_path}\n")
                os.rename(file_path, new_file_path)

            zip_file_path = shutil.make_archive(study_dir, "zip", study_dir)
            # zip_file_path = shutil.make_archive(study_dir, "zip", study_dir, compresslevel=1)

            logging.info(f"Zip: {zip_file_path} is completed\n")
            self.send_file(zip_file_path)

        except Exception as e:
            logging.error(f"Error processing study: {study_instance_uid}, Error: {e}")

    def send_file(self, zip_file_path):
        try:
            headers = {"Authorization": "Basic T3J0aGFuYzpPcnRoYW5jQDEyMzQ="}
            with open(zip_file_path, "rb") as f:
                logging.info(
                    f"Sending {zip_file_path} to orthanc instance api as an input\n"
                )
                response = requests.post(
                    self.api_url, headers=headers, data=f, verify=False
                )
                logging.info(
                    f"Response from orthanc_instances api is {response.text}\n"
                )

            if response.status_code == 200:
                logging.info(
                    f"Successfully sent file {zip_file_path} to orthanc_studies, create patient and create patient report apis\n"
                )
                self.upload(response.json())
                # Remove the zip file
                if os.path.isfile(zip_file_path):
                    os.remove(zip_file_path)
                    logging.info(f"Successfully removed zip file {zip_file_path}")

                # Remove the extracted directory
                extracted_dir_path = zip_file_path.replace(".zip", "")
                if os.path.isdir(extracted_dir_path):
                    shutil.rmtree(extracted_dir_path)
                    logging.info(f"Successfully removed directory {extracted_dir_path}")

            else:
                logging.info(
                    f"Failed to send file: {zip_file_path}, Status code: {response.status_code}, {response.json()}"
                )

        except Exception as e:
            logging.error(f"Error sending file: {zip_file_path}, Error: {e}")

    def upload(self, orthanc_response):
        try:
            for i in range(0, len(orthanc_response)):
                study_id = orthanc_response[i]["ParentStudy"]
                api_url = f"https://pacs.smaro.app/orthanc/studies/{study_id}"
                headers = {"Authorization": "Basic T3J0aGFuYzpPcnRoYW5jQDEyMzQ="}
                response = requests.get(api_url, headers=headers, verify=False)

                if response.status_code == 200:
                    logging.info(
                        "Got the patient level information successfully from orthanc_studies api"
                    )
                    patient_info = response.json()["PatientMainDicomTags"]
                    instance_id = response.json()["MainDicomTags"]["StudyInstanceUID"]
                    response_create_patient = self.create_patient(
                        patient_info,
                        response.json()["PatientMainDicomTags"]["PatientID"],
                    )
                    pat_id = response_create_patient.get("patient_id")
                    self.create_patient_report(pat_id, instance_id, study_id)
                else:
                    logging.info(
                        f"Failed to get the patient level information from orthanc_studies api, Status code: {response.status_code}, {response.json()}"
                    )

        except Exception as e:
            logging.error(f"Error uploading file Error: {e}")

    def create_patient(self, patient_info, patient_id):
        date_string = patient_info["PatientBirthDate"]
        name = patient_info["PatientName"]
        gender = patient_info["PatientSex"]
        # if date_string!='':
        #     date_object = datetime.datetime.strptime(date_string, f'%Y%m%d')
        # print(data_object)
        try:
            patient_data = {
                "ref_Code": "",
                "patient_name": name if name != "" else None,
                "patient_mobile": "",
                "patient_email": "",
                "dob": (
                    datetime.datetime.strptime(date_string, f"%Y%m%d").date()
                    if date_string != ""
                    else None
                ),
                "gender": gender if gender != "" else None,
                "address": "",
                "client_id": int(self.client_id),
                "branch_id": int(self.branch_id),
                "estatus": 1,
            }
            logging.info(f"Input to create_patient api is {patient_data}\n")
            headers = {"Content-Type": "application/json", "token": self.token}
            response = requests.post(
                "https://api.smaro.app/api/app/patient/create",
                json=patient_data,
                headers=headers,
                verify=False,
            )

            if response.json()["statusCode"] in (200, 409):
                logging.info(f"Response from create_patient api is {response.json()}\n")
                patient_id = response.json()["data"]["insertId"]
                return {"success": True, "patient_id": patient_id}
            else:
                logging.info(
                    f"Failed to upload create patient, Status code: {response.status_code}, {response.json()}\n"
                )
                return {"success": False, "patient_id": ""}

        except Exception as e:
            logging.error(f"Error uploading create patient. Error: {e}")
            return {"success": False, "patient_id": ""}

    def create_patient_report(self, patient_id, instance_id, study_id):
        try:
            report_data = {
                "patient_id": patient_id,
                "patient_study_id": study_id,
                "patient_study_instance_id": instance_id,
                "test_type_id": 0,
                "test_sub_type_id": 0,
                "priority_type": "normal",
                "techniques": "",
                "clinical_history": "",
                "clinical_history_file": None,
                "results_type": "",
                "doctor_id": 0,
                "hospital_id": 0,
                "client_login_id": self.user_id,
                "radiologist_id": 0,
                "client_id": self.client_id,
                "branch_id": self.branch_id,
            }
            logging.info(f"Input to create_patient_report api is {report_data}\n")
            headers = {"Content-Type": "application/json", "token": self.token}
            response = requests.post(
                "https://api.smaro.app/api/console/patient/report/create",
                json=report_data,
                headers=headers,
                verify=False,
            )

            if response.json()["statusCode"] in (200, 409):
                logging.info(
                    f"Response from create_patient_report api is {response.json()}\n"
                )
                # patient_id = response.json()['data']['insertId']
                return {"success": True}
            else:
                logging.info(
                    f"Failed to upload create patient report, Status code: {response.status_code}, {response.json()}\n"
                )
                return {"success": False}

        except Exception as e:
            logging.error(f"Error uploading create patient report. Error: {e}")
            return {"success": False}

    def set_write_access(self, folder_path):
        system = platform.system()
        if system == "Windows":
            # On Windows, set the folder to be writable
            os.chmod(folder_path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
            logging.info(f"Write access granted to '{folder_path}' on Windows.")
        else:
            # On Unix-based systems, set permissions to allow writing (e.g., 755 or 775)
            # This example sets permissions to rwxrwxr-x (775) allowing write access for owner and group
            os.chmod(folder_path, 0o775)
            logging.info(f"Write access granted to '{folder_path}' on Unix-like system (e.g., macOS).")

def start_storescp(port, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.environ["DCMDICTPATH"] = "dcmtk/share/dcmtk-3.6.8/dicom.dic"
    command = [
        "./dcmtk/bin/storescp",
        str(port),
        "--filename-extension",
        ".dcm",
        "--output-directory",
        output_dir,
        "--max-pdu",
        "46726",
        "+xa",
        "+B",
    ]

    storescp_process = subprocess.Popen(command)
    logging.info(f"storescp started with PID: {storescp_process.pid}")
    return storescp_process


def stop_storescp(storescp_process):
    storescp_process.terminate()
    storescp_process.wait()
    logging.info("storescp stopped.")


def start_monitoring(output_dir, api_url, token, client_id, branch_id, user_id):
    event_handler = FileHandler(
        output_dir, api_url, token, client_id, branch_id, user_id
    )
    observer = Observer()
    observer.schedule(event_handler, path=output_dir, recursive=True)
    observer.start()
    logging.info(f"Started monitoring {output_dir}")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# Example usage
if __name__ == "__main__":
    token = sys.argv[1]
    client_id = sys.argv[2]
    branch_id = sys.argv[3]
    user_id = sys.argv[4]
    port = 11114  # Port to listen on
    output_dir = "./output/"  # Directory to store received DICOM files
    api_url = (
        "https://pacs.smaro.app/orthanc/instances"  # Replace with your API endpoint
    )
    storescp_process = start_storescp(port, output_dir)
    logging.info("storescp started. Press Ctrl+C to stop.")
    start_monitoring(output_dir, api_url, token, client_id, branch_id, user_id)
    stop_storescp(storescp_process)
