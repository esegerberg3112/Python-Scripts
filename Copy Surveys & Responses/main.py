# Program to take an Excel file of Survey ID's that exist in one Qualtrics brand,
# and then to copy those surveys and their response data to a separate Qualtrics brand
import os
import responses
import qsf
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time
import threading

# Defining the survey object to hold the relevant QSF/response data
class SurveyObject:
    """ class object created per survey

    Attributes:
        sourceId -- the survey ID from the originating brand
        qsfPath -- file path to the QSF that is downloaded for each survey
        responsePath -- file path to the downloaded CSV containing responses for each survey
        destId -- the survey ID of the newly created survey in the destination brand
        surveyName -- name of the survey that is being copied from the original brand

     """
    def __init__(self, survey):
        self.sourceId = survey
        self.qsfPath = ""
        self.responsePath = ""
        self.destId = ""
        self.surveyName = ""

# defining a multi-threading function to speed up API call processing, want it to be re-usable
# making it dynamic since will be hitting multiple endpoints for multiple functions that need threading
def runner(func_name, obj_list):
    threads = []
    check_function = str(func_name)
    with ThreadPoolExecutor(max_workers=20) as executor:
        for obj in obj_list:
            threads.append(executor.submit(func_name, obj))
        executor.shutdown(wait=True)

        # print out notifications when each step of the process is complete
        if "download" in check_function:
            print("All QSF's have been downloaded.\n")
        if "import_qsf" in check_function:
            print("All QSF's have been imported.\n")
        if "export" in check_function:
            print("All response files have been exported.\n")
        if "start_import" in check_function:
            print("All response imports have been started.\n")
    return obj_list

def main():
    # defining environment variables for the source and destination brands
    os.environ["source_api_token"] = ""
    os.environ["dest_api_token"] = ""
    os.environ["source_dc"] = ""
    os.environ["dest_dc"] = ""

    # specify path for file that contains survey IDs
    file_path = ''

    # process the file and try creating a data frame object for the survey column
    try:
        processed_file = pd.DataFrame(pd.read_csv(file_path), columns=['SurveyID'])
    except Exception as e:
        print(e)
        raise SystemExit(e)
    # create a Survey Object for each ID
    id_list = processed_file['SurveyID'].values.tolist()
    obj_list = [SurveyObject(survey_id) for survey_id in id_list]

    # Perform the following actions using other modules and the ThreadPoolExecutor
    # 1. download the QSF from the original brand
    # 2. import the QSF into the destination brand
    # 3. export the responses from the survey in the original brand
    # 4. import the responses into the new survey in the destination brand
    runner(qsf.download_qsf, obj_list)
    runner(qsf.import_qsf, obj_list)
    runner(responses.get_export_file, obj_list)
    runner(responses.start_import, obj_list)
    return

if __name__ == "__main__":
    main()
