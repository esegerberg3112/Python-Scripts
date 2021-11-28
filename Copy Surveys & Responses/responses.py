import requests
import json
import io, os
import zipfile
import time
import datetime

# defining global variable for headers since is used by multiple API endpoints
def get_headers(loc_type):
    loc = loc_type + "_api_token"
    headers = {
        "X-API-TOKEN": os.environ.get(loc),
        "Content-Type": "application/json"
    }
    return headers
# function to kick off response export process
def start_export(survey_id):
    # setting the headers for this function, which will use the source brand API token
    headers = get_headers("source")
    url = "https://{dc}.qualtrics.com/API/v3/surveys/{id}/export-responses".format(
        dc=os.environ.get("source_dc"), id=survey_id)

    data = {
        "format": "csv",
        "timeZone": "America/Denver",
        "useLabels": True,
        "breakoutSets" : False
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    # catch non-200 response before polling for progress
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # Error, wasn't a 200
        print("Http error:", err)
        raise SystemExit(err)
    return response.text

# function to track the progress of an individual export
def export_progress(survey_id, progress_id):
    # setting the headers for this function, which will use the source brand API token
    headers = get_headers("source")
    # check if progress_id is null
    if not progress_id:
        print("Progress ID is null for the {survey} export. Retry".format(survey=survey_id))
        return

    url = "https://{dc}.qualtrics.com/API/v3/surveys/{id}/export-responses/{progress}".format(
        dc=os.environ.get("source_dc"), id=survey_id, progress=progress_id)

    response = requests.get(url, headers=headers)
    # catch non-200 responses
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # Error, wasn't a 200
        print("Http error:", err)
        raise SystemExit(err)
    # return response output text
    return response.text

# function to complete the export and return the file path to the export
def get_export_file(obj):
    # setting the headers for this function, which will use the source brand API token
    headers = get_headers("source")
    # grab the source survey ID of the object, start the export process
    survey_id = obj.sourceId
    progress_id = json.loads(start_export(survey_id))['result']['progressId']
    # keep polling for progress until progress call returns a status of "complete"
    file_id = ""
    status = ""
    while status != "complete":
        response = export_progress(survey_id, progress_id)
        # get the status of the progress poll from the response
        response_json = json.loads(response)
        status = response_json['result']['status']
        # large exports can take a bit, so pause in between requests
        if status != "complete":
            ct = datetime.datetime.now()
            print("Sleeping - " + ct)
            time.sleep(5)
        else:
            # if it's complete, grab the file ID for the final API call
            file_id = response_json['result']['fileId']

    export_file_url = "https://{dc}.qualtrics.com/API/v3/surveys/{id}/export-responses/{fileId}/file".format(
        dc=os.environ.get("source_dc"), id=survey_id, fileId=file_id)
    request_download = requests.get(export_file_url, headers=headers, stream=True)

    # file path where the unzipped files will go, save to the SurveyObject
    directory_path = ""
    # the actual file path will be the directory + the name of the original survey. Save that to the object
    survey_name = obj.surveyName
    obj.responsePath = directory_path + "/" + survey_name + ".csv"
    # unzip the downloaded file to the specified directory
    zipfile.ZipFile(io.BytesIO(request_download.content)).extractall(path=directory_path)
    return

def check_import(survey_id, progress_id):
    # setting the headers for this function, which will use the source brand API token
    headers = get_headers("dest")
    # make request to check the import status
    url = "https://{dc}.qualtrics.com/API/v3/surveys/{survey_id}/import-responses/{progress_id}".format(
        dc=os.environ.get("dest_dc"), survey_id=survey_id, progress_id=progress_id)
    request = requests.get(url, headers=headers)
    # catch non-200 response
    try:
        request.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # Error, wasn't a 200
        print("Http error:", err)
        raise SystemExit(err)
    return request.text

def start_import(obj):
    # pull the needed params from the objects attributes
    file_path = obj.responsePath
    survey_id = obj.destId

    headers = {
        "X-API-TOKEN": os.environ.get("dest_api_token"),
        "Content-Type": "text/csv; charset=UTF-8"
    }

    # set body for request
    data = open(file_path, 'rb').read()

    # start the import process
    url = "https://{dc}.qualtrics.com/API/v3/surveys/{id}/import-responses".format(
        dc=os.environ.get("dest_dc"), id=survey_id)
    request = requests.post(url, headers=headers, data=data)

    # catch non-200 response before polling for progress
    try:
        request.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # Error, wasn't a 200
        print("Http error:", err)
        raise SystemExit(err)

    # get the progress ID so can poll for progress until import is complete
    output = json.loads(request.text)
    progress_id = output['result']['progressId']
    status = ""

    # loop and check import progress until it is complete
    while status != "complete":
        response = check_import(survey_id, progress_id)
        # get the status of the progress poll from the response
        response_json = json.loads(response)
        status = response_json['result']['status']
        # large exports can take a bit, so pause in between progress requests
        if status != "complete":
            ct = datetime.datetime.now()
            print("Sleeping - " + ct)
            print()
            time.sleep(10)
    print("Finished the import for {id}".format(id=survey_id))
    return
