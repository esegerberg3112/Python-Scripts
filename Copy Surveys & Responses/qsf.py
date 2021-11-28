import requests
import json
import os

# function to take a JSON object of data, and write it to a QSF file to be imported
def write_to_qsf(data, survey_id, directory):
    path = directory + survey_id + "_qsf.qsf"
    with open(path, "w") as output_file:
        json.dump(data, output_file)
    return path

# function to download a QSF (Qualtrics Survey Format) for the specified survey
def download_qsf(obj):
    headers = {
        "X-API-TOKEN": os.environ.get("source_api_token"),
        "Content-Type": "application/json"
    }
    params = {
        'format': 'qsf'
    }
    # set the survey ID from the object attribute 'sourceId', make API request
    survey_id = obj.sourceId
    request_url = "https://{dc}.qualtrics.com/API/v3/survey-definitions/{id}".format(
        dc=os.environ.get("source_dc"), id=survey_id)
    request = requests.get(request_url, headers=headers, params=params)
    try:
        request_obj = json.loads(request.text)
    except ValueError as ve:
        print("The qsf data is not valid JSON")
        raise

    # create a QSF file from the output of the API call, add to directory for files
    directory = '/Users/esegerberg/PycharmProjects/PublishingTesting/qsf_files/'
    qsf_data = request_obj['result']
    survey_name = request_obj['result']['SurveyEntry']['SurveyName']
    obj.surveyName = survey_name

    # call function to create a QSF file for the given survey in the specified directory
    qsf_path = write_to_qsf(qsf_data, survey_id, directory)
    obj.qsfPath = qsf_path
    return

def import_qsf(obj):
    qsf_path = obj.qsfPath
    survey_name = obj.surveyName
    files = {
        'file': (qsf_path, open(qsf_path, 'rb'), 'application/vnd.qualtrics.survey.qsf')
    }

    headers = {
        "X-API-TOKEN": os.environ.get("dest_api_token")
    }

    data = {
        "name": survey_name + " - new"
    }

    request_url = "https://{dc}.qualtrics.com/API/v3/surveys".format(dc=os.environ.get("dest_dc"))
    response = requests.post(request_url, files=files, data=data, headers=headers)
    dest_id = json.loads(response.text)['result']['id']
    obj.destId = dest_id
    return