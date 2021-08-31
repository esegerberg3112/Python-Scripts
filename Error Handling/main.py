import requests
import jsonschema
import pandas as pd
import json
from datetime import datetime, timedelta
import os

# creating a class object to define each specific survey and persist data through various functions
class SurveyObject:
    """ object created per config file to persist relevant info

    Attributes:
        fields -- the ticket/survey fields that need to be checked per survey, specified by config file
        survey -- survey that needs to be checked
        query -- search query to find tickets specific to a certain survey
        lastRun -- last run date of the automation
        token -- api token for the user account that contains the survey
        dc -- data center the brand that contains the surveys is on
        errors -- list of responses that have incomplete data
    """
    def __init__(self, tktFields, surveyId, querySearch, lastRun, apiToken, dc):
        self.fields = tktFields
        self.survey = surveyId
        self.query = querySearch
        self.lastRun = lastRun
        self.token = apiToken
        self.dc = dc
        self.errors = []
    # add any mismatching responses that need to be reviewed to a list
    def append_errors(self, responseId):
        self.errors.append(responseId)
    # return list of mismatching errors
    def return_errors(self):
        return self.errors
    # print list of mismatching errors
    def print_errors(self):
        print(str(self.errors))
        return

# API gives default error message, but want to be able to catch errors and handle accordingly with custom class
class ApiResponseError(Exception):
    """ Exception raised for errors in the API call (non-200 responses)

    Attributes:
        error_code -- HTTP status code
        message -- description returned for the given HTTP status code

    Methods:
        __str__ -- overwriting default str method for a custom print
    """
    def __init__(self,error_code, message):
        self.error_code = error_code
        self.message = message

    def __str__(self):
        return '{code}, {message}'.format(code=self.error_code,message=self.message)

# transform string to ISO date format object w/ Zulu offset (what the API ingests)
def iso_format_object(date_string):
    datetime_obj = datetime.strptime(date_string,'%Y-%m-%dT%H:%M:%S.%fZ')
    return datetime_obj

# transform ISO date object into string w/ Zulu offset
def iso_format_string(date_obj):
    strDate = date_obj.isoformat() + ".000Z"
    return strDate

# add 1 week to last run date to update config file with after job runs
def add_one_week(date_obj):
    new_date = date_obj + timedelta(days=7)
    return new_date

# function to get config parameters from file (returned as JSON object)
def list_config_info(file):
    schema = {
        "type": "object",
        "required": ["ticketFields", "config"]
    }
    # read in JSON file and extract data needed. Throw errors if ill-formatted
    with open(file) as json_file:
        try:
            data = json.load(json_file)
            jsonschema.validate(data, schema)
            return data['config']
        except jsonschema.exceptions.ValidationError:
            return
        except json.decoder.JSONDecodeError:
            return

# function to get the ticket/survey data fields to compare (returned as JSON object)
def list_ticket_fields(file):
    schema = {
        "type": "object",
        "required": ["ticketFields", "config"]
    }
    # read in JSON file and extract data needed. Throw errors if ill-fortmated
    with open(file) as json_file:
        try:
            data = json.load(json_file)
            jsonschema.validate(data, schema)
            return data['ticketFields']
        except jsonschema.exceptions.ValidationError:
            return
        except json.decoder.JSONDecodeError:
            return

# function to raise an exception if API doesn't return a 200
def check_request(request_object):
    http_status = request_object['meta']['httpStatus']
    # check request status for ticket API
    if '200' not in http_status:
        error_message = request_object['meta']['error']['errorMessage']
        raise ApiResponseError(http_status, error_message)
    return

# for configs where data is incomplete, create an output file for each run
def create_csv(run_dict):
    # create a data frame, and then create a csv based on that data frame for the current run
    data_frame = pd.DataFrame.from_dict(run_dict, orient='index')
    current_date = datetime.today().strftime('%Y_%m_%d')
    file_path = "/ErrorHandling/ReviewFiles/{file}.csv".format(file="run_"+current_date)
    data_frame.transpose().to_csv(file_path, index=False)
    return

# write lastRunDate back to config files
def write_config_params(file, nextRunDate):
    with open(file, 'r') as json_file:
        data = json.load(json_file)
    data['config']['lastRunDate'] = nextRunDate
    with open(file, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    return

# function to compare response and ticket data
def data_comparison(survey_object, tkt_key, headers, dc):
    # pull the ticket data for the given key
    tkt_url = "https://{dc}.qualtrics.com/API/v3/tickets/{key}".format(dc=dc, key=tkt_key)
    tkt_request = requests.get(tkt_url, headers=headers)
    tkt_data_object = json.loads(tkt_request.text)                                     # object containing ticket data
    # check request status for ticket API. If errors out, move onto the next ticket
    try:
        check_request(tkt_data_object)
    except ApiResponseError as e:
        print("GET Ticket error: " + str(e))
        return

    # get associated response info from ticket and then build 2nd API call
    response_id = tkt_data_object['result']['responseId']
    survey_id = tkt_data_object['result']['sourceId']

    # pull the response data for the associated responses
    response_url = "https://{dc}.qualtrics.com/API/v3/surveys/{sid}/responses/{rid}".format(dc=dc, sid=survey_id, rid=response_id)
    response_request = requests.get(response_url, headers=headers)
    response_data_object = json.loads(response_request.text)                           # object containing response data
    # check request status for ticket API. If errors out, move onto the next ticket
    try:
        check_request(response_data_object)
    except ApiResponseError as e:
        print("GET Response error: " + str(e))
        return

    # for the ticket, we have gotten the survey and ticket data. Now, need to check that the fields (specified by the
    # config files) match between the 2 data sources
    list_of_fields = survey_object.fields
    for dict_val in list_of_fields:
        tkt_field = tkt_data_object['result'][dict_val['ticketRecordField']]
        survey_field = response_data_object['result']['values'][dict_val['primarySurveyEmbeddedField']]
        if dict_val['ticketRecordField'] == "status":
            if tkt_field == 2 and survey_field == "Closed":
                continue
        if str(tkt_field) != str(survey_field):
            return response_id
    return

# function to list the closed ticket for each program
def find_mismatched_responses(survey_object):
    # set parameters
    dc = survey_object.dc
    token = survey_object.token
    last_run = survey_object.lastRun
    query = survey_object.query
    # set header for API request
    headers = {
        "X-API-TOKEN": token,
        "Content-Type": "application/json"
    }
    # API request URL
    request_url = "https://{dc}.qualtrics.com/API/v3/tickets/search".format(dc=dc)
    # set the body for the Search Tickets request
    body = {
        "query": {
            "queryType": "compound",
            "comparison": "and",
            "children": [
                {
                    "queryType": "closedAt",
                    "comparison": "gt",
                    "value": last_run
                },
                {
                    "queryType": "name",
                    "comparison": "contains",
                    "value": query
                }
            ]
        }
    }
    # make API request, allowing for pagination of response (max 50 per page)
    while request_url is not None:
        request = requests.post(request_url, data=json.dumps(body), headers=headers)
        request_text = json.loads(request.text)
        # check request status for search tickets API success
        check_request(request_text)

        # checking for a non-zero # of tickets
        if len(request_text['result']['elements']) == 0:
            print("{survey} has no Closed Tickets since {date}".format(date=last_run, survey=survey_object.survey))
            return

        ticket_list = request_text['result']['elements']
        # iterate through each closed ticket return from the query
        for tkt in ticket_list:
            key = tkt['key']
            response_error = data_comparison(survey_object, key, headers, dc)
            if response_error is not None:
                survey_object.append_errors(response_error)
        request_url = request_text['result']['links']['next']['href']
    # get the survey objects list of response ID errors and return
    mismatched_responses = survey_object.return_errors()
    return mismatched_responses

def main():
    # specify path for where to find config files
    config_files_path = '/ErrorHandling/ConfigFiles'
    config_file_list = []
    # get all files in the specified directory, if it exists
    try:
        next(os.walk(config_files_path))
        for root, dirs, files in os.walk(config_files_path):
            config_file_list = [os.path.join(root, name) for name in files]
    except StopIteration:
        print("That directory path doesn't exist. Please update in code (line 211).")

    # check if no files
    if len(config_file_list) == 0:
        print("That directory has no config files.")
        return None

    # iterate through all config files and create an object for each unique file/survey. Add to list
    obj_list = {}
    for file in config_file_list:
        # check for if json file is not configured correctly for 2 dictionaries we pull
        try:
            tkt_fields = list_ticket_fields(file)
            if tkt_fields is None:
                raise TypeError
        except TypeError:
            print("File {file} has invalid JSON, please correct.".format(file=file.split("/")[-1]))
            return

        try:
            survey_id = list_config_info(file)['surveyId']
            if survey_id is None:
                raise TypeError
        except TypeError:
            print("File {file} has invalid JSON, please correct.".format(file=file.split("/")[-1]))
            return
        # Public API is limited to searching by queries rather than identifying what survey a ticket came from
        query = list_config_info(file)['ticketQuerySearch']
        api_token = list_config_info(file)['apiToken']
        dc = list_config_info(file)['dataCenter']
        last_run = list_config_info(file)['lastRunDate']

        # instantiate SurveyObject class for each config file/survey, add to a dictionary
        obj = SurveyObject(tkt_fields, survey_id, query, last_run, api_token, dc)
        # now get the list of all relevant tickets for each survey/config
        obj_list[survey_id] = find_mismatched_responses(obj)

        # add a week to the lastRunDate to write as the new run date
        last_run_obj = iso_format_object(last_run)
        next_run_obj = add_one_week(last_run_obj)
        next_run_date = iso_format_string(next_run_obj)

        # update config file with new lastRunTime
        write_config_params(file, next_run_date)

    # call function to create output csv file of incorrect data pairs
    create_csv(obj_list)
    return

if __name__ == '__main__':
    main()
