import os
import xmltodict, json
import pprint
import datetime
import argparse

# requires pip3 install --upgrade google-cloud-bigquery
from google.cloud import bigquery

pp = pprint.PrettyPrinter()

'''
This script converts xml test reports from scala automated tests into a format that
can be ingested by Google BigQuery.  Generates a Newline Deliminated Json file that 
is sent to a bucket to be read by a BigQuery dataset.

Author: @jroberti
'''

def sanitize_key_names(data_struct):
    '''
    :param data_struct: a dict of lists or list
    :return: modified dict or list
    '''
    new = {}
    for key,value in data_struct.items():
        new_value = value        
        # recurse in for lists of dicts
        if isinstance(value, dict):
            new_value = sanitize_key_names(value)
        elif isinstance(value, list):
            new_value = list()
            for list_entry in value:
                new_value.append(sanitize_key_names(list_entry))

        # clean the values of keys, base values will be left alone
        clean_key = key.replace("@", "").replace("-", "_")
        new[clean_key] = new_value
    return new


def normalize_test_dict(testcase, additional_fields={}):
    '''
    Takes a dict of xml data for a particular test case and
        - converts time field to float
        - adds boolean fields for if test passed, failed, or was skipped
        - appends additional metadata to the test case
    :param testcase: A dict of test data read from xml
    :param additional_fields: A dict of additional fields to merge with the dict
    :return: Normalized dict of test data
    '''
    testcase['time'] = float(testcase['time'])

    # Handle testcase failure messages
    if testcase.get('failure'):
        testcase['errorMessage'] = testcase['failure']["message"]
        testcase['stacktrace'] = testcase['failure']["#text"]
        testcase['errorClass'] = testcase['failure']["type"].split(" ")[1]
        testcase['failure'] = True
    else:
        testcase['failure'] = False

    bool_fields = ['skipped', 'error']
    for f in bool_fields:
        testcase[f] = True if testcase.get(f) else False

    # if there are any additional fields, apprend them to the test entry
    if additional_fields:
        for k, v in additional_fields.items():
            testcase[k] = v


def parse_test_report_xml(filepath, additional_fields):
    '''
    Converts an xml test report into a list of dicts of all tests in the testsuite.
    Strips bad characters, removes redundant fields, and adds additional metadata.
    :param filepath: string path to an xml file
    :param ts: a timestamp (format %Y-%m-%dT%H:%M:%S)
    :return: a list of dicts
    '''

    # convert xml to dict
    test_data_dict = xmltodict.parse(open(filepath, 'rb'))['testsuite']
    
    # sanitize key names
    test_data_dict = sanitize_key_names(test_data_dict)

    # cast values of 'tests' to a float so we can run num operations against it.
    test_data_dict['tests'] = float(test_data_dict['tests'])

    if test_data_dict['tests'] > 1:
        for testcase in test_data_dict['testcase']:
            normalize_test_dict(testcase, additional_fields)

        return test_data_dict['testcase']
    elif test_data_dict['tests'] == 1:
        normalize_test_dict(test_data_dict['testcase'], additional_fields)
        return [test_data_dict['testcase']]
    else:
        return []


def process_directory(results_dir, additional_fields):
    '''
    Converts all xml test reports in a given directory to list-dicts; flattens list of dicts into a single list.
    :param results_dir: string path to directory of xml test results
    :param ts: a timestamp (format %Y-%m-%dT%H:%M:%S)
    :return: list of dicts of all test cases
    '''

    # each list entry should be a dict representing a single test result 
    test_dicts = []
    for file_path in os.listdir(results_dir):
        if os.path.splitext(file_path)[1] == '.xml':
            filepath = os.path.join(results_dir, file_path)
            test_dicts.append(parse_test_report_xml(filepath, additional_fields))
    
    # returns a list of dicts
    return [i for sublist in test_dicts for i in sublist]


def write_json_to_bq_file(input_json, filename):
    '''
    Writes a json to a bigquery-compatible file at specified path
    :param json: json object to write to a file
    :param filename: filename of file to write to
    '''
    
    # Write as Newline Deliminated Json (for Google Big Query)
    # This means dump each dict onto a newline
    with open(filename, "w") as report_file:
        for entry in input_json:
            report_file.write(json.dumps(entry))
            report_file.write('\n')


def append_file_to_bigquery(filename, bq_table_id):
    '''
    Uploads a bq-formatted json file to a specified bq table
    :param filename: filename of file to upload
    :param bq_table_id: bigquery table to append to, should look like "PROJECT.GROUP.TABLE" i.e. "broad-dsde-qa.test.test_table_1"
    '''
    client = bigquery.Client(project="broad-dsde-qa")

    my_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

    # reopen the file and upload it 
    with open(filename, "rb") as report_file:
        load_job = client.load_table_from_file(report_file, bq_table_id, job_config=my_job_config)
        load_job.result() # wait for job to finish and report any errors 


def main(main_args):
    '''
    This script converts xml test reports from scala automated tests into a format that
    can be ingested by Google BigQuery.  Generates a Newline Deliminated Json file that
    is sent to a bucket to be read by a BigQuery dataset.
    :param main_args: command-line arguments from argparse.parser
    '''

    # timestamp refers to time of reporting not running.
    report_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    filename = report_timestamp + "-" + main_args.service_name + "-report.json"

    # additonal fields to attach to the report (not in xml)
    additional_fields = {
        "report_timestamp": report_timestamp,
        "service": main_args.service_name,
        "testRunUUID": int(main_args.uuid)
        }

    if main_args.serviceTestRunUUID:
        additional_fields["serviceTestRunUUID"] = main_args.serviceTestRunUUID
    if main_args.env:
        additional_fields["env"] = main_args.env

    print(additional_fields)

    # Process test results
    results_as_list_of_json = process_directory(main_args.testDirectory, additional_fields)

    # write processed data to bigquery
    write_json_to_bq_file(results_as_list_of_json, filename)
    append_file_to_bigquery(filename, main_args.bigQueryTable)

    # Debug outputs
    print(results_as_list_of_json)
    print(filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Uploads Scala test results to bigquery')

    parser.add_argument(
        '-n',
        '--name',
        dest='service_name',
        type=str,
        help='name of the service, i.e. "sam"'
        )

    parser.add_argument(
        '-i',
        '--uuid',
        type=str,
        help='unique id of the test run'
        )

    parser.add_argument(
        '-e',
        '--env',
        type=str,
        help='environment?'
        )

    parser.add_argument(
        '-s',
        '--subuuid',
        dest='serviceTestRunUUID',
        type=str,
        help='name of the service Test Run UUID?'
        )
    
    parser.add_argument(
        '-d',
        '--directory',
        dest='testDirectory',
        type=str,
        help='relative path to the test report directory'
        )
    
    parser.add_argument(
        '-t',
        '--bqtable',
        dest='bigQueryTable',
        type=str,
        help='full name of big query table e.g. broad-dsde-qa.test-dataset.test-results'
        )

    main_args = parser.parse_args()
    print(main_args)

    main(main_args)
