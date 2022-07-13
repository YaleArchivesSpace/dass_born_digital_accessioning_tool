#!/usr/bin/python3

from collections import defaultdict
import csv
from datetime import datetime
import html as html_core
import json
import os
import logging
import logging.config
import requests
import shutil
import yaml
from rich.logging import RichHandler
from rich.progress import track
from rich.console import Console


import network_setup
import send_notifications

# from rich.progress import (
#     BarColumn,
#     DownloadColumn,
#     TextColumn,
#     TransferSpeedColumn,
#     TimeRemainingColumn,
#     Progress,
#     TaskID,
# )

### Styling ###

console = Console(record=False)

# progress = Progress(
#     TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
#     BarColumn(bar_width=None),
#     "[progress.percentage]{task.percentage:>3.1f}%",
#     "•",
#     "[progress.completed]{task.completed}/{task.total}",
#     "•",    
#     TimeRemainingColumn(),
#     console=console
# )

### Exceptions ###

class FileNameError(Exception):

  def __init__(self, filename, message=f"Filename does not contain 'create' or 'update'. Please modify filename and try again."):
      self.filename = filename
      self.message = message
      super().__init__(self.message)

  def __str__(self):
      return f'{self.filename} -> {self.message}'

class LoginError(Exception):

  def __init__(self, status_code, url, username, message=f"Login failed!"):
      self.status_code = status_code
      self.url = url
      self.username = username
      self.message = message
      super().__init__(self.message)

  def __str__(self):
      return f"{self.message} URL: {self.url}, Username: {self.username}, Status code: {self.status_code}"

class DataValidationError(Exception):
  def __init__(self, value, correct_value, message=f"Invalid data! Check data entry rules and try again."):
      self.value = value
      self.correct_value = correct_value
      self.message = message
      super().__init__(self.message)

  def __str__(self):
      return f"Value you entered: {self.value} --> Correct format: {self.correct_value} --> {self.message}"

class RecordNotFoundError(Exception):
  def __init__(self, value, message=f"Value(s) not found! Check data and try again."):
      self.value = value
      self.message = message
      super().__init__(self.message)

  def __str__(self):
      return f"Value you entered: {self.value} --> {self.message}"

class ArchivesSpaceError(Exception):
  def __init__(self, uri, status_code, aspace_message, message=f"ArchivesSpace Error!"):
      self.uri = uri
      self.status_code = status_code
      self.aspace_message = aspace_message
      self.message = message
      super().__init__(self.message)

  def __str__(self):
      return f"{self.message} URI: {self.uri}, Status code: {self.status_code}, Message: {self.aspace_message.get('error')}"

### Utilities ###

def get_drive_paths(cfg):
  return (cfg.get('test_drive_path'), cfg.get('prod_drive_path'))

def set_api_url(drive_path, cfg):
  if 'test' in drive_path:
    return cfg.get('test_api_url')
  elif 'prod' in drive_path:
    return cfg.get('api_url')

def get_spreadsheet_list(drive_path):
  files = os.listdir(drive_path)
  return [f"{drive_path}/{filename}" for filename in files if filename.endswith('.csv')]

def setup_logging(log_path, default_level=logging.DEBUG):
  if os.path.exists(log_path):
    with open('logging_config.yml', 'r', encoding='utf8') as file_path:
      cfg = yaml.safe_load(file_path.read())
      cfg['handlers']['debug_file_handler']['filename'] = f'{log_path}/debug.log'
      cfg['handlers']['error_file_handler']['filename'] = f'{log_path}/errors.log'
      logging.config.dictConfig(cfg)
  else:
    logging.config.basicConfig(level=default_level)

def get_config(config_file_path="config.yml"):
  with open(config_file_path) as config_file:
    config = yaml.safe_load(config_file.read())
    return config

def set_fieldnames(extras=False):
  fieldnames = ['Repository Name', 'Security Tag', 'Parent Record', 'Title', 'Component Unique ID', 'Type_1', 'Number_of_bytes', 'Container_Summary', 'Top Container', 'Collection Name', 'Event_Type_1', 'Outcome_1', 'Begin_1', 'Outcome_Note_1', 'Event_Type_2', 'Outcome_2', 'Begin_2', 'Outcome_Note_2', 'Event_Type_3', 'Outcome_3', 'Begin_3', 'Outcome_Note_3', 'This field will not be ingested into ArchivesSpace, this information is only shared with the Digital Accessioning Service']
  if extras:
    fieldnames.extend(['New_Component_URI', 'Event_URI_1', 'Event_URI_2', 'Event_URI_3'])
    return fieldnames
  else:
    return fieldnames

def get_row_data(input_file, header_row_count=2):
  with open(input_file, encoding='utf8') as infile:
    csvfile = csv.reader(infile)
    # skips the header_row(s) - can set the number of rows to skip
    for i in range(header_row_count):
      next(csvfile)
    csvlist = list(csvfile)
    # returns the row count and the first row
    return len(csvlist), csvlist[0]

def set_parent(first_row):
  # error handling - should be a digit between 0 and 9999999
  return str(first_row[2]).rpartition("_")[2]

def get_agent_id(first_row):
  return first_row[22]

def set_resource(row):
  # error handling? should be a digit between 0 and 99999
  if '/#' in str(row['Parent Record']):
    resource_id = (row['Parent Record'].partition('/#')[0].rpartition('/')[2])
  else:
    resource_id = (row['Parent Record'].partition('#')[0].rpartition('/')[2])
  return resource_id

def set_repository(first_row, api_url, sesh):
  repo_dict = get_repositories(api_url, sesh)
  if first_row[0] in repo_dict:
    return repo_dict[first_row[0]]
  else:
    raise RecordNotFoundError(first_row[0])

def set_action_type(input_file):
  if 'create' in input_file.lower():
    return create_archival_object
  elif 'update' in input_file.lower():
    return update_archival_object
  else:
    raise FileNameError(input_file)

def get_credentials(url, username, password):
  if (url in (None, '')) or (username in (None, '')) or (password in (None, '')):
    url = input('Please enter the ArchivesSpace API URL: ')
    username = input('Please enter your username: ')
    password = input('Please enter your password: ')
  return url, username, password

def start_session(url=None, username=None, password=None):
  url, username, password = get_credentials(url, username, password)
  session = requests.Session()
  session.headers.update({'Content_Type': 'application/json'})
  auth_request = session.post(f"{url}/users/{username}/login?password={password}")
  if auth_request.status_code == 200:
    console.log(f'Login successful!: {url}')
    logging.debug(f'Login successful!: {url}')
    session_token = json.loads(auth_request.text)['session']
    session.headers['X-ArchivesSpace-Session'] = session_token
    return url, session
  else:
    raise LoginError(auth_request.status_code, url, username)

def get_record(url, sesh):
  record = sesh.get(url)
  if record.status_code == 200:
    return json.loads(record.text)
  else:
    raise ArchivesSpaceError(url, record.status_code, json.loads(record.text))

def post_record(url, sesh, record_json):
  record = sesh.post(url, json=record_json)
  # what if the text cannot be converted to json? need to make sure it works
  if record.status_code == 200:
    return json.loads(record.text)
  else:
    raise ArchivesSpaceError(url, record.status_code, json.loads(record.text))

def create_backups(dirpath, uri, record_json):
  with open(f"{dirpath}/{uri[1:].replace('/','_')}.json", 'a', encoding='utf8') as outfile:
    json.dump(record_json, outfile, sort_keys=True, indent=4)

### ArchivesSpace Stuff ###

def get_repositories(api_url, sesh):
  endpoint = f"{api_url}/repositories"
  try:
    repo_list = get_record(endpoint, sesh)
    repo_dict = {repo.get('repo_code'): str(repo.get('uri')[14:]) for repo in repo_list}
    return repo_dict
  except ArchivesSpaceError:
    # and don't return anything?
    console.log(repo_list)
    console.print_exception()
    logging.exception('Error: ')
    logging.debug(repo_list)

def get_uris(record_json):
  container_uri_list = []
  for instance in record_json.get('instances'):
    if instance.get('instance_type') != 'digital_object':
      container_uri_list.append(instance['sub_container']['top_container']['ref'])
  return container_uri_list

def generate_container_list(api_url, sesh, parent_json):
  # is it an ok idea to mix try/excepts and raising exceptions?
  container_store = []
  container_uri_list = get_uris(parent_json)
  if container_uri_list:
    for container_uri in container_uri_list:
      container_uri = f"{api_url}{container_uri}"
      try:
        record_json = get_record(container_uri, sesh)
        container_store.append((record_json['uri'], record_json['indicator']))
      except ArchivesSpaceError:
        console.log(record_json)
        console.print_exception()
        logging.exception('Error: ')
    return container_store
  else:
    raise RecordNotFoundError(parent_json)

def get_instance_data(api_url, sesh, record_json):
  # filters out digital object instances from the instance subrecord
  container_instances = [instance for instance in record_json.get('instances') if instance.get('instance_type') != 'digital_object']
  # if there are items in the container list, the generate container list function is run to get the URI and the container number
  if container_instances:
    try:
      container_list = generate_container_list(api_url, sesh, record_json)
      return container_list
    except RecordNotFoundError:
      logging.exception('Error: ')
      console.print_exception()
  else:
    raise RecordNotFoundError(record_json)

def get_containers(api_url, sesh, parent_id, repo_id, container_list=None):
  # if there's an 'error' returned, the status code would not be 200, correct? try passing in a bum uri and find out
  try:
    record_url = f"{api_url}/repositories/{repo_id}/archival_objects/{parent_id}"
    record_json = get_record(record_url, sesh)
    try:
      container_list = get_instance_data(api_url, sesh, record_json)
      return container_list
    except RecordNotFoundError:
      try:
      # if there isn't a container linked to the object, it checks the parent for a container
        parent_url = f"{api_url}/{record_json['ancestors'][0]['ref']}"
        parent_json = get_record(parent_url, sesh)
        try:
          container_list = get_instance_data(api_url, sesh, parent_json)
          return container_list
        except RecordNotFoundError:
          logging.exception('Error: ')
          console.print_exception()
      except ArchivesSpaceError:
        logging.exception('Error: ')          
        console.print_exception()
  # need to make sure this works if there's a bad response - also just not sure if it's the right thing to do, just to get the message
  except (ArchivesSpaceError, RecordNotFoundError):
    logging.exception('Error: ')
    console.print_exception()

def match_containers(container_list, container_number):
  for uri, indicator in container_list:
    if indicator == container_number:
      return uri
  else:
    raise RecordNotFoundError(container_number)

def get_current_user(api_url, sesh):
  # don't have any error handling here...but if the login worked this should work
  current_user = sesh.get(f"{api_url}/users/current-user").json()
  return current_user['agent_record']['ref']

def set_agent(api_url, sesh, agent_authorizer, username):
  if (agent_authorizer not in ("", None) and agent_authorizer != username):
    search_agents = sesh.get(f"{api_url}/search?page=1&type[]=agent_person&q=title:{agent_authorizer}").json()
    if search_agents.get('total_hits') == 1:
      return search_agents['results'][0]['uri']
    elif search_agents.get('total_hits') == 0:
      console.log('Agent search error: no results found')
      logging.debug('Agent search error: no results found')
      raise RecordNotFoundError(agent_authorizer)
    elif search_agents.get('total_hits') > 1:
      console.log('Agents search error: multiple results found')
      logging.debug('Agents search error: multiple results found')
      raise RecordNotFoundError(agent_authorizer)
    else:
      console.log('Agent search error: other error')
      logging.debug('Agent search error: other error')
      raise ArchivesSpaceError(url, search_agents)
  else:
    return get_current_user(api_url, sesh)

def create_instance(container_uri):
  return {"instance_type": 'mixed_materials',
                              "jsonmodel_type": 'instance',
                              "sub_container": {"jsonmodel_type": 'sub_container',
                                                "top_container": {"ref": container_uri}}}

def update_extents(row):
  new_extent_list = []
  if row['Type_1'] != '':
    first_extent = { "number": '1', "portion": "whole", "extent_type": row['Type_1'], "jsonmodel_type": "extent"}
    new_extent_list.append(first_extent)
  if row['Number_of_bytes'] != '':
    second_extent = { "number": row['Number_of_bytes'].replace(',', ''), "portion": "whole", "extent_type": 'bytes', "jsonmodel_type": "extent"}
    # no container summary available for the first extent?
    if row['Container_Summary'] != '':
      second_extent['container_summary'] = row['Container_Summary']
    new_extent_list.append(second_extent)
  return new_extent_list

def check_dates(date_value):
  if '/' in date_value:
    last_value = date_value.split('/')[-1]
    first_value = date_value.split('/')[0]
    if len(last_value) == 4:
      # this should be a 4 digit year and easy enough to fix
      return datetime.strptime(date_value, '%m/%d/%Y').strftime('%Y-%m-%d')
    if len(last_value) == 2:
      if len(first_value) != 4:
        return datetime.strptime(date_value, '%m/%d/%y').strftime('%Y-%m-%d')
      else:
        return datetime.strptime(date_value, '%Y/%m/%d').strftime('%Y-%m-%d')
    else:
      raise DataValidationError(date_value, 'YYYY-MM-DD')
  elif ('-' in date_value and '/' not in date_value):
    if len(date_value) < 10:
      # hmm not sure about this. Like, i don't think it's right
      return datetime.strptime(date_value, '%m-%d-%y').strftime('%Y-%m-%d')
  else:
    raise DataValidationError(date_value, 'YYYY-MM-DD')

def create_event(agent_uri, ao_id, repo_id, event_type, outcome, date_value, outcome_note):
  try:
    date_value = check_dates(date_value)
    return {"event_type": event_type.lower(), "jsonmodel_type": "event", "outcome": outcome.lower(),
              "outcome_note": outcome_note, "linked_agents": [{ "role": "authorizer", "ref": agent_uri}],
              "linked_records": [{ "role": "source", "ref": f'/repositories/{repo_id}/archival_objects/{ao_id}'}],
              "date": { "begin": date_value, "date_type": "single", "label": "event", "jsonmodel_type": "date"}}
  except DataValidationError:
    console.print_exception()
    logging.exception('Error: ')

def event_helper(api_url, sesh, agent_uri, record_uri, repo_id, event_type, outcome, begin_date, outcome_note):
  try:
    new_event = create_event(agent_uri, record_uri, repo_id, event_type, outcome, begin_date, outcome_note)
    if new_event:
      event_json = post_record(f"{api_url}/repositories/{repo_id}/events", sesh, new_event)
      return event_json.get('uri')
    else:
      # this is sort of redundant, might want to remove
      raise DataValidationError(new_event, '')
  except (DataValidationError, ArchivesSpaceError):
    logging.exception('Error: ')
    console.print_exception()

def post_events(row, agent_uri, repo_id, record_uri, api_url, sesh):
  event_uris = {}
  try:
    if row['Event_Type_1'] not in ('', None):
      new_event_uri = event_helper(api_url, sesh, agent_uri, record_uri, repo_id, row['Event_Type_1'], row['Outcome_1'], row['Begin_1'], row['Outcome_Note_1'] )
      event_uris['Event_URI_1'] = new_event_uri
    if row['Event_Type_2'] not in ('', None):
      new_event_uri = event_helper(api_url, sesh, agent_uri, record_uri, repo_id, row['Event_Type_2'], row['Outcome_2'], row['Begin_2'], row['Outcome_Note_2'] )
      event_uris['Event_URI_2'] = new_event_uri
    if row['Event_Type_3'] not in ('', None):
      new_event_uri = event_helper(api_url, sesh, agent_uri, record_uri, repo_id, row['Event_Type_3'], row['Outcome_3'], row['Begin_3'], row['Outcome_Note_3'] )
      event_uris['Event_URI_3'] = new_event_uri
    return event_uris
  except (ArchivesSpaceError, DataValidationError):
    logging.exception('Error: ')
    console.print_exception()

def get_uris(record_json):
  container_uri_list = []
  for instance in record_json.get('instances'):
    if instance.get('instance_type') != 'digital_object':
      container_uri_list.append(instance['sub_container']['top_container']['ref'])
  return container_uri_list

def update_archival_object(api_url, sesh, row, ao_id, repo_id, dirpath):
  record_uri = f"{api_url}/repositories/{repo_id}/archival_objects/{ao_id}"
  try:
    record_json = get_record(record_uri, sesh)
    create_backups(dirpath, f"/repositories/{repo_id}/archival_objects/{ao_id}", record_json)
    record_json['component_id'] = row['Component Unique ID'] # updates with the new component ID  
    record_json['extents'] = update_extents(row) # runs the update_extents function to create the new extents. Replaces any existing extents
    if row['Top Container'] not in ('', None):
      current_container_uris = get_uris(record_json) # Getting a list of top container URIs currently linked to the record. A lot of times this will be the same as the container lookup that happened before, but not always, since the container lookup will also check the parent
      if row['Top Container'] not in current_container_uris: # checks if the container that is listed in the Top Container field is in the instance field - this might not be the case if the box is linked to the parent
        new_instance = create_instance(row['Top Container']) # if there isn't already a container instance, make one; note that nothing is deleted, so if there's a container listed at the parent level it will still be there. Should maybe fix that?
        record_json['instances'].append(new_instance)
    endpoint = f"/repositories/{repo_id}/archival_objects/{ao_id}"
    return record_json, endpoint
  except ArchivesSpaceError:
    logging.exception('Error: ')
    console.print_exception()

def create_archival_object(row, repo_id, parent_id, resource_id):
  new_archival_object = {"publish": True, "title": row['Title'], "level": "item",
             "component_id": row['Component Unique ID'], "instances": [], 
             "jsonmodel_type": "archival_object", "resource": {"ref": f"/repositories/{repo_id}/resources/{resource_id}"},
            "parent": {"ref": f"/repositories/{repo_id}/archival_objects/{parent_id}"}}
  if row['Title'] == '':
    new_archival_object['title'] = '[no label]'
  new_archival_object['extents'] = update_extents(row)
  if row['Top Container'] not in ('', None):
      new_instance = create_instance(row['Top Container'])
      new_archival_object['instances'].append(new_instance)
  endpoint = f"/repositories/{repo_id}/archival_objects"
  return new_archival_object, endpoint

### Stuff to run in main function ###

def get_action(input_file):
  try:
    return set_action_type(input_file)
  except FileNameError as err:
    logging.exception(err)
    console.print_exception()

def get_session(url, username, password):
  try:
    return start_session(url, username, password)
  except LoginError as err:
    logging.exception(err)
    console.print_exception()

def get_repo(row, api_url, sesh):
  try:
    # could i put the error handling elsewhere?
    return set_repository(row, api_url, sesh)
  except (ArchivesSpaceError, RecordNotFoundError) as err:
    logging.exception(err)
    console.print_exception()

def get_agent(api_url, sesh, agent_authorizer, username):
  try:
    return set_agent(api_url, sesh, agent_authorizer, username)
  except (ArchivesSpaceError, RecordNotFoundError) as err:
    logging.exception(err)
    console.print_exception()
  
def get_matched_containers(container_list, container_number):
  try:
    return match_containers(container_list, container_number)
  except Exception as err:
    console.log(container_list)
    console.print_exception()
    logging.exception(err)
    logging.debug(container_list)
  
def skip_rows(reader, number_of_rows=2):
  for i in range(number_of_rows):
    next(reader)
  return reader

def move_files_helper(values, key):
  for source_path in values:
    #FIX
    dest_path = source_path.replace('aspace_spreadsheets_all_repos/', f'aspace_spreadsheets_all_repos/{key}/')
    logging.debug(f"Moving: {source_path} --> {dest_path}")
    shutil.move(source_path, dest_path)

def move_files(file_results, drive_path):
  for key, values in file_results.items():
    if key == 'complete':
      move_files_helper(values, key)
    elif key == 'errors':
      move_files_helper(values, key)

def main(results=False):
  try:
    config = get_config() ### load the configuration 
    drive_path = config.get('network_drive_path')
    setup_logging(f"{drive_path}/logs")
    file_listing = get_spreadsheet_list(drive_path)
    file_results = defaultdict(list)
    api_url, sesh = get_session(config.get('api_url'), config.get('username'), config.get('password')) ### Log in to the ArchivesSpace API and start a session
    for input_csv_file in file_listing:
      logging.debug(input_csv_file)
      console.log(input_csv_file)
      output_csv_file = f"{input_csv_file.replace('.csv', '').replace(drive_path, f'{drive_path}/outputs')}_out.csv" ###
      row_count, first_row = get_row_data(input_csv_file) ### Retrieve the number of rows, for use in the progress bar. Also retrieve the first row, which is used to generate the repository and resource identifiers
      action = get_action(input_csv_file) ### checks the input file for the presence of 'create' or 'update' in the filename, and chooses the function to run based on that. If neither word is present an exception is raised
      # don't need a try block here because the get_session function already has one
      # if the username and password are different....
      # need to change this, as this is not always the
      repo_identifier = get_repo(first_row, api_url, sesh) ### Set the repository ID, using the first_row variable above
      parent_identifier = set_parent(first_row) ### Set the parent ID of the first row
      agent_identifier = config.get('event_authorizer')
      # don't need a try block here because the get_agent function already has one
      agent_uri = get_agent(api_url, sesh, agent_identifier, config.get('username')) ### Set the agent URI - this allows the user to assign a different agent authorizer, other than themselves)
      fieldnames = set_fieldnames() ### Set fieldnames for the input and output CSV files - required for the csv.DictReader and csv.DictWriter classes
      new_fieldnames = set_fieldnames(extras=True) ###
      previous_container = first_row[8] ### Store the first top container indicator
      # ...not sure about this - did have a wrapper function w a try/except, but I think I covered with
      # the changes I made to get containers. But maybe I should have kept all the raises
      # and then just had the wrapper function???
      if previous_container != '':
        container_list = get_containers(api_url, sesh, parent_identifier, repo_identifier) ### get a container list for the first row. This checks the record itself and the parent
      with open(input_csv_file, encoding='utf8') as infile, open(output_csv_file, 'a', encoding='utf8') as outfile: ### Open the input and output files
        reader = csv.DictReader(infile, fieldnames=fieldnames) ### Open the CSV as a dictionary
        reader = skip_rows(reader) ### skip the first two rows
        writer = csv.DictWriter(outfile, fieldnames=new_fieldnames) ### Open the output CSV file, also as a dictionary, with some extra columns that aren't in the input CSV
        writer.writeheader()
        for row in track(reader, total=row_count): ### Loop through the input CSV. The track function initializes a progress bar
          resource_identifier = set_resource(row)
          record_id = row['Parent Record'].rpartition("_")[2] ### extracts the archival object identifier from the ArchivesSpace URL
          if record_id not in ('', None):
            # we know that sometimes the top container field will not be filled out.
            if row['Top Container'] != '':
              if row['Top Container'] != previous_container: ### if the number of the container is not the same as the last container
                container_list = get_containers(api_url, sesh, record_id, repo_identifier) ### do the lookup again
              # don't need a try block here because the get_matched_containers function already has one
              row['Top Container'] = get_matched_containers(container_list, row['Top Container']) ### match the container number with the URI, and replace the indicator value with the URI
            # this if/else block uses the result of the get_action function above to run the correct function; returns the updated or created records
            # and the endpoint to post to (either the existing URi or the /archival_objects endpoint)
            if 'update_archival_object' in str(action):
              record_json, endpoint = update_archival_object(api_url, sesh, row, record_id, repo_identifier, f"{drive_path}/backups")
            elif 'create_archival_object' in str(action):
              record_json, endpoint = create_archival_object(row, repo_identifier, record_id, resource_identifier)
            try:
              record_post = post_record(f"{api_url}{endpoint}", sesh, record_json)
              row['New_Component_URI'] = record_post.get('uri')
              event_uris = post_events(row, agent_uri, repo_identifier, record_post.get('uri'), api_url, sesh)
              row.update(event_uris)
              writer.writerow(row)
            except (ArchivesSpaceError, requests.exceptions.RequestException) as err:
              logging.exception(err)
              logging.debug(row)
              console.log(row)
              console.print_exception()
              file_results['errors'].append(input_csv_file)
              # THIS IS NEW: the script will stop reading the file if there is an error. It will break out of the loop 
              # and move on to the next file. That is better than 
              break
          else:
            console.log('Skipping row: missing Aspace URI')
            console.log(row)
            logging.debug('Skipping row: missing ASpace URI')
            logging.debug(row)
      file_results['complete'].append(input_csv_file)  
    logging.debug('Done! Check outfile for details.')
    console.log('Done! Check log and outfile for details.')
    results = True
  except Exception as exc:
    console.print(row)
    logging.exception(exc)
    logging.debug(row)
  finally:
    move_files(file_results, drive_path)
    send_notifications.send_it(success=results)

if __name__ == "__main__":
  main()

"""# Questions?

[Email me](mailto:alicia.detelich@yale.edu)

Submit issue to [Github repo](https://github.com/ucancallmealicia/born-digital-accessioner)

### To-Dos

- Logging to file
- Backups
- Testing/more error handling and data validation
  - Validate extents - common point of failure
  - Better container validation
- Display/styling
"""