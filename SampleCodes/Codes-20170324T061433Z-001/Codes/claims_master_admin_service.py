from optparse import OptionParser
from threading import Timer
import asyncore
import claims_util
import datetime
import dbutils
import glob
import fnmatch
import load_ancillary_data
import load_claims_to_master
import load_rx_claims_to_master
import logutil
import model
import os
import re
import collections
import pprint
import shutil
import smtplib
import socket
import stage_claims_from_file
import stage_claim_dimension_from_file 
import stage_rx_claims_from_file
import sys
import threading
import subprocess
import time
import traceback
import urllib
import utils
import whcfg
import yaml
import stage_dental_claims_from_file
import load_dental_claims_to_master
from export_identified_claims_pcs import export_identified_claims, create_export_dump
from export_rx_claims_pcs import export_identified_rx_claims, create_rx_export_dump
import rehash_claims_master
import rehash_rx_claims_master
from claims_util import *
from jira_rest import JiraRest
from django_email import DjangoEmail
from cleanse_utils import get_proper_casing
import finalize_claims
import drx_exporter
import drx_file_exporter

from ui.files_dashboard.settings import SERVER_PORT

"""
Sample Request:

category: claims
method: stage
options:
    insurance_company: aetna
    employer: safeway
"""

LOG = logutil.initlog('importer')
UPLOADED_FILES_TABLE = 'uploaded_files'
#NOTIFICATION_EMAILS = ['jtripathy@castlighthealth.com','vshah@castlighthealth.com']
#FILE_ARRIVAL_NOTIFICATION_EMAILS = 'jtripathy@castlighthealth.com'
NOTIFICATION_EMAILS = 'wh_ops@castlighthealth.com'
FILE_ARRIVAL_NOTIFICATION_EMAILS = 'file_notification@castlighthealth.com'
PASSPHRASE_FILE = '/home/whops/testphrase'
SKIP_FILE_EXTENSIONS = '.sig'

class ResettableTimer(threading.Thread):
  """
  The ResettableTimer class is a timer whose counting loop can be reset
  arbitrarily. Its duration is configurable. Commands can be specified
  for both expiration and update. Its update resolution can also be
  specified. Resettable timer keeps counting until the "run" method
  is explicitly killed with the "kill" method.
  """
  def __init__(self, maxtime, expire, ifc_condition, ifc_data, inc=None, update=None, properties=None, t_name=None):
    """
    @param maxtime: time in seconds before expiration after resetting
                    in seconds
    @param expire: function called when timer expires
    @param ifc_condition: threading.Condition which will be used for threads synchronization
    @param ifc_data: shared IFC data
    @param inc: amount by which timer increments before
                updating in seconds, default is maxtime/2
    @param update: function called when timer updates
    """
    self.t_name = t_name
    self.maxtime = maxtime
    self.expire = expire
    self.ifc_condition = ifc_condition
    self.ifc_data = ifc_data
    if inc:
      self.inc = inc
    else:
      self.inc = maxtime/2
    if update:
      self.update = update
    else:
      self.update = lambda c : None
    self.counter = 0
    self.active = True
    self.stop = False
    self.interrupt = False
    if properties:
        self.refresh_properties(properties)
    threading.Thread.__init__(self)
    self.setDaemon(True)

  def refresh_properties(self, properties):
      stop_thread = properties.get('stop_thread')
      if stop_thread and self.active:
          self.kill()
      
      run_status = properties.get('run_status')
      if run_status and run_status.lower() == 'inactive' and self.active:
          self.deactivate()
      if (run_status and run_status.lower() == 'active' and not self.active):
          self.reset()
      
      if self.inc <> int(properties.get('polling_interval')):
          self.set_inc(int(properties.get('polling_interval')))
  
  def set_inc(self, t):
      self.inc = t
      self.interrupt = True
      
  
  def set_counter(self, t):
    """
    Set self.counter to t.

    @param t: new counter value
    """
    self.counter = t
    
  def deactivate(self):
    """
    Set self.active to False.
    """
    self.active = False
    self.interrupt = True
    
  def kill(self):
    """
    Will stop the counting loop before next update.
    """
    self.stop = True
    self.interrupt = True

  def interrupt_thread(self):
    self.interrupt = True
        
  def reset(self):
    """
    Fully rewinds the timer and makes the timer active, such that
    the expire and update commands will be called when appropriate.
    """
    self.counter = 0
    self.active = True

  def run(self):
    """
    Run the timer loop.
    """
    start_update = 0
    finish_update = 0
    while True:
      self.counter = 0
      # If self.maxtime is not set, the thread never ends 
      maxtime = self.counter + 1 if not self.maxtime else self.maxtime
      while self.counter < maxtime:
        self.counter += self.inc
        # If self.maxtime is not set, the thread never ends 
        maxtime = self.counter + 1 if not self.maxtime else self.maxtime
        
        if self.stop:
            break
        
        if self.active:
          start_update = time.time()
          self.update(self.counter, self.ifc_condition, self.ifc_data, self.t_name)
          finish_update = time.time()
        else:
          logutil.log(LOG, logutil.INFO,"%s thread is inactive. Skipping call to run method." % (self.t_name))                        
        
          
        # We want a task that runs exactly every so many seconds.
        # We must therefore subtract the run time of the previous task, if
        # it is less than the default wait time interval. If the run time of 
        # the previous task is more than the default wait time, we must start
        # the next task immediately.
        
        # If a stop command is issued, 
        # (i) Any running task completes before stopping
        # (ii) Sleep gets interrupted
        
        # If self.inc is changed,
        # (i) Any running task is first completed before the new self.inc is used
        # (ii) Ongoing sleep gets interrupted, and the next task is immediately kicked off. Subsequent sleeps use the newly set self.inc
        
        sleep_time = int(self.inc - (finish_update - start_update) + 1)
        if sleep_time > 0:
            for i in xrange(sleep_time):
                
                if i == 0 and self.active: 
                    self.interrupt = False
                
                if not self.interrupt:
                    time.sleep(1)
                else:
                    self.interrupt = False
                    break
                    
                        
            
      if self.active:
        self.expire()
        self.active = False


class ImportFileConfig(collections.MutableMapping, dict):
    _instance = None
    _is_initialized = False
    
    def __new__(cls, *param, **kwargs):
        """ Implement singleton class
        """
        if not cls._instance:
            cls._instance = super(ImportFileConfig, cls).__new__(cls, *param, **kwargs)
            
        return cls._instance
    
    def __init__(self, *args, **kwargs):
        if not ImportFileConfig._is_initialized:
            collections.MutableMapping.__init__(self, *args, **kwargs)
            ImportFileConfig._is_initialized = True
            dict.__init__(self, *args, **kwargs)
        
    def __setitem__(self, key, value):
        return dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        return dict.__getitem__(self, key)
    
    def __delitem__(self, key):
        dict.__delitem__(self, key)
    
    def __iter__(self):
        return dict.__iter__(self)
    
    def __len__(self):
        return dict.__len__(self)
    
    def __contains__(self, x):
        return dict.__contains__(self, x)
    
    def __get_import_file_config_data(self):
        """ Retrieves import file config data from DB
        """
        logutil.log(LOG, logutil.INFO, "START: __get_import_file_config_data method")
        
        spec_properties = {}
        claims_master_conn = None
        try:
            claims_master_conn = dbutils.getDBConnection(dbname = self.properties.get('config').get('admin_server_dbschema'),
                                              host = self.properties.get('config').get('admin_server_dbhost'),
                                              user = self.properties.get('config').get('admin_server_dbuser'),
                                              passwd = self.properties.get('config').get('admin_server_dbpassword'),
                                              useDictCursor = True)
    
            claims_master_cursor = claims_master_conn.cursor()
            
            query_str = """SELECT ifc.id,  yml_entry_name, e.key as employer, employer_key_yml, `environment`, `phase`, 
                            `file_detection_rule`, `does_not_contain`, `file_extension`, `destination_folder`, `expected_date`, `frequency`, 
                            `file_type`, `monitor_directory`, lower(ic.name) as insurance_company, insurance_company_yml, `source`, `stage_options`, 
                            `layout_file_location`, `load_properties_file_location`, `table_name`, `dimensions`, `load_state`, `is_encrypted`, `subdir_search`
                             FROM import_file_employer_config as ifc 
                             LEFT JOIN import_file_payor_config as ifpc ON ifc.payor_info_config_id=ifpc.id
                             LEFT JOIN `employers` as e ON ifc.employer_id=e.id
                             LEFT JOIN `insurance_companies` as ic ON ifpc.insurance_company_id=ic.id
                             WHERE ifc.is_active = 1
                             ORDER BY yml_entry_name
                        """
    
            claims_master_cursor.execute(query_str)
            config_data = claims_master_cursor.fetchall()
            
            logutil.log(LOG, logutil.INFO, "Retrieved data from DB. Started preparing python dict.")
    
            for each_record in config_data:
                if each_record['yml_entry_name'] in spec_properties:
                    employer_setting = {'env': each_record['environment'],
                                        'phase': each_record['phase'],
                                        'file_detection_rule': {'contains': each_record['file_detection_rule'], 
                                                                'does_not_contain': each_record['does_not_contain'],
                                                                'file_extension': each_record['file_extension']
                                                                },
                                        'destination_folder': each_record['destination_folder'],
                                        'expected_date': each_record['expected_date'],
                                        'frequency': each_record['frequency'],
                                        'load_state': each_record['load_state'],
                                        'is_encrypted': each_record['is_encrypted'],
                                        'ifc_id': each_record['id']
                                        }
                    
                    if each_record['employer']:
                        spec_properties[each_record['yml_entry_name']]['employers'].update({each_record['employer']: employer_setting})
                    elif each_record['employer_key_yml']:
                        spec_properties[each_record['yml_entry_name']]['employers'].update({each_record['employer_key_yml']: employer_setting})
                        
                        logutil.log(LOG, logutil.WARNING, "Using '%s' employer_key_yml as Employer. Employer is not present for yml_entry_name=%s." \
                                                % (each_record['employer_key_yml'], each_record['yml_entry_name'],))
                    else:
                        logutil.log(LOG, logutil.WARNING, "Employer is not present for yml_entry_name=%s, possibly duplicate entries. Overriding employer settings" \
                                                % (each_record['yml_entry_name'],))
                        spec_properties[each_record['yml_entry_name']].update(employer_setting)
                else:
                    insurance_company = each_record['insurance_company']
                    
                    if not each_record['insurance_company'] and each_record['insurance_company_yml']:
                        insurance_company = each_record['insurance_company_yml']
    
                        logutil.log(LOG, logutil.WARNING, "Using '%s' insurance_company_yml as Insurance Company. Insurance Company is not present %s%s." \
                                                % (each_record['insurance_company_yml'], "for yml_entry_name=", each_record['yml_entry_name'],))
    
                    spec_properties[each_record['yml_entry_name']] = {'file_type': each_record['file_type'], \
                                                'monitor_directory': each_record['monitor_directory'], \
                                                'subdir_search': each_record['subdir_search'], \
                                                'insurance_company': insurance_company, \
                                                'source': each_record['source'], \
                                                'stage_options': each_record['stage_options'], \
                                                'layout_file_location': each_record['layout_file_location'], \
                                                'load_properties_file_location': each_record['load_properties_file_location'], \
                                                'table_name': each_record['table_name'], \
                                                'dimensions': [each_dimenion.strip() if each_dimenion.strip() else None \
                                                                    for each_dimenion in each_record['dimensions'].split(',') ] \
                                                                    if each_record['dimensions'] else None
                                                }
                    
                    employer_setting = {'env': each_record['environment'],
                                        'phase': each_record['phase'],
                                        'file_detection_rule': {'contains': each_record['file_detection_rule'], 
                                                                'does_not_contain': each_record['does_not_contain'],
                                                                'file_extension': each_record['file_extension']
                                                                },
                                        'destination_folder': each_record['destination_folder'],
                                        'expected_date': each_record['expected_date'],
                                        'frequency': each_record['frequency'],
                                        'load_state': each_record['load_state'],
                                        'is_encrypted': each_record['is_encrypted'],
                                        'ifc_id': each_record['id']
                                        }
                    
                    if each_record['employer']:
                        spec_properties[each_record['yml_entry_name']].update({
                                                'employers': {each_record['employer']: employer_setting}
                                                })
                    elif each_record['employer_key_yml']:
                        spec_properties[each_record['yml_entry_name']]['employers'] = {each_record['employer_key_yml']: employer_setting}
                        
                        logutil.log(LOG, logutil.WARNING, "Using '%s' employer_key_yml as Employer. Employer is not present for yml_entry_name=%s." \
                                                % (each_record['employer_key_yml'], each_record['yml_entry_name'],))
                    else:
                        spec_properties[each_record['yml_entry_name']].update(employer_setting)
        finally:
            if claims_master_conn:
                claims_master_conn.close()
        
        logutil.log(LOG, logutil.INFO, "END: __get_import_file_config_data method")
        
        self.clear()
        for each_key in spec_properties:
            self.__setitem__(each_key, spec_properties[each_key])
    
    def update_ifc_data(self, properties):
        self.properties = properties
        self.__get_import_file_config_data()


class BaseManager():
    
    def get_time(self):
        timevalue = datetime.datetime.now()
        now = timevalue.isoformat(' ').split('.')[0]
        return now
    
    def get_connection(self):
        
        if self.claims_master_conn:
            if not self.claims_master_conn.open:
                self.refresh_connection()
#            try:
#                self.claims_master_conn.ping()
#            except:
#                self.refresh_connection()
        else:
            self.refresh_connection()

#        conns_closed = dbutils.close_asleep_vm_connections(exclude_list=[self.claims_master_conn.thread_id()])
#        if conns_closed:
#            logutil.log(LOG, logutil.INFO,"Successfully Closed Connections in this VM that are in Sleep state: %s" % conns_closed)
#            
        return self.claims_master_conn
    
    def get_import_file_config_data(self):
        """ Retrieves import file config data from DB
        """
        self.ifc.update_ifc_data(self.parent_admin_service.properties)

        return self.ifc
    
    def refresh_connection(self):
        
        if self.claims_master_conn:
            try:
                self.claims_master_conn.close()
            except:
                assert True
            
        self.claims_master_conn = dbutils.getDBConnection(dbname = self.parent_admin_service.properties.get('config').get('admin_server_dbschema'),
                                          host = self.parent_admin_service.properties.get('config').get('admin_server_dbhost'),
                                          user = self.parent_admin_service.properties.get('config').get('admin_server_dbuser'),
                                          passwd = self.parent_admin_service.properties.get('config').get('admin_server_dbpassword'),
                                          useDictCursor = True)  
        
        self.insurance_companies = utils.query_insurance_companies(self.claims_master_conn)
        self.employers = utils.query_employers(self.claims_master_conn)
        
#        self.fac_ucf = model.ModelFactory.get_instance(self.claims_master_conn, UPLOADED_FILES_TABLE)      

    def _send_generic_email(self, body, recipients = [NOTIFICATION_EMAILS]):
        if self.parent_admin_service.test_run:
            #recipients = ['dataops_offshore@castlighthealth.com']
             recipients = ['jtripathy@castlighthealth.com']
        email = ''
        hostname = socket.gethostname()
        try:
            username = os.getlogin()
        except OSError:
            # some terminal emulators do not update utmp, which is needed by getlogin()
            import pwd
            username = pwd.getpwuid(os.geteuid())[0]
        timestamp = datetime.datetime.now()
        sender = username + '@castlighthealth.com'
        hdr = 'From: \'claims_master_admin_service\'<%s>\r\nTo: %s\r\nSubject: %s claims_master_admin_service %s\r\n\r\n' % (sender, ', '.join(recipients), hostname, str(timestamp))
        email = hdr + body
        server = smtplib.SMTP('localhost')
        server.sendmail(sender, recipients, email)
        server.quit()

    def refresh_properties(self):
#        self.imported_claim_tables = yaml.load(open(whcfg.providerhome + '/claims/import/common/imported_claim_tables.yml'))
        self.env_status_col = '%s_status' % self.parent_admin_service.environment
        self.env_icf_id_col = '%s_imported_claim_file_id' % self.parent_admin_service.environment
        self.env_processed_date_col = '%s_date_processed' % self.parent_admin_service.environment
        conn = None
        try:
            conn = self.get_connection()
            self.insurance_companies = utils.query_insurance_companies(conn)
            self.employers = utils.query_employers(conn)
            #TODO: refresh property
#            self.imported_claim_tables = self.get_import_file_config_data()
        finally:
            if conn:
                conn.close()
        self.monitor_directories = list(set([v.get('monitor_directory') for v in self.imported_claim_tables.values() if v.get('monitor_directory')]))
        
        self.jira_rest.close()
        self.jira_rest = JiraRest(self.parent_admin_service.properties.get('JIRA').get('jira_server'), \
                                self.parent_admin_service.properties.get('JIRA').get('jira_user'), \
                                self.parent_admin_service.properties.get('JIRA').get('jira_password'))
       
    def __init__(self, parent_admin_service):
        
        self.lc = 0 
      
        self.parent_admin_service = parent_admin_service
        self.claims_master_conn = None
        
        #configure DJango template path to use in the email templates
        template_dir = '%s/claims/import/common/templates' % whcfg.providerhome

        self.django_email = DjangoEmail(template_dir, logutil, LOG)
        
#        self.get_connection()
#        self.claims_master_conn = dbutils.getDBConnection(dbname = self.parent_admin_service.properties.get('config').get('admin_server_dbschema'),
#                                          host = self.parent_admin_service.properties.get('config').get('admin_server_dbhost'),
#                                          user = self.parent_admin_service.properties.get('config').get('admin_server_dbuser'),
#                                          passwd = self.parent_admin_service.properties.get('config').get('admin_server_dbpassword'),
#                                          useDictCursor = True)
        
#        self.imported_claim_tables = yaml.load(open(whcfg.providerhome + '/claims/import/common/imported_claim_tables.yml'))
                
        self.env_status_col = '%s_status' % self.parent_admin_service.environment
        self.env_icf_id_col = '%s_imported_claim_file_id' % self.parent_admin_service.environment
        self.env_processed_date_col = '%s_date_processed' % self.parent_admin_service.environment
        self.logged_icfs = {'stage':set([]), 'load':set([])}
#       pprint.pprint(self.imported_claim_tables)
        conn = None
        try:
            conn = self.get_connection()
            self.insurance_companies = utils.query_insurance_companies(conn)
            self.employers = utils.query_employers(conn)
            self.imported_claim_tables = self.parent_admin_service.ifc_data
#            self.imported_claim_tables = self.get_import_file_config_data()
        finally:
            if conn:
                conn.close()        
#        self.fac_ucf = model.ModelFactory.get_instance(self.get_connection(), UPLOADED_FILES_TABLE)

        self.monitor_directories = list(set([v.get('monitor_directory') for v in self.imported_claim_tables.values() if v.get('monitor_directory')]))
   
        self.jira_rest = JiraRest(self.parent_admin_service.properties.get('JIRA').get('jira_server'), \
                                self.parent_admin_service.properties.get('JIRA').get('jira_user'), \
                                self.parent_admin_service.properties.get('JIRA').get('jira_password'))
        
        self.internal_properties = {}
        
    def check_file(self, file_name, file_detection_rule):
        
        # If no file detection rule is passed return false
        if not file_detection_rule: return False
        
        contains_check = file_detection_rule.get('contains').lower() if file_detection_rule.get('contains') else None
        does_not_contain_check = file_detection_rule.get('does_not_contain').lower() if file_detection_rule.get('does_not_contain') else None
        file_extension_check = file_detection_rule.get('file_extension').lower() if file_detection_rule.get('file_extension') else None
        
        # If File detection rule does not include a valid contains check, return false
        if not contains_check: return False
        
        # If contains check fails, return false
        if not file_name.lower().find(contains_check) >= 0 and not file_name.lower().find(contains_check.replace(' ','_')) >= 0: return False
        
        # If does not contain check is provided and it passes, return false
        if does_not_contain_check and (file_name.lower().find(does_not_contain_check) >= 0 or file_name.lower().find(does_not_contain_check.replace(' ','_')) >= 0): return False
        
        # If file extension check is provided and it fails, return false
        if file_extension_check and not file_name.lower().endswith(file_extension_check): return False
        
        # Finally return true
        return True
            
    def all_referenced_dimensions(self):
        all_referenced_dimensions = []
        for table_name, table_entry in self.imported_claim_tables.iteritems():
            if table_entry.get('dimensions'):
                all_referenced_dimensions.extend(table_entry.get('dimensions'))
        return all_referenced_dimensions
        
    def resolve_command_options(self, ucf):
        file_name = ucf['source_file_name']
        file_path = ucf['source_file_path'].strip('/')
        command_options = {}
        stage_properties = {}
        load_properties = {}
        for table_name, table_entry in self.imported_claim_tables.iteritems():
            if not table_entry.get('monitor_directory') or \
            ((table_entry.get('monitor_directory','').strip('/')!= file_path and table_entry.get('subdir_search')==0) or \
                 (table_entry.get('subdir_search')==1 and os.path.commonprefix([file_path,table_entry.get('monitor_directory','').strip('/')])!=table_entry.get('monitor_directory','').strip('/'))):
                continue
          
            icf_table_name = table_entry.get('table_name', None)
            if not icf_table_name:
                icf_table_name = table_name
            for employer_key, employer_options in table_entry.get('employers',{}).iteritems():
                if self.check_file(file_name, employer_options.get('file_detection_rule')):
                    command_options['yml_entry'] = table_name
                    command_options['table_name'] = icf_table_name
                    command_options['employer_key'] = employer_key
                    
                    for k,v in table_entry.items():
                        if isinstance(v, str):
                            command_options[k] = v
                        elif isinstance(v, list):
                            command_options[k] = v

                    for k,v in employer_options.items():
                        if isinstance(v, str):
                            command_options[k] = v
                    
                    command_options['environment'] = employer_options.get('env')
                    command_options['file_detection_rule_contains'] = employer_options.get('file_detection_rule').get('contains')
                    command_options['load_state'] = employer_options.get('load_state')
                    command_options['is_encrypted'] = employer_options.get('is_encrypted')
                    command_options['ifc_id'] = employer_options.get('ifc_id')
                    
                    break 

            if command_options:
                break
        
        return command_options

    def run(self, a, ifc_condition, ifc_data, t_name):
        try:
#            self.refresh_connection()
            self.ifc_condition = ifc_condition
            self.t_name = t_name
            self.imported_claim_tables = ifc_data
            self._run(a)
        except:
            i = sys.exc_info()
            status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
            logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
            self._send_generic_email(body = 'Unexpected Error Occurred: %s' % (status_message))
            
    def get_jira_session_and_params(self, is_jira_test):
        """ Gets Jira server settings for property file and create jira session.
            @param is_test: this is test mode and will not create real Jira Tickets.
        """
        jira_session = self.jira_rest.get_session()
        
        #Jira issue data
        jira_timetracking = self.parent_admin_service.properties.get('JIRA').get('jira_timetracking')
        
        jira_timetracking_details = {}

        if jira_timetracking:
            detail_time_data = re.match(r'(\d+)d (\d+)h (\d+)m', jira_timetracking)

            if detail_time_data:
                detail_time_data = detail_time_data.groups()
                jira_timetracking_details.update({'days': int(detail_time_data[0]), \
                                        'hours': int(detail_time_data[1]), \
                                        'mins': int(detail_time_data[2])})
            
        #prepare list of watchers
        watchers = self.parent_admin_service.properties.get('JIRA').get('watchers')
        if watchers:
            watchers = [each_user.strip() for each_user in watchers.split(',')]
        #Note:
        #u'customfield_10021: severity_value, u'name': u'Severity', u'type': u'string'
        #u'customfield_10022: Environment, u'name': u'Environment', u'type': u'array'
        components = None
        if self.__class__.__name__ == 'ClaimsManager':
            components = self.parent_admin_service.properties.get('prod').get('claims_manager').get('jira_components')
        elif self.__class__.__name__ == 'ClaimsFileManager':
            components = self.parent_admin_service.properties.get('prod').get('claims_file_manager').get('jira_components')
            
        jira_params = {'jira': jira_session,
                        'is_test': is_jira_test,
                        'jira_project': self.parent_admin_service.properties.get('JIRA').get('jira_project'), 
                        'jira_issuetype': self.parent_admin_service.properties.get('JIRA').get('jira_issuetype'), 
                        'jira_assignee': self.parent_admin_service.properties.get('JIRA').get('jira_assignee'),
                        'jira_timetracking': jira_timetracking,
                        'components': components,
                        'watchers': watchers,
                        'severity_value': self.parent_admin_service.properties.get('JIRA').get('severity_value'),
                        }
        
        if jira_params['jira_project'] != 'DOPS':
            jira_params.update({'environment_value': self.parent_admin_service.properties.get('JIRA').get('environment_value') })

        logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(jira_params,)))
        
        return jira_params, jira_timetracking_details

    def create_jira_ticket_validation_fail(self, validations_type, error_type, files_description, column_names, 
        jira_params, jira_timetracking_details, file_dashboard_url, is_jira_duedate=True, export_location=None):
        """ Creates JIRA ticket as per the given parameters.
        """
        if validations_type == 'export':
            summary = "%s-%s-%s exported successfully" % (files_description['payor'], \
                                                          files_description['employer'], files_description['file_type'])
        else:
            summary = "%s-%s-%s-%s-%s" % (error_type, files_description['file_type'], \
                                          validations_type, files_description['payor'], files_description['employer'])

        description = "h5. File details are:\n|" + '|\n|'.join(['*'+each_column+'*|'+str(files_description[each_column]) for each_column in column_names]) + '|'
        jira_params['jira_assignee'] = self.parent_admin_service.properties.get('JIRA').get('jira_failure_assignee')

        if file_dashboard_url:
            description += "\n\nQuality Metrics URL:\n%s" % (file_dashboard_url,)
        
        if export_location:
            description += "\n\nLocation of the claims export dump file:\n%s" % (export_location,)
        
        jira_params.update({'summary': summary, 'description': description})
        
        jira_duedate = None
        if jira_params['jira_timetracking'] and is_jira_duedate:
            jira_params['jira_duedate'] = (datetime.timedelta(days = jira_timetracking_details['days'], \
                                hours = jira_timetracking_details['hours'], \
                                minutes = jira_timetracking_details['mins']) + datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

        logutil.log(LOG, logutil.INFO, '\n')
        logutil.log(LOG, logutil.WARNING, "Creating JIRA ticket for source_file_name=%s" % (files_description['source_file_name'],))
        logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(jira_params,)))

        jira_ticket = self.jira_rest.create_jira_issue(**jira_params)
        jira_params['jira'] = self.jira_rest.get_session()
        
        logutil.log(LOG, logutil.WARNING, "JIRA ticket '%s' created for %s/%s file" % (jira_ticket.key, \
                                files_description['source_file_path'], files_description['source_file_name']))
        
        select_cursor = self.claims_master_conn.cursor()
        select_cursor.execute("select jira_ticket from uploaded_files where id = %s", files_description['file_id'])
        data_load_jira = select_cursor.fetchone()
        data_load_jira_id = data_load_jira['jira_ticket']
        link_comment = summary + ". Created Jira: %s"
        
        if data_load_jira_id and jira_ticket and not jira_params['is_test']:
            jira_params['jira'].create_issue_link(type = 'Blocks', inwardIssue = jira_ticket.key, outwardIssue = data_load_jira_id)
            jira_params['jira'].add_comment(data_load_jira_id, link_comment %(jira_ticket.key))
        return jira_ticket

class ImportFileConfigManager(BaseManager):
    def _run(self, a):
        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Polling for new Import File Configurations ...')
        
        with self.ifc_condition:
            logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Got lock on IFC data by Import File Config Manager thread And will refresh IFC data...')
            
            conn = None
            try:
                conn = self.get_connection()
                settings_cursor = conn.cursor()
                
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Got DB connection, will check 'refresh_ifc' setting... ")
                
                settings_cursor.execute("""SELECT `key`, value FROM claims_master_admin_service_settings WHERE `key`='refresh_ifc'""")
                refresh_ifc_settings = settings_cursor.fetchall()
                
                if refresh_ifc_settings and refresh_ifc_settings[0] and int(refresh_ifc_settings[0]['value']):
                    self.imported_claim_tables.update_ifc_data(self.parent_admin_service.properties)
                    
                    update_query = """    UPDATE claims_master_admin_service_settings 
                                             SET value = 0
                                           WHERE `key`='refresh_ifc'
                                   """
                    
                    settings_cursor.execute(update_query)
                    logutil.log(LOG, logutil.INFO, str(self.t_name) + "::IFC data refreshed and 'refresh_ifc' setting updated(cleared refresh_ifc).")
                else:
                    logutil.log(LOG, logutil.INFO, str(self.t_name) + "::'refresh_ifc' is not set... IFC data is not refreshed.")
            finally:
                if conn:
                    conn.close()

            #notify other consumers of IFC data
            logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Notifying to all waiting threads...')
            self.ifc_condition.notifyAll()
            
        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Released lock on IFC data by Import File Config Manager thread.')

    def run_before_expire(self):
        print "Expiring Import File Config Manager Thread!"


class ClaimsManager(BaseManager):

    def __fetch_capabilities(self):
        return set(self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('capabilities'))
    
    def __build_stage_load_options(self, command_options, ucf):
        if command_options:
            command_options['stage_properties'] = ("""-s %s -t %s -l %s/%s -f %s/%s %s -e %s -i %s %s %s""" % (whcfg.claims_master_schema,
                                                   command_options['table_name'],
                                                   whcfg.providerhome, command_options['layout_file_location'],
                                                   ucf['file_path'], ucf['file_name'],
                                                   '-o %s' % self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('temp_directory') if command_options.get('file_type') == 'medical_claims' else '',
                                                   command_options.get('employer_key'),
                                                   command_options.get('insurance_company'),
                                                   command_options.get('stage_options',''),
                                                   '-p %s/%s' % (whcfg.providerhome, command_options.get('load_properties_file_location')) 
                                                                                if (command_options.get('file_type') == 'medical_claims' or \
                                                                                    command_options.get('file_type') == 'pharmacy_claims' or \
                                                                                    command_options.get('file_type') == 'dental_claims') \
                                                                                else ''
                                                   )).split(' ')
            
            if command_options.get('load_properties_file_location') and len(command_options['load_properties_file_location'].strip()) > 0:
                command_options['load_properties'] = ("""-p %s/%s""" % (whcfg.providerhome, command_options['load_properties_file_location'])).split(' ') 
        else:
            command_options['stage_properties'] = []
            command_options['load_properties'] = [] 

    def __build_stage_dim_load_options(self, command_options, ucf, icf_id = None):
        if command_options:
            icf_option = ' -i %s' % icf_id if icf_id else ' -e %s -p %s' % (command_options.get('employer_key'), command_options.get('insurance_company'))
            command_options['stage_properties'] = ("""-s %s%s -t %s -l %s/%s -f %s/%s %s %s""" % (whcfg.claims_master_schema,
               	                                   icf_option,command_options['table_name'],
               	                                   whcfg.providerhome, command_options['layout_file_location'],
               	                                   ucf['file_path'], ucf['file_name'],
               	                                   '-o %s' % self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('temp_directory') if command_options.get('file_type') == 'medical_claims_dimension' else '',
               	                                       command_options.get('stage_options',''))).split(' ')
            
            if command_options.get('load_properties_file_location') and len(command_options['load_properties_file_location'].strip()) > 0:
                command_options['load_properties'] = ("""-p %s/%s""" % (whcfg.providerhome, command_options['load_properties_file_location'])).split(' ') 
        else:
            command_options['stage_properties'] = []
            command_options['load_properties'] = []

    def __stage_dimension(self, uf_id,icf_id):
        stage_status = True
        conn_stage = None
        conn_stage = self.get_connection()
        fac_ucf_md = model.ModelFactory.get_instance(conn_stage, UPLOADED_FILES_TABLE)      
        t_ucf_md = fac_ucf_md.table
        t_ucf_md.search("status='received' and file_type IN ('medical_claims_dimension') and %s is NULL and parent_uploaded_file_id = %s" % (self.env_status_col, uf_id))
        r_ucf_md = []
        for ucf_md in t_ucf_md:
	        r_ucf_md.append(ucf_md)
        for ucf_md in r_ucf_md:
            command_options_md = self.resolve_command_options(ucf_md)
            self.__build_stage_dim_load_options(command_options_md, ucf_md,icf_id)
    	    if command_options_md.get('stage_properties'):
                if self.parent_admin_service.environment == command_options_md.get('environment'):
                    update_entry_dim = {'id':ucf_md['id'],
        	               		        self.env_status_col:'staging-wh',
        	                                self.env_processed_date_col:self.get_time()} 
                    t_ucf_md.update(update_entry_dim)
                    logutil.log(LOG, logutil.INFO,"Staging raw Dimension claim file: %s/%s." % (ucf_md['source_file_path'],ucf_md['source_file_name']))
                    ## Change the code of  stage_claims_dimension_from_file to have main function and also return if the error arrives
                    icdf_id = stage_claim_dimension_from_file.main(command_options_md.get("stage_properties"))
                    update_entry_dim = {'id':ucf_md['id'],
        	                                self.env_status_col:'staged-wh',
        	                                self.env_processed_date_col:self.get_time()}
                    t_ucf_md.update(update_entry_dim)
            else:
                logutil.log(LOG, logutil.INFO,"Unable to find stage properties for raw dimension claim file: %s/%s. Marking file unstageable." % (ucf_md['source_file_path'],ucf_md['source_file_name']))
                update_entry_dim = {'id':ucf_md['id'],
	                             self.env_status_col:'unstageable-wh-dimension',
	                             self.env_processed_date_col:self.get_time()}
                t_ucf_md.update(update_entry_dim)    
                stage_status = False  
        return  stage_status                	
    
    def create_jira_ticket_data_load(self, files_description, column_names, jira_params, jira_timetracking_details, is_jira_duedate=True):
        """ Creates data load JIRA ticket as per the given parameters.
        """
        jira_params['jira_project'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_jira_project')
        jira_params['components'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_jira_components')
        jira_params['jira_issuetype'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_jira_issuetype')
        jira_params['jira_assignee'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_jira_assignee')
        jira_params['priority'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_jira_priority')
        
        summary = "Load %s file %s from %s " % (files_description['file_type'], files_description['source_file_name'], files_description['payor'])
        description = "h5. File details are:\n|" + '|\n|'.join(['*'+each_column+'*|'+str(files_description[each_column]) for each_column in column_names]) + '|'
        
        jira_params.update({'summary': summary, 'description': description})
        
        jira_duedate = None
        if jira_params['jira_timetracking'] and is_jira_duedate:
            jira_params['jira_duedate'] = (datetime.timedelta(days = jira_timetracking_details['days'], \
                                hours = jira_timetracking_details['hours'], \
                                minutes = jira_timetracking_details['mins']) + datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

        logutil.log(LOG, logutil.INFO, '\n')
        logutil.log(LOG, logutil.WARNING, "Creating JIRA ticket for source_file_name=%s" % (files_description['source_file_name'],))
        logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(jira_params,)))
        
        jira_ticket = self.jira_rest.create_jira_issue(**jira_params)
        jira_params['jira'] = self.jira_rest.get_session()
        
        logutil.log(LOG, logutil.WARNING, "JIRA ticket '%s' created for %s/%s file" % (jira_ticket.key, \
                               files_description['source_file_path'], files_description['source_file_name']))
        update_query = """    UPDATE uploaded_files 
                              SET jira_ticket = '%s'
                              WHERE id=%s
                       """%(jira_ticket.key if jira_ticket else 'NA', files_description['file_id'])
        self.claims_master_conn.cursor().execute(update_query)
        return jira_ticket
        
    def __process_received(self):
        
#        t_ucf = dbutils.Table(self.claims_master_conn, UPLOADED_FILES_TABLE)
        conn = None
        try:
            conn = self.get_connection()
            fac_ucf = model.ModelFactory.get_instance(conn, UPLOADED_FILES_TABLE)      
    
            emp_cursor = conn.cursor()
            emp_cursor.execute("""SELECT `key`, name FROM employers""")
            employer_names = {}
            [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]

            #get JIRA parameters and Jira Server session
            jira_params, jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
            dataload_jira_params, dataload_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
            t_ucf = fac_ucf.table
#            t_ucf.search("status='received' and file_type IN ('medical_claims', 'pharmacy_claims') and %s is NULL and environment='%s'" % (self.env_status_col, self.parent_admin_service.environment))
            
            t_ucf.search("""status='received' and %s is NULL and environment='%s'
            AND (file_type IN ('medical_claims', 'pharmacy_claims', 'dental_claims'))""" % (self.env_status_col, self.parent_admin_service.environment))
            #t_ucf.search("""status='received' and %s is NULL and environment='%s'
            #AND (file_type IN ('medical_claims', 'pharmacy_claims', 'dental_claims') OR (file_type='medical_claims_dimension' AND parent_uploaded_file_id IS NULL))""" % (self.env_status_col, self.parent_admin_service.environment))
            
            
            t_ucf.sort('id')
            r_ucf = []
            for ucf in t_ucf:
                r_ucf.append(ucf)
            s_ucf = []
            for ucf in r_ucf:
                update_entry = {}
                error_status = None
                status_message = None
                command_options = {}
                icf_id = 0
                idc_data = None
                export_location = None
                try :
                    # Stage
                    command_options = self.resolve_command_options(ucf)
                    column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'employer', 'payor']
                    files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), ucf['source_file_path'], \
                                             ucf['source_file_name'], command_options.get('employer_key'), \
                                             command_options.get('insurance_company')]))
                    data_load_ticket = self.create_jira_ticket_data_load(files_description, column_names, dataload_jira_params, dataload_timetracking_details)          
                    if 'stage' in self.__fetch_capabilities():
                        
                        #get employer name
                        employer_name = get_proper_casing(employer_names[command_options.get('employer_key')])
                        
                        #get insurance company name
                        insurance_company_name = "NA"
                        if command_options.get('insurance_company'):
                            insurance_company_name = get_proper_casing(command_options.get('insurance_company'))
                        
                        if command_options.get('file_type') == 'medical_claims_dimension':
                            self.__build_stage_dim_load_options(command_options, ucf)
                        else:
                            self.__build_stage_load_options(command_options, ucf)
                        
                        if command_options.get('stage_properties'):
                            if self.parent_admin_service.environment == command_options.get('environment'):
                        
                                update_entry = {'id':ucf['id'],
                                                self.env_status_col:'staging-wh',
                                                self.env_processed_date_col:self.get_time()}
                                t_ucf.update(update_entry)
                                logutil.log(LOG, logutil.INFO,"Staging raw claim file: %s/%s." % (ucf['source_file_path'],ucf['source_file_name']))
                                
                                raw_validations_param = None
                                if command_options.get('file_type') == 'medical_claims':
                                    icf_id = stage_claims_from_file.main(command_options.get("stage_properties"))
                                    
                                    # Run raw validations parameters
#                                    if (command_options.get('insurance_company') <>'cigna'  or command_options.get('insurance_company') <> 'aetna'):
                                    raw_validations_param = {'imported_claim_file_ids': icf_id}

                                elif command_options.get('file_type') == 'pharmacy_claims':
                                    icf_id = stage_rx_claims_from_file.main(command_options.get("stage_properties"))
                                    
                                    # Run raw validations parameters
                                    raw_validations_param = {'imported_claim_file_ids': icf_id, 
                                                                'validation_file': '/claims/import/util/rx_claims_validations.yml', 
                                                                'claim_type': 'pharma'}
                                #need to write DentalBulkLoaderFactory (used int he claims_utils script)
				#elif command_options.get('file_type') == 'dental_claims':
				  #  icf_id=stage_dental_claims_from_file.main(command_options.get("stage_properties"))  
                                 #   raw_validations_param = {'imported_claim_file_ids': icf_id, 
                                   #                             'validation_file': '/claims/import/util/dental_raw_validation.yml', 
                                    #                            'claim_type': 'dental'}  
                                elif command_options.get('file_type') == 'medical_claims_dimension':
                                    icf_id = stage_claim_dimension_from_file.main(command_options.get("stage_properties"))
                                    self._resolve_jira(data_load_ticket.key, dataload_jira_params, \
                                                           'File load process completed. Resolved by Claims master admin service.')
                                    logutil.log(LOG, logutil.INFO,"Resolved DataLoad Ticket for file: %s/%s. Ticket id: %s" % \
                                                    (ucf['source_file_path'], ucf['source_file_name'], data_load_ticket.key))
                                    
                                stage_status = 'staged-wh'
                                validation_error_type = 'failure'
                                stage_success = True
                                
                                update_entry = {'id':ucf['id'],
                                                self.env_status_col: stage_status,
                                                self.env_icf_id_col: icf_id,
                                                self.env_processed_date_col: self.get_time()}
                                t_ucf.update(update_entry)
                                
                                logutil.log(LOG, logutil.INFO,"Staged raw claim file: %s/%s. Imported claim file id: %s" % \
                                                                    (ucf['source_file_path'], ucf['source_file_name'], icf_id))
                               
                                #raw_validations_param = None 
                                if raw_validations_param:
                                    logutil.log(LOG, logutil.INFO,"Calling Raw claim file validation: %s/%s. " % \
                                                                (ucf['source_file_path'],ucf['source_file_name']))
                                    try:
                                        # Run raw validations
                                        claims_util.claims_validation_report(\
                                                                self.parent_admin_service.properties.get('config').get('admin_server_dbschema'), \
                                                                **raw_validations_param)
                                        log_msg = "Raw claim file validation completed: %s%s." % (ucf['source_file_path'],ucf['source_file_name'],)
                                        log_type = logutil.INFO
                                    except Warning as e:
                                        log_msg = "Warnings logged during Raw Claims Validations: %s%s." % (ucf['source_file_path'],ucf['source_file_name'],)
                                        log_type = logutil.WARNING
                                    except:
                                        i = sys.exc_info()
                                        status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))

                                        log_msg = "Error logged during Raw Claims Validations: %s%s.\n%s" % \
                                                                (ucf['source_file_path'], ucf['source_file_name'], status_message)
                                                                
                                        log_type = logutil.ERROR
                                        #Raw Validation failed, so dont check result of Raw Validations
                                        raw_validations_param = False
                                        stage_status = 'staged-wh-validation-error'
                                        stage_success = False
                                        validation_error_type = 'error'
                                        
                                    logutil.log(LOG, log_type,"%s" % (log_msg,))

                                #check Raw Validations result and decide stage_status
                                validate_data_status = 0
                                if raw_validations_param:
                                    proc_cursor = conn.cursor()
                                    try:
                                        proc_cursor.execute(""" Call update_quality_metric_status (%s, @status_return)""" % (icf_id,))
                                    except Warning as e:
                                        logutil.log(LOG, logutil.WARNING,"Warnings logged during Raw Claims Validations Checks: %s/%s." % \
                                                                (ucf['source_file_path'], ucf['source_file_name']))
                                    except:
                                        validate_data_status = 1
                                        i = sys.exc_info()
                                        status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                                        logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)

                                        logutil.log(LOG, logutil.ERROR,"Error logged during Raw Claims Validations Checks: %s/%s.\n%s" % \
                                                                (ucf['source_file_path'], ucf['source_file_name'], status_message))

                                    if not validate_data_status:
                                        proc_cursor.execute("""SELECT @status_return as status_return""")
                                        validate_data_status = proc_cursor.fetchone()
                                        validate_data_status = validate_data_status['status_return'] if validate_data_status['status_return'] else 0
                                    
                                    if validate_data_status == 1:
                                        stage_status = 'staged-wh-validation-failure'
                                        validation_error_type = 'failure'
                                        stage_success = False
                                
                                    logutil.log(LOG, logutil.INFO,"Checking Raw Validations completed, return status= %s. \
                                                                    Raw claim file: %s/%s. Imported claim file id: %s" % \
                                                                    (str(validate_data_status), ucf['source_file_path'], ucf['source_file_name'], icf_id))
                                
                                #update status for error or failure or successful staging
                                update_entry = {'id':ucf['id'],
                                                    self.env_status_col: stage_status,
                                                    self.env_icf_id_col: icf_id,
                                                    self.env_processed_date_col: self.get_time()}

                                t_ucf.update(update_entry)

                                if stage_status == 'staged-wh-validation-error' or stage_status == 'staged-wh-validation-failure':
                                    #generate quality metrics URL
                                    file_dashboard_url = self.prepare_files_dashboard_url(employer_name, insurance_company_name, \
                                                                    icf_id, command_options.get('file_type'), update_entry.get(self.env_status_col))
                                    
                                    #create Jira Ticket for the failed/error-out raw validations
                                    column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'file_status', 'employer', 'payor', \
                                                            'imported_claim_file_id', self.env_processed_date_col]
                                    files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), ucf['source_file_path'], \
                                                            ucf['source_file_name'], update_entry.get(self.env_status_col), \
                                                            command_options.get('employer_key'), command_options.get('insurance_company'), icf_id, self.get_time()]))
                                    
                                    failure_jira_ticket = self.create_jira_ticket_validation_fail('Raw', validation_error_type, files_description, \
                                                            column_names, jira_params, jira_timetracking_details, file_dashboard_url)

                                    #send status of file staging if configured in the property file
                                    #if self.parent_admin_service.properties.get('prod').get('claims_manager').get('stage_notification'):
                                    self.notify_users([{'ucf': ucf, 'command_options': command_options, 'update_entry': update_entry, \
                                                                    'success': stage_success,
                                                                    'status': update_entry.get(self.env_status_col),'failure_jira_ticket': failure_jira_ticket.key,
                                                                    'status_message': status_message}], NOTIFICATION_EMAILS, employer_names, conn)

                                    #skip loading and normalization
                                    continue
                                
                        else:
                            logutil.log(LOG, logutil.INFO,"Unable to find stage properties for raw claim file: %s/%s. Marking file unstageable." % (ucf['source_file_path'],ucf['source_file_name']))
                            update_entry = {'id':ucf['id'],
                                            self.env_status_col:'unstageable-wh',
                                            self.env_icf_id_col:icf_id,
                                            self.env_processed_date_col:self.get_time()}
                            t_ucf.update(update_entry)                        
                        dim_status = self.__stage_dimension(ucf['id'],icf_id)
                        if dim_status == False:
                           update_entry = {'id':ucf['id'],
                                            self.env_status_col:'staged-wh-dim-error',
                                            self.env_icf_id_col:icf_id,
                                            self.env_processed_date_col:self.get_time()}
                           t_ucf.update(update_entry)             
                        # Validate staged claims
                        
                        # Load/Normalize
                        if icf_id > 0 and 'load' in self.__fetch_capabilities() and (command_options['load_state'] == 0 or command_options['load_state'] > 1):
                            if self.__status_dim_staging(ucf['id']):
                                if command_options.get('load_properties') or command_options.get('insurance_company') == 'cigna' or command_options.get('insurance_company') == 'aetna':
                                    if self.parent_admin_service.environment == command_options.get('environment'):
                                        logutil.log(LOG, logutil.INFO,"Loading imported claim file: %s." % str(icf_id))
                                        load_command = command_options.get('load_properties',[])
                                        load_command.insert(len(load_command), '-i')
                                        load_command.insert(len(load_command), str(icf_id))
                                        update_entry = {'id':ucf['id'],
                                                        self.env_status_col:'loading-wh',
                                                        self.env_processed_date_col:self.get_time()}
                                        t_ucf.update(update_entry)
                                        lock_flag = None
                                        if command_options.get('file_type') == 'medical_claims':
                                            if command_options.get('insurance_company') not in ['cigna','aetna']:
                                                load_command.insert(len(load_command), '-b')
                                            lock_flag = load_claims_to_master.main(load_command)
                                        elif command_options.get('file_type') == 'pharmacy_claims':
                                            lock_flag = load_rx_claims_to_master.main(load_command)
					elif command_options.get('file_type') == 'dental_claims':
					    lock_flag = load_dental_claims_to_master.main(load_command)
                                        if lock_flag == False:
                                            update_entry = {'id':ucf['id'],
                                                     self.env_status_col:'staged-wh',
                                                     self.env_icf_id_col: icf_id,
                                                     self.env_processed_date_col:self.get_time()}
                                            t_ucf.update(update_entry)
                                            continue                           
                                        
                                        update_entry = {'id':ucf['id'],
                                                        self.env_status_col:'loaded-wh',
                                                        self.env_icf_id_col: icf_id,
                                                        self.env_processed_date_col:self.get_time()}
                                        t_ucf.update(update_entry)     
                                        update_has_eligibility = """update imported_claim_files icf join patients p
                                                                            on icf.employer_id = p.updated_by_employer_id
                                                                            set has_eligibility = 1
                                                                            where icf.id = %s"""%(icf_id)
                                        conn.cursor().execute(update_has_eligibility)
 
                                        proc_call_query = None
                                        if command_options.get('file_type') == 'medical_claims':
                                            proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                                                self.employers.get(command_options.get('employer_key')),
                                                                self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                                str(icf_id), 
                                                                3)
                                        elif command_options.get('file_type') == 'pharmacy_claims':
                                            proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                                                self.employers.get(command_options.get('employer_key')),
                                                                self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                                str(icf_id), 
                                                                2)
                                        #elif command_options.get('file_type') == 'dental_claims':
                                         #   proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                          #                      self.employers.get(command_options.get('employer_key')),
                                           #                     self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                            #                    str(icf_id), 
                                             #                   10)   
                       ##Report gr:10 for dental normalizated claims, validation for normalized dental claims is not yet in prod, so commented this part.                                           
                                        
                                        file_success = True
                                        if proc_call_query:
                                            proc_cursor = conn.cursor()
                                            try:
                                                proc_cursor.execute(proc_call_query)
                                            except Warning as e:
                                                logutil.log(LOG, logutil.INFO,"Warnings logged during Normalized Claims Validations for imported_claim_file_id: %s." % str(icf_id))
                                      
                                            proc_cursor.execute("""SELECT @validation_status as validation_status""")
                                            validate_data_status = proc_cursor.fetchone()
                                            
                                            logutil.log(LOG, logutil.INFO,"""Called validate_data procedure: %s, 
                                                            values: employer_id=%s, insurance_company_id=%s, file_id=%s, 
                                                            Return result: validation_status=%s
                                                            """ % (proc_call_query,
                                                            self.employers.get(command_options.get('employer_key')),
                                                            self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                            str(icf_id), str(validate_data_status)))

                                            if validate_data_status['validation_status'] != 1:
                                                update_row = {'id':ucf['id'],
                                                                'data_quality_check_flag':1}
                                                t_ucf.update(update_row)
                                            else:
                                                file_success = False
                                                failure_status = "loaded-wh-validation-failure"
                                                failure_count_query = """select count(*) as failure_count 
                                                                         from data_quality_results dr join validation_sql val 
                                                                         on dr.metric_id = val.id 
                                                                         where dr.imported_claim_file_id = %s and dr.metric_status = 2 and val.report_group in (2,3);""" % (icf_id)
                                                master_cursor = conn.cursor()
                                                master_cursor.execute(failure_count_query) 
                                                failure_count_result = master_cursor.fetchone()

                                                if failure_count_result['failure_count'] == 1:
                                                    get_has_eligibility_query = """select has_eligibility 
                                                                                   from imported_claim_files where id = %s""" % (icf_id)
                                                    master_cursor.execute(get_has_eligibility_query)
                                                    has_eligibility_result = master_cursor.fetchone()
                                                    if has_eligibility_result['has_eligibility'] == 0:
                                                        failure_status = 'waiting-for-eligibility'
                                                update_entry = {'id': ucf['id'],
                                                                        self.env_icf_id_col: icf_id,
                                                                        self.env_status_col: failure_status,
                                                                        self.env_processed_date_col: self.get_time()}
                                                t_ucf.update(update_entry)

                                                if failure_status == "loaded-wh-validation-failure":
                                        
                                                    validation_error_type = 'failure'
                                                    column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'file_status', 'employer', \
                                                                        'payor', 'imported_claim_file_id', self.env_processed_date_col]
                                                    files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), \
                                                                        ucf['source_file_path'], ucf['source_file_name'], \
                                                                        update_entry.get(self.env_status_col), command_options.get('employer_key'), \
                                                                        command_options.get('insurance_company'), icf_id, self.get_time()]))
                                                
                                                    #generate quality metrics URL
                                                    file_dashboard_url = self.prepare_files_dashboard_url(employer_name, insurance_company_name, \
                                                                                icf_id, command_options.get('file_type'), update_entry.get(self.env_status_col))

                                                    failure_jira_ticket = self.create_jira_ticket_validation_fail('Normalization', validation_error_type, files_description, \
                                                                        column_names, jira_params, jira_timetracking_details, file_dashboard_url)
                                            #if self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_notification'):
                                                    self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                                                'update_entry': update_entry, 'success': file_success,
                                                                'status': update_entry.get(self.env_status_col), 'failure_jira_ticket': failure_jira_ticket.key,
                                                                'status_message': None}], NOTIFICATION_EMAILS, employer_names, conn)
                                        if file_success == True and 'export' in self.__fetch_capabilities() and (command_options['load_state'] == 0 or command_options['load_state'] > 2):
                                            ucf[self.env_icf_id_col] = icf_id
                                            update_entry, idc_data, export_location, failure_jira_ticket = self.__export_claim_icf(conn, employer_names, command_options, t_ucf, ucf)
                                            if update_entry.get(self.env_status_col,'') != 'claims-exported':
                                                error_status = update_entry.get(self.env_status_col,'')
                                else:
                                    logutil.log(LOG, logutil.INFO,"Unable to find load properties for imported claim file: %s. Marking file as unloadable." % str(icf_id))
                                    update_entry = {'id':ucf['id'],
                                        self.env_status_col:'unloadable',
                                        self.env_processed_date_col:self.get_time()}
                                    t_ucf.update(update_entry) 
                            else:
                                 logutil.log(LOG, logutil.INFO,"Load cannot proceed ahead, all dependent files are not staged. Not attempting to load imported claim file: %s." % str(icf_id)) 
                        elif icf_id > 0 and icf_id not in self.logged_icfs.get('load'):
                            self.logged_icfs['load'].update(set([icf_id]))
                            logutil.log(LOG, logutil.INFO,"Load capability has been disable. Not attempting to load imported claim file: %s." % str(icf_id))        
                    elif icf_id not in self.logged_icfs.get('stage'):
                        self.logged_icfs['stage'].update(set([icf_id]))
                        logutil.log(LOG, logutil.INFO,"Stage capability has been disable. Not attempting to stage raw claim file: %s/%s." % (ucf['source_file_path'],ucf['source_file_name']))
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
                    error_status = update_entry.get(self.env_status_col,'') + '-error'       
                finally:
                    if error_status:
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:error_status,
                                        self.env_processed_date_col:self.get_time()}
                        t_ucf.update(update_entry)    
                    s_ucf.append({'ucf':ucf,
                                  'command_options': command_options,
                                  'update_entry': update_entry, 
                                  'success':False if error_status else True,
                                  'status':update_entry.get(self.env_status_col),
                                  'status_message':status_message,
                                  'idc_data': idc_data if idc_data else None,
                                  'export_location': export_location if export_location else None})
            if s_ucf:
#                print "\n\ns_ucf", s_ucf
                email_style = self.parent_admin_service.properties.get('prod').get('claims_manager').get('bulk_email_style') 
                if email_style == 'old':
                    self.__send_email(s_ucf)
                elif email_style == 'new_single_email':
                    self.notify_users(s_ucf, NOTIFICATION_EMAILS, employer_names, conn, False)
                elif email_style == 'new_multiple_email':
                    pass
                    #self.notify_users(s_ucf, NOTIFICATION_EMAILS, employer_names, conn)
        finally:
            if conn:
                conn.close()
                
    def __status_dim_staging(self,uf_id):
       stage_status = True
       conn_dim = None
       conn_dim  = self.get_connection()
       fac_ucf_md = model.ModelFactory.get_instance(conn_dim , UPLOADED_FILES_TABLE)      
       t_ucf_md = fac_ucf_md.table
       t_ucf_md.search("file_type IN ('medical_claims_dimension') and parent_uploaded_file_id =%s" % (uf_id))
       r_ucf_md = []
       for ucf_md in t_ucf_md:
	   r_ucf_md.append(ucf_md) 
       for ucf_md in r_ucf_md:
           if ucf_md[self.env_status_col] <> 'staged-wh':
	      stage_status = False  
       return stage_status                	
 
            
    def __process_staged(self):
        if 'load' not in self.__fetch_capabilities():
            return
        conn = None
        conn = self.get_connection()
        
        #get JIRA parameters and Jira Server session
        jira_params, jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)

        emp_cursor = conn.cursor()
        emp_cursor.execute("""SELECT `key`, name FROM employers""")
        employer_names = {}
        [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]

        try:
            t_ucf = dbutils.Table(conn, UPLOADED_FILES_TABLE)
            t_ucf.search("status='received' AND file_type IN ('medical_claims', 'pharmacy_claims', 'dental_claims') AND %s='staged-wh' and environment='%s'" % (self.env_status_col, self.parent_admin_service.environment))
            t_ucf.sort('id')
            r_ucf = []
            s_ucf = []
            for ucf in t_ucf:
                r_ucf.append(ucf)
                
            for ucf in r_ucf:
                update_entry = {}
                error_status = None
                status_message = None
                command_options = {}
                is_state_disabled = False
                idc_data = None
                export_location = None
                try :
                    
                    # Validate staged claims (Raw Validations)
                    
                    icf_id = ucf[self.env_icf_id_col]
                    # Load/Normalize
                    command_options = self.resolve_command_options(ucf)
                    if 'load' in self.__fetch_capabilities() and command_options['load_state'] == 1:
                        is_state_disabled = True
                        
                        self.logged_icfs['load'].update(set([icf_id]))
                        logutil.log(LOG, logutil.INFO,"Load capability has been disable. Not attempting to load imported claim file: %s." % str(icf_id))
                    elif 'load' in self.__fetch_capabilities() and (command_options['load_state'] == 0 or command_options['load_state'] > 1):
                        if self.__status_dim_staging(ucf['id']):
                            
                            
                            #get employer name
                            employer_name = get_proper_casing(employer_names[command_options.get('employer_key')])
                            
                            #get insurance company name
                            insurance_company_name = "NA"
                            if command_options.get('insurance_company'):
                                insurance_company_name = get_proper_casing(command_options.get('insurance_company'))
                            
                            self.__build_stage_load_options(command_options, ucf)
                            
                            if command_options.get('load_properties') \
                            or command_options.get('insurance_company') == 'cigna' \
                            or command_options.get('insurance_company') == 'aetna':
                                if self.parent_admin_service.environment == command_options.get('environment'):
                                    logutil.log(LOG, logutil.INFO,"Loading imported claim file: %s." % str(icf_id))
                                    
                                    load_command = command_options.get("load_properties",[])
                                    load_command.insert(len(load_command), '-i')
                                    load_command.insert(len(load_command), str(icf_id))
                                    update_entry = {'id':ucf['id'],
                                                    self.env_status_col:'loading-wh',
                                                    self.env_processed_date_col:self.get_time()}
                                    t_ucf.update(update_entry)
                                    lock_flag = None 
                                    if command_options.get('file_type') == 'medical_claims':
                                        if command_options.get('insurance_company') not in ['cigna','aetna']:
                                            load_command.insert(len(load_command), '-b')
                                        lock_flag = load_claims_to_master.main(load_command)
                                    elif command_options.get('file_type') == 'pharmacy_claims':
                                        lock_flag = load_rx_claims_to_master.main(load_command)
				    elif command_options.get('file_type') == 'dental_claims':
				        lock_flag = load_dental_claims_to_master.main(load_command) 
                                    if lock_flag == False:
                                        update_entry = {'id':ucf['id'],
                                                    self.env_status_col:'staged-wh',
                                                    self.env_processed_date_col:self.get_time()}
                                        t_ucf.update(update_entry)
                                        continue
                                    update_entry = {'id':ucf['id'],
                                                    self.env_status_col:'loaded-wh',
                                                    self.env_processed_date_col:self.get_time()}
                                    t_ucf.update(update_entry)
                                    update_has_eligibility = """update imported_claim_files icf join patients p
                                                                    on icf.employer_id = p.updated_by_employer_id
                                                                    set has_eligibility = 1
                                                                    where icf.id = %s"""%(icf_id)
                                    conn.cursor().execute(update_has_eligibility)
 
                                    proc_call_query = None
                                    if command_options.get('file_type') == 'medical_claims':
                                        proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                                            self.employers.get(command_options.get('employer_key')),
                                                            self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                            str(icf_id), 
                                                            3)
                                    elif command_options.get('file_type') == 'pharmacy_claims':
                                        proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                                            self.employers.get(command_options.get('employer_key')),
                                                            self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                            str(icf_id), 
                                                            2)
                                    #elif command_options.get('file_type') == 'dental_claims':
                                     #   proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                      #                      self.employers.get(command_options.get('employer_key')),
                                       #                     self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                        #                    str(icf_id), 
                                         #                   10)    
                                                
                                    if proc_call_query:
                                        proc_cursor = conn.cursor()
                                        try:
                                            proc_cursor.execute(proc_call_query)
                                        except Warning as e:
                                            logutil.log(LOG, logutil.INFO,"Warnings logged during Normalized Claims Validations for imported_claim_file_id: %s." % str(icf_id))
                                        
                                        proc_cursor.execute("""SELECT @validation_status as validation_status""")
                                        validate_data_status = proc_cursor.fetchone()
                                        
                                        logutil.log(LOG, logutil.INFO,"""Called validate_data procedure: %s, 
                                                            values: employer_id=%s, insurance_company_id=%s, file_id=%s, Return result: validation_status=%s
                                                            """ % (proc_call_query,
                                                            self.employers.get(command_options.get('employer_key')),
                                                            self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                                            str(icf_id), str(validate_data_status)))

                                        file_success = False
                                        
                                        if validate_data_status['validation_status'] != 1:
                                            update_row = {'id':ucf['id'],
                                                            'data_quality_check_flag':1}
                                            t_ucf.update(update_row)
                                            file_success = True
                                        else:
                                            failure_status = "loaded-wh-validation-failure"
                                            failure_count_query = """select count(*) as failure_count 
                                                                     from data_quality_results dr join validation_sql val 
                                                                     on dr.metric_id = val.id 
                                                                     where dr.imported_claim_file_id = %s and dr.metric_status = 2 and val.report_group in (2,3);""" % (icf_id)
                                            master_cursor = conn.cursor()
                                            master_cursor.execute(failure_count_query)
                                            failure_count_result = master_cursor.fetchone()
                                            if failure_count_result['failure_count'] == 1:
                                                get_has_eligibility_query = """select has_eligibility 
                                                                             from imported_claim_files where id = %s""" % (icf_id)
                                                master_cursor.execute(get_has_eligibility_query)
                                                has_eligibility_result = master_cursor.fetchone()
                                                if has_eligibility_result['has_eligibility'] == 0:
                                                    failure_status = 'waiting-for-eligibility'
                                            
                                            update_entry = {'id': ucf['id'],
                                                                    self.env_status_col: failure_status,
                                                                    self.env_processed_date_col: self.get_time()}
                                            t_ucf.update(update_entry)
                                            if failure_status == "loaded-wh-validation-failure":    
                                                validation_error_type = 'failure'
                                                column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'file_status', 'employer', \
                                                                    'payor', 'imported_claim_file_id', self.env_processed_date_col]
                                                files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), \
                                                                    ucf['source_file_path'], ucf['source_file_name'], \
                                                                    update_entry.get(self.env_status_col), command_options.get('employer_key'), \
                                                                    command_options.get('insurance_company'), icf_id, self.get_time()]))
                                            
                                                #generate quality metrics URL
                                                file_dashboard_url = self.prepare_files_dashboard_url(employer_name, insurance_company_name, \
                                                                            icf_id, command_options.get('file_type'), update_entry.get(self.env_status_col))

                                                failure_jira_ticket = self.create_jira_ticket_validation_fail('Normalization', validation_error_type, files_description, \
                                                                    column_names, jira_params, jira_timetracking_details, file_dashboard_url)
                                            #if self.parent_admin_service.properties.get('prod').get('claims_manager').get('load_notification'):
                                                self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                                                'update_entry': update_entry, 'success': file_success,
                                                                'status': update_entry.get(self.env_status_col), 'failure_jira_ticket': failure_jira_ticket.key,
                                                                'status_message': None}], NOTIFICATION_EMAILS, employer_names, conn)
                                        if file_success == True and 'export' in self.__fetch_capabilities() and (command_options['load_state'] == 0 or command_options['load_state'] > 2):
                                            update_entry, idc_data, export_location, failure_jira_ticket = self.__export_claim_icf(conn, employer_names, command_options, t_ucf, ucf)
                                            if  update_entry.get(self.env_status_col,'') != 'claims-exported':
                                                error_status = update_entry.get(self.env_status_col,'')
# 		                else:
# 		                    logutil.log(LOG, logutil.INFO,"Unable to find load properties for imported claim file: %s. Marking file as unloadable." % str(icf_id))
# 		                    update_entry = {'id':ucf['id'],
# 		                                    self.env_status_col:'unloadable',
# 		                                    self.env_processed_date_col:self.get_time()}
# 		                    t_ucf.update(update_entry)
# 			else:
# 			     logutil.log(LOG, logutil.INFO,"Load cannot proceed ahead as all dependent files are not staged. Not attempting to load imported claim file: %s." % str(icf_id)) 
                            else:
                                logutil.log(LOG, logutil.INFO,"Unable to find load properties for imported claim file: %s. Marking file as unloadable." % str(icf_id))
                                update_entry = {'id':ucf['id'],
                                                self.env_status_col:'unloadable',
                                                self.env_processed_date_col:self.get_time()}
                                t_ucf.update(update_entry)
                        else:
                            logutil.log(LOG, logutil.INFO,"Load cannot proceed ahead as all dependent files are not staged. Not attempting to load imported claim file: %s." % str(icf_id)) 
                    elif icf_id not in self.logged_icfs.get('load'):
                        is_state_disabled = True
                        
                        self.logged_icfs['load'].update(set([icf_id]))
                        logutil.log(LOG, logutil.INFO,"Load capability has been disable. Not attempting to load imported claim file: %s." % str(icf_id))
    
                    # Validate loaded claims (Normalized Validations)
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
                    error_status = update_entry.get(self.env_status_col,'') + '-error'       
                finally:
                    if error_status:
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:error_status,
                                        self.env_processed_date_col:self.get_time()}
                        t_ucf.update(update_entry)
                    
                    if not is_state_disabled:
                        s_ucf.append({'ucf':ucf,
                                      'command_options': command_options,
                                      'update_entry': update_entry, 
                                      'success':False if error_status else True,
                                      'status':update_entry.get(self.env_status_col),
                                      'status_message':status_message,
                                      'idc_data': idc_data if idc_data else None,
                                      'export_location': export_location if export_location else None})
            if s_ucf:
                email_style = self.parent_admin_service.properties.get('prod').get('claims_manager').get('bulk_email_style') 
                if email_style == 'old':
                    self.__send_email(s_ucf)
                elif email_style == 'new_single_email':
                    self.notify_users(s_ucf, NOTIFICATION_EMAILS, employer_names, conn, False)
                elif email_style == 'new_multiple_email':
                    self.notify_users(s_ucf, NOTIFICATION_EMAILS, employer_names, conn)
        finally:
            if conn:
                conn.close()

    def __export_claim_icf(self, claims_master_conn, employer_names, command_options, t_ucf, ucf, jira_params = None,\
                            jira_timetracking_details = None, v_jira_params = None, v_jira_timetracking_details = None):
        
        failure_jira_ticket = None
        #get JIRA parameters and Jira Server session
        if jira_params == None and jira_timetracking_details == None:
            jira_params, jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
            jira_params['jira_project'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_project')
            jira_params['components'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_components')
            jira_params['jira_issuetype'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_issuetype')
            jira_params['jira_assignee'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_assignee')
            jira_params['priority'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_priority')

        if v_jira_params == None and v_jira_timetracking_details == None:
            v_jira_params, v_jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
                
        #NetOps Jira project doesnt have severity and jira_duedate parameters
        if jira_params.has_key('severity_value'):
            jira_params.pop('severity_value')
        #get employer name
        employer_name = get_proper_casing(employer_names[command_options.get('employer_key')])
        #get insurance company name
        insurance_company_name = "NA"
        if command_options.get('insurance_company'):
            insurance_company_name = get_proper_casing(command_options.get('insurance_company')) 
        icf_id = ucf[self.env_icf_id_col]
        update_entry = {}
        error_status = None
        status_message = None
        is_state_disabled = False
        c_m_cursor = claims_master_conn.cursor() 
        if command_options.get('file_type') == 'medical_claims':
            export_claims_caller = export_identified_claims
        elif command_options.get('file_type') == 'pharmacy_claims':
            export_claims_caller = export_identified_rx_claims
        elif command_options.get('file_type') == 'dental_claims':
            export_claims_caller = export_dental_claims
            
        export_location = self.parent_admin_service.properties.get('prod').get('claims_manager').get('claims_export_directory')
        
        export_params = {'output_folder': export_location,
                         'imported_claim_file_ids': ["%s" % (icf_id,)],
                         'employer_key': command_options.get('employer_key'),
                         'payer': command_options.get('insurance_company')
                        }
                        
        update_entry = {'id': ucf['id'],
                         self.env_status_col: 'claims-exporting',
                         self.env_processed_date_col: self.get_time()}
                   
        t_ucf.update(update_entry)
        try:
            export_claims_caller(export_params.get('employer_key'), export_params.get('payer'), export_params.get('imported_claim_file_ids'))
                            
            # Validate exported medical claims (IDC claims Validations)
            proc_call_query = None
            file_success = True
            env_status_col = 'claims-exported'
            idc_data = None
            file_name = None
                            
            if command_options.get('file_type') == 'medical_claims':
                                
                proc_call_query = """ call validate_data ('%s','%s','%s',%s,@validation_status) """ % (  
                                      self.employers.get(command_options.get('employer_key')),
                                      self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                      str(icf_id), 
                                      4)
    
                try:
                    c_m_cursor.execute(proc_call_query)
                except Warning as e:
                    logutil.log(LOG, logutil.INFO,"Warnings logged during IDC Claims Validations for imported_claim_file_id: %s." % str(icf_id))
                          
                c_m_cursor.execute("""SELECT @validation_status as validation_status""")
                validate_data_status = c_m_cursor.fetchone()
                                
                logutil.log(LOG, logutil.INFO,"""Called validate_data procedure: %s, 
                           values: employer_id=%s, insurance_company_id=%s, file_id=%s, report_group=%s 
                           Return result: validation_status=%s
                           """ % (proc_call_query,
                           self.employers.get(command_options.get('employer_key')),
                           self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                           str(icf_id), 4, str(validate_data_status)))
    
                if validate_data_status['validation_status'] == 1:
                    file_success = False
                    env_status_col = 'claims-export-validation-failure'
                    validation_error_type = 'failure'
                    column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'file_status', 'employer', 'payor', \
                                                            'imported_claim_file_id', self.env_processed_date_col]
                    files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), \
                                            ucf['source_file_path'], ucf['source_file_name'], \
                                            env_status_col, command_options.get('employer_key'), \
                                            command_options.get('insurance_company'), icf_id, self.get_time()]))
                                                    
                                                    #generate quality metrics URL
                    file_dashboard_url = self.prepare_files_dashboard_url(employer_name, insurance_company_name, \
                                                                        icf_id, command_options.get('file_type'), env_status_col)
    
                    failure_jira_ticket = self.create_jira_ticket_validation_fail('IDC', validation_error_type, files_description, \
                                                                            column_names, v_jira_params, v_jira_timetracking_details, file_dashboard_url)
    
                    query_str = """SELECT metric_id, `Metric Name` as metric_name, `Metric Value` as metric_value, 
                                Benchmark, `Metric Status` as metric_status, Employer,
                                Payor, `File Type` as file_type, employer_id,
                                insurance_company_id, `Run Date` as run_date, `File ID` as file_id,
                                `Report Type` as report_type, 
                                `Percent Value` as percent_value 
                                FROM v_data_quality_results
                                WHERE employer_id = %s
                                AND insurance_company_id = %s
                                AND lower(`Report Type`) = 'IDC'
                                AND (`Metric Status` = 2 OR `Metric Status` = 1)
                                AND `File ID` = %s
                                """ % (self.employers.get(command_options.get('employer_key')),
                                            self.insurance_companies.get(command_options.get('insurance_company','').lower()),
                                            str(icf_id))
    
                    c_m_cursor.execute(query_str)
                    idc_data = c_m_cursor.fetchall()
                else:
                    create_export_dump(export_params.get('imported_claim_file_ids'), export_params.get('output_folder'))
                    file_name = "/identified_claims_export_%s.dmp" % (icf_id)
            elif command_options.get('file_type') == 'pharmacy_claims':
                create_rx_export_dump(export_params.get('imported_claim_file_ids'), export_params.get('output_folder'))
                file_name = "/rx_claims_export_%s.dmp" % (icf_id)
            elif command_options.get('file_type') == 'dental_claims':
                create_rx_export_dump(export_params.get('imported_claim_file_ids'), export_params.get('output_folder'))
                file_name = "/dental_claims_export_%s.dmp" % (icf_id)    
                            #update status after export and validations
            update_entry = {'id': ucf['id'],
                            self.env_status_col: env_status_col,
                            self.env_processed_date_col: self.get_time()}
                            
            t_ucf.update(update_entry)
    
                            #send export notification email
            if file_name != None:
                export_location = export_location + file_name
                            
            '''self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                            'update_entry': update_entry, 'success': file_success,
                            'status': update_entry.get(self.env_status_col), 'failure_jira_ticket': failure_jira_ticket.key
                            'status_message': None, 'idc_data': idc_data, 'export_location': export_location}], \
                            NOTIFICATION_EMAILS, employer_names)'''
    
                            #create Netops Jira ticket if validations are successful
            if file_success:
                column_names = ['file_id', 'file_type', 'source_file_path', 'source_file_name', 'file_status', 'employer', 'payor', \
                                'imported_claim_file_id', self.env_processed_date_col]
                files_description = dict( zip(column_names, [ucf['id'], command_options.get('file_type'), \
                                ucf['source_file_path'], ucf['source_file_name'], \
                                update_entry.get(self.env_status_col), command_options.get('employer_key'), \
                                command_options.get('insurance_company'), icf_id, self.get_time()]))
                                
                failure_jira_ticket = self.create_jira_ticket_validation_fail('export', None, files_description, \
                column_names, jira_params, jira_timetracking_details, None, False, export_location)
                                
                self._resolve_jira(ucf['jira_ticket'], jira_params, \
                                                            'File load process completed. Resolved by Claims master admin service.')
                                
            logutil.log(LOG, logutil.INFO, "Export finished: imported claim file-%s." % str(icf_id))
        except:
            i = sys.exc_info()
            status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
            logutil.log(LOG, logutil.CRITICAL, "SEVERE ERROR: %s" % status_message)
            error_status = update_entry.get(self.env_status_col,'') + '-error'
            update_entry = {'id':ucf['id'],
                            self.env_status_col:error_status,
                            self.env_processed_date_col:self.get_time()}
            raise
        return update_entry, idc_data, export_location, failure_jira_ticket
                        
        
    def __export_claims(self):
        if 'export' not in self.__fetch_capabilities():
            logutil.log(LOG, logutil.INFO, "Export capability has been disable. Not attempting to export imported claim files")
            return
        
        claims_master_conn = None
        claims_master_conn = self.get_connection()
        
        #get JIRA parameters and Jira Server session
        jira_params, jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
        v_jira_params, v_jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
        jira_params['jira_project'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_project')
        jira_params['components'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_components')
        jira_params['jira_issuetype'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_issuetype')
        jira_params['jira_assignee'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_assignee')
        jira_params['priority'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_priority')
        
        #NetOps Jira project doesnt have severity and jira_duedate parameters
        if jira_params.has_key('severity_value'):
            jira_params.pop('severity_value')
            
        emp_cursor = claims_master_conn.cursor()
        emp_cursor.execute("""SELECT `key`, name FROM employers""")
        employer_names = {}
        [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]

        try:
            t_ucf = dbutils.Table(claims_master_conn, UPLOADED_FILES_TABLE)
            t_ucf.search("status='received' AND file_type IN ('medical_claims', 'pharmacy_claims', 'dental_claims') AND %s='loaded-wh' and environment='%s'" \
                                                % (self.env_status_col, self.parent_admin_service.environment))
            t_ucf.sort('id')
            r_ucf = []
            s_ucf = []
            for ucf in t_ucf:
                r_ucf.append(ucf)
                
            for ucf in r_ucf:
                update_entry = {}
                error_status = None
                status_message = None
                is_state_disabled = False
                command_options = {}
                try :
                    icf_id = ucf[self.env_icf_id_col]
                    command_options = self.resolve_command_options(ucf)

                    if 'export' in self.__fetch_capabilities() and self.parent_admin_service.environment == command_options.get('environment') \
                    and (command_options['load_state'] == 0 or command_options['load_state'] > 2):
                        logutil.log(LOG, logutil.INFO, "Exporting imported claim file: %s." % str(icf_id))
                        update_entry, idc_data, export_location, failure_jira_ticket = self.__export_claim_icf(claims_master_conn, employer_names, command_options, t_ucf, ucf,\
                                                                                          jira_params, jira_timetracking_details, v_jira_params, v_jira_timetracking_details)
                        # Export/Validate
                        logutil.log(LOG, logutil.INFO, "Export finished: imported claim file-%s." % str(icf_id))
                        if  update_entry.get(self.env_status_col,'') != 'claims-exported':
                            file_success = False
                        else:
                            file_success = True
                        self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                                    'update_entry': update_entry, 'success': file_success,
                                                    'status': update_entry.get(self.env_status_col), 'failure_jira_ticket': failure_jira_ticket.key,
                                                    'status_message': None, 'idc_data': idc_data, 'export_location': export_location}], \
                                                    NOTIFICATION_EMAILS, employer_names, claims_master_conn)
                    elif 'export' not in self.__fetch_capabilities():
                        logutil.log(LOG, logutil.INFO,"Export capability has been disabled. Not attempting to export imported claim file: %s." % str(icf_id))
                        
                        is_state_disabled = True
                        
#                except Warning as e:
#                    i = sys.exc_info()
#                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
#                    error_status = update_entry.get(self.env_status_col,'') + '-error'
#                    logutil.log(LOG, logutil.WARNING, "WARNINGS logged during Claims Export for imported_claim_file_id: %s." % str(icf_id))
#                    logutil.log(LOG, logutil.WARNING, "%s" % status_message)
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.CRITICAL, "SEVERE ERROR: %s" % status_message)
                    error_status = update_entry.get(self.env_status_col,'') + '-error'
                    
                finally:
                    if error_status:
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:error_status,
                                        self.env_processed_date_col:self.get_time()}
                        t_ucf.update(update_entry)
                        
                        self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                            'update_entry': update_entry, 'success': False,
                                            'status': update_entry.get(self.env_status_col), 'failure_jira_ticket': failure_jira_ticket.key,
                                            'status_message': status_message}], NOTIFICATION_EMAILS, employer_names, claims_master_conn)
                        
                    if not is_state_disabled:
                        s_ucf.append({'ucf':ucf,
                                      'command_options': command_options,
                                      'update_entry': update_entry, 
                                      'success': False if error_status else True,
                                      'status': update_entry.get(self.env_status_col),
                                      'status_message': status_message})
        finally:
            if claims_master_conn:
                claims_master_conn.close()
                
    def __drx_export(self):
        if 'drx_export_claims' not in self.__fetch_capabilities():
            return
        
        claims_master_conn = None
        claims_master_conn = self.get_connection()
        
        #get JIRA parameters and Jira Server session
        jira_params, jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
        v_jira_params, v_jira_timetracking_details = self.get_jira_session_and_params(self.parent_admin_service.test_jira)
        jira_params['jira_project'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_project')
        jira_params['components'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_components')
        jira_params['jira_issuetype'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_issuetype')
        jira_params['jira_assignee'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_assignee')
        jira_params['priority'] = self.parent_admin_service.properties.get('prod').get('claims_manager').get('export_jira_priority')
        
        #NetOps Jira project doesnt have severity and jira_duedate parameters
        if jira_params.has_key('severity_value'):
            jira_params.pop('severity_value')
            
        drx_emp_cursor = claims_master_conn.cursor()
        drx_emp_cursor.execute("""SELECT DISTINCT employer_id 
                                FROM {drx_db}.drx_export_files 
                                WHERE export_type = 'claim'
                                ORDER BY 1""".format(drx_db=whcfg.export_schema))
        drx_emp_ids = []
        [drx_emp_ids.append(emp['employer_id']) for emp in drx_emp_cursor.fetchall()]
        
        emp_ids_csv = ""
        for emp in drx_emp_ids:
            if len(emp_ids_csv) == 0:
                emp_ids_csv += str(emp)  
            else:
                emp_ids_csv += "," + str(emp)
        
        emp_cursor = claims_master_conn.cursor()
        emp_cursor.execute("""SELECT `key`, name FROM employers""")
        employer_names = {}
        [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]

        try:
            new_employer_ids = self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('drx_export_employer_ids')
            drx_export_file_location = self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('drx_export_file_location')
            if new_employer_ids != None and len(new_employer_ids) != 0:
                emp_ids_csv += "," + new_employer_ids
                
            t_ucf = dbutils.Table(claims_master_conn, UPLOADED_FILES_TABLE)
            t_ucf.search("status='received' AND file_type IN ('pharmacy_claims') AND %s='loaded-wh' and environment='%s' and employer_id in(%s)" \
                                                % (self.env_status_col, self.parent_admin_service.environment, emp_ids_csv))
            t_ucf.sort('id')
            r_ucf = []
            s_ucf = []
            for ucf in t_ucf:
                r_ucf.append(ucf)
                
            for ucf in r_ucf:
                update_entry = {}
                error_status = None
                status_message = None
                is_state_disabled = False
                command_options = {}
                try :
                    icf_id = ucf[self.env_icf_id_col]
                    command_options = self.resolve_command_options(ucf)

                    if 'drx_export_claims' in self.__fetch_capabilities() and self.parent_admin_service.environment == command_options.get('environment') \
                    and (command_options['load_state'] == 0 or command_options['load_state'] > 2):
                        logutil.log(LOG, logutil.INFO, "Exporting imported claim file: %s." % str(icf_id))
                        insurance_company_id = ucf["insurance_company_id"]
                        employer_id = ucf["employer_id"]
                        
                        drx_exporter_options = "-i %s" % insurance_company_id
                        drx_exporter_options += "-e %s" % employer_id
                        drx_exporter_options += "-c %s" % icf_id
                        export_status = drx_exporter.main(drx_exporter_options.split(" "))
                        if export_status == True:
                            drx_export_file_location += ("/" + "drx_export_claims_" + str(employer_id) + "_" + str(icf_id) + ".dmp")
                            drx_exporter_options = "-i %s" % insurance_company_id
                            drx_exporter_options += "-e %s" % employer_id
                            drx_exporter_options += "-c %s" % icf_id
                            drx_exporter_options += "-f %s" % drx_export_file_location
                            export_status = drx_file_exporter.main(drx_exporter_options.split(" "))
                            if export_status == True:
                                file_success = True
                        else:
                            file_success = False
                        
                        # Export/Validate
                        logutil.log(LOG, logutil.INFO, "Export finished: imported claim file-%s." % str(icf_id))
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:'claims-exported',
                                        self.env_processed_date_col:self.get_time()}
                        self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                                    'update_entry': update_entry, 'success': file_success,
                                                    'status': update_entry.get(self.env_status_col),
                                                    'status_message': None, 'export_location': drx_export_file_location}], \
                                                    NOTIFICATION_EMAILS, employer_names)
                    elif 'drx_export_claims' not in self.__fetch_capabilities():
                        logutil.log(LOG, logutil.INFO,"DRX Export capability has been disabled. Not attempting to export imported claim file: %s." % str(icf_id))
                        
                        is_state_disabled = True
                        
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.CRITICAL, "SEVERE ERROR: %s" % status_message)
                    error_status = update_entry.get(self.env_status_col,'') + '-error'
                    
                finally:
                    if error_status:
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:error_status,
                                        self.env_processed_date_col:self.get_time()}
                        t_ucf.update(update_entry)
                        
                        self.notify_users([{'ucf': ucf, 'command_options': command_options, 
                                            'update_entry': update_entry, 'success': False,
                                            'status': update_entry.get(self.env_status_col),
                                            'status_message': status_message}], NOTIFICATION_EMAILS, employer_names)
                        
                    if not is_state_disabled:
                        s_ucf.append({'ucf':ucf,
                                      'command_options': command_options,
                                      'update_entry': update_entry, 
                                      'success': False if error_status else True,
                                      'status': update_entry.get(self.env_status_col),
                                      'status_message': status_message})
        finally:
            if claims_master_conn:
                claims_master_conn.close()
    
    def _resolve_jira(self, jira_key, jira_params, comment):
        """ Resolves given JIRA ticket.
        """
        jira_params['jira'] = self.jira_rest.get_session()
        
        if not jira_key or jira_key == 'NA' or self.parent_admin_service.test_jira:
            return
         
        try:
            issue = jira_params['jira'].issue(jira_key)
            transitions = jira_params['jira'].transitions(issue)
            
            if '5' in [t['id'] for t in transitions] or 'Resolve Issue' in [t['name'] for t in transitions]:
                jira_params['jira'].add_comment(jira_key, comment)
                jira_params['jira'].transition_issue(issue, '5')
        except:
            pass

    def __process_rehash(self):
        """ Retrieves claim file ids that are ready for rehash and then runs the rehash script for them.
        """
        if 'rehash' not in self.__fetch_capabilities():
            logutil.log(LOG, logutil.INFO, "Rehash is disabled.Skipping rehash process")
            return
        
        logutil.log(LOG, logutil.INFO, "Starting processing files ready for rehash")
        conn = None
        conn = self.get_connection()
        rehash_file_query="""select T.*
                             from (
                             select icf.id as imported_claim_file_id, uf.*
                             from imported_claim_files icf join uploaded_files uf on icf.id = uf.%s_imported_claim_file_id
                             where icf.has_eligibility = 0 and uf.%s_status in ('waiting-for-eligibility', 'loaded-wh-validation-failure')
                             )T join patients p on p.updated_by_employer_id = T.employer_id 
                             GROUP BY T.imported_claim_file_id"""% (self.parent_admin_service.environment,self.parent_admin_service.environment)
        try:
            rehash_cursor=conn.cursor()
            rehash_cursor.execute(rehash_file_query)
            icf_ids = rehash_cursor.fetchall()
            
            logutil.log(LOG, logutil.INFO, "Identified the files ready for rehash")
            emp_cursor = conn.cursor()
            emp_cursor.execute("""SELECT `key`, name FROM employers""")
            employer_names = {}
            [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]
            emp_cursor.close()
            for icf_id_row in icf_ids:
                icf_id = icf_id_row['imported_claim_file_id']
                e_key = icf_id_row['employer_key']
                e_id = icf_id_row['employer_id']
                file_type = icf_id_row['file_type'].lower()
                p_id = icf_id_row['insurance_company_id']
                u_file_id = icf_id_row['id']
                status_message = None
                rehash_success = False
                file_success = False
                try:
                    logutil.log(LOG,logutil.INFO,"Running rehash for file id: %s"%icf_id)
                    rehash_options = {}
                    rehash_options['rehash_script_options'] = ("""-e %s -d patients -i %s""" % (e_key, icf_id)).split(' ')
                    lock_flag = None
                    if file_type == 'medical_claims':
                        rehash_options['group_id']=3
                        lock_flag = rehash_claims_master.main(rehash_options.get('rehash_script_options'))
                    elif file_type == 'pharmacy_claims':
                        rehash_options['group_id']=2
                        lock_flag = rehash_rx_claims_master.main(rehash_options.get('rehash_script_options'))
                    elif file_type == 'dental_claims':
                        rehash_options['group_id']=10
                        lock_flag = rehash_dental_claims_master.main(rehash_options.get('rehash_script_options'))     
                    if lock_flag == False:
                        continue
                    rehash_success = True
                    v_cursor = conn.cursor()
                    validate_status = 1
                    update_entry={'id':u_file_id, self.env_status_col:'loaded-wh-validation-failure', self.env_processed_date_col: '', 'data_quality_flag': 'NULL'}
                    try:
                        v_cursor.execute( "call validate_data('%s', '%s', '%s', '%s', @validation_status);", (e_id, p_id, icf_id, rehash_options.get('group_id')) )
                    except Warning as e:
                        logutil.log(LOG, logutil.INFO, "Warnings logged during Normalized Claims Validations for imported_claim_file_id: %s." % (icf_id))
                    except:
                        validate_status = 0
                        i = sys.exc_info()
                        status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR,"SEVERE ERROR: %s" % status_message)
                        logutil.log(LOG, logutil.ERROR, 'Validation Procedure failed for file id %s' % icf_id)
                    if validate_status == 1:
                        logutil.log(LOG, logutil.INFO, "Called validate_data procedure: %s" % icf_id)
                        v_cursor.execute("""SELECT @validation_status as validation_status""")
                        validate_data_status = v_cursor.fetchone()
                        if validate_data_status['validation_status'] != 1:
                            update_entry['data_quality_flag'] = 1
                            update_entry[self.env_status_col] = 'loaded-wh'
                            file_success = True
                    conn.commit()
                    v_cursor.close()
                    update_entry[self.env_processed_date_col] = self.get_time()
                    u_query = """UPDATE %s.uploaded_files 
                                    SET %s_status = '%s',
                                        %s_date_processed = '%s',
                                        data_quality_check_flag = %s
                                  WHERE %s_imported_claim_file_id = '%s'""" % (whcfg.claims_master_schema,
                                                                      self.parent_admin_service.environment,
                                                                      update_entry[self.env_status_col],
                                                                      self.parent_admin_service.environment,
                                                                      update_entry[self.env_processed_date_col],
                                                                      update_entry['data_quality_flag'],
                                                                      self.parent_admin_service.environment,
                                                                      icf_id)
                    try:
                        conn.cursor().execute(u_query)
                        set_has_eligibility = """update imported_claim_files icf join patients p
                                                 on icf.employer_id = p.updated_by_employer_id
                                                 set has_eligibility = 1 where icf.id = %s"""%(icf_id)
                        conn.cursor().execute(set_has_eligibility)
                        conn.commit()
                    except:
                        logutil.log(LOG, logutil.ERROR, "Error occured while updating the status of imported_claim_file %s" % icf_id )
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.ERROR, "SEVERE ERROR: %s" % status_message)
                finally:
                    self.notify_users([{'ucf': icf_id_row, 'command_options': self.resolve_command_options(icf_id_row), 
                                            'update_entry': update_entry, 'success': file_success,
                                            'status': update_entry.get(self.env_status_col),
                                            'status_message': status_message,
                                            'rehash': rehash_success}], NOTIFICATION_EMAILS, employer_names, conn)
        except Exception as e:
            logutil.log(LOG, logutil.ERROR, "Error occurred during rehash:%s" % e )
        finally:
            rehash_cursor.close()
            
            if conn:
                conn.close() 
                
    def __process_finalization(self):
        if 'finalize' not in self.__fetch_capabilities():
            return
        
        conn = None
        conn = self.get_connection()
        emp_cursor = conn.cursor()
        emp_cursor.execute("""SELECT `key`, name FROM employers""")
        employer_names = {}
        [employer_names.update({emp['key']: emp['name']}) for emp in emp_cursor.fetchall()]

        try:
            file_types = self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('file_types')
            insurance_company_ids = self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('insurance_company_ids')
            t_ucf = dbutils.Table(conn, UPLOADED_FILES_TABLE)
            t_ucf.search("""status='received' 
                            AND file_type IN (%s) 
                            AND %s='Loaded-wh' 
                            AND environment='%s'
                            AND insurance_company_id IN(%s)""" % (file_types, self.env_status_col, self.parent_admin_service.environment, insurance_company_ids))
            t_ucf.sort('id')
            r_ucf = []
            for ucf in t_ucf:
                r_ucf.append(ucf)
                
            for ucf in r_ucf:
                update_entry = {}
                error_status = None
                status_message = None
                command_options = {}
                try :
                    icf_id = ucf[self.env_icf_id_col]
                    command_options = self.resolve_command_options(ucf)
                    
                    if 'finalize' in self.__fetch_capabilities() and (command_options['load_state'] == 0 or command_options['load_state'] > 1):
                        #get employer name
                        employer_name = get_proper_casing(employer_names[command_options.get('employer_key')])
                        
                        #get insurance company name
                        insurance_company_name = "NA"
                        if command_options.get('insurance_company'):
                            insurance_company_name = get_proper_casing(command_options.get('insurance_company'))
                            
                        logutil.log(LOG, logutil.INFO,"Processing finalization for payer '%s' and employer '%s'" % (insurance_company_name, employer_name))
                        
                        finalization_options = "-f %s" % icf_id
                        finalize_claims.main(finalization_options.split(" "))
                except:
                    i = sys.exc_info()
                    status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
                    error_status = update_entry.get(self.env_status_col,'') + '-error'       
                finally:
                    if error_status:
                        update_entry = {'id':ucf['id'],
                                        self.env_status_col:error_status,
                                        self.env_processed_date_col:self.get_time()}
                        t_ucf.update(update_entry)
        finally:
            if conn:
                conn.close()
        

    def __send_email(self, s_ucfs, recipients = [NOTIFICATION_EMAILS]):
        
        if self.parent_admin_service.test_run:
           # recipients = ['dataops_offshore@castlighthealth.com']
            recipients = ['jtripathy@castlighthealth.com'] 
        
        email = ''
        hostname = socket.gethostname()
        try:
            username = os.getlogin()
        except OSError:
            # some terminal emulators do not update utmp, which is needed by getlogin()
            import pwd
            username = pwd.getpwuid(os.geteuid())[0]
        timestamp = datetime.datetime.now()
        sender = username + '@castlighthealth.com'
        hdr = 'From: \'claims_master_admin_service\'<%s>\r\nTo: %s\r\nSubject: %s claims_master_admin_service %s\r\n\r\n' % (sender, ', '.join(recipients), hostname, str(timestamp))
        append_string = ''
        for s_ucf in s_ucfs:
           append_string = append_string + "\n%s: file: %s, status: %s, status_message: %s" % (str(s_ucf['ucf']['id']), s_ucf['ucf']['source_file_name'], s_ucf['status'], s_ucf['status_message'] if s_ucf['status_message'] else '')
        email = hdr + append_string
        server = smtplib.SMTP('localhost')
        server.sendmail(sender, recipients, email)
        server.quit()
        
    def prepare_files_dashboard_url(self, employer, insurance_company, imported_claim_file_id, file_type, file_status):
        #Get IP address and Port of Files Dashboard UI
        hostname = socket.gethostname() 
        ip_address = socket.gethostbyname(hostname)
        port = SERVER_PORT
        
        file_status_lower = file_status.lower()
        file_type_lower = file_type.lower()

        #generate quality metrics URL
        url_params = [('employer', employer), ('payor', insurance_company), ('file_id', imported_claim_file_id), ('rows', '250')]
        file_dashboard_url = """http://%s:%s/filesdashboard/metrics/?""" % (ip_address, str(port))
        
        report_type_value = None
        if file_type_lower == 'medical_claims':
            url_params.append(('file_type', "Medical Claims"))
            
            if file_status_lower == 'staged-wh':
                report_type_value = "Raw"
            elif file_status_lower == 'loaded-wh':
                report_type_value = "Normalized"
            elif file_status_lower == 'claims-exported':
                report_type_value = "IDC"
            elif 'stag' in file_status_lower:
                report_type_value = "Raw"
            elif 'load' in file_status_lower:
                report_type_value = "Normalized"
            elif 'export' in file_status_lower:
                report_type_value = "IDC"
        elif file_type_lower == 'pharmacy_claims':
            url_params.append(('file_type', "Pharmacy Claims"))
            
            if 'stag' in file_status_lower:
                report_type_value = "Raw"
            elif 'load' in file_status_lower:
                report_type_value = "Rx"
        elif file_type_lower == 'dental_claims':
            url_params.append(('file_type', "Dental Claims")) 

            if 'stag' in file_status_lower:
                report_type_value = "Raw"
            elif 'load' in file_status_lower:
                report_type_value = "Normalized"
            elif 'export' in file_status_lower:
                report_type_value = "IDC"
            
        url_params.append(('report_type', report_type_value))
        
        #handle special characters in the URL
        return file_dashboard_url + urllib.urlencode(url_params)
        
    def notify_users(self, s_ucf, recipients, employer_names, conn, is_multiple_email=True):
        """ Sends user alerts via email messages 
        """
        settings_cursor = conn.cursor()

        uploaded_file_ids = [each_ucf['ucf']['id'] for each_ucf in s_ucf]

        logutil.log(LOG, logutil.INFO, "Sending email notification for uploaded_file_ids=%s, recipients=%s" % (str(uploaded_file_ids), str(recipients)))

        hostname = socket.gethostname()
        
        if self.parent_admin_service.test_run:
            try:
                username = os.getlogin()
            except OSError:
                # some terminal emulators do not update utmp, which is needed by getlogin()
                import pwd
                username = pwd.getpwuid(os.geteuid())[0]
                
            recipients = [username + '@castlighthealth.com']

        timestamp = datetime.datetime.now()
        subject = "%s claims_master_admin_service %s" % (hostname, str(timestamp))
        subject = " ".join(subject.split())
        
        from_email = 'claims_master_admin_service <wh_ops@castlighthealth.com>'
        template_name = 'staged_loaded_email'
        
        context_data = []
        for each_ucf in s_ucf:
            uploaded_file_id = str(each_ucf['ucf']['id'])
            
            file_status = each_ucf['status']
            file_success = each_ucf['success']
            #file_status_lower = None
            #if file_status is not None:
            file_status_lower = file_status.lower()
            if each_ucf['status_message'] is not None and type(recipients) is not list:
                recipients = [each_user.strip() for each_user in recipients.split(',')] 
            
            if 'error' in file_status_lower or 'failure' in file_status_lower:
                file_success = False
            
            if file_success == True:
                success_string = 'Success'
            else:
                success_string = 'Error'
            load_type_string = None
            #if file_status_lower is None:
            #    load_type_string = None
            if file_status_lower == 'staged-wh':
                load_type_string = 'staged'
            elif file_status_lower == 'loaded-wh':
                load_type_string = 'loaded'
            elif file_status_lower == 'claims-exported':
                load_type_string = 'exported'
            elif 'export' in file_status_lower:
                load_type_string = 'exporting'
            elif 'stag' in file_status_lower:
                load_type_string = 'staging'
            elif 'load' in file_status_lower:
                load_type_string = 'loading'
            if 'rehash' in each_ucf:
                load_type_string = 'rehash'
                if each_ucf['rehash'] == True:
                    file_success = True

            imported_claim_file_id = str(each_ucf['update_entry'].get(self.env_icf_id_col, each_ucf['ucf'][self.env_icf_id_col]))
            processed_date = each_ucf['update_entry'].get(self.env_processed_date_col, each_ucf['ucf'][self.env_processed_date_col])
            
            file_type = each_ucf['ucf']['file_type']
            file_type_lower = file_type.lower()
            file_type = get_proper_casing(file_type)
            employer = get_proper_casing(employer_names[each_ucf['command_options']['employer_key']])
            
            insurance_company = "NA"
            if each_ucf['command_options']['insurance_company']:
                insurance_company = get_proper_casing(each_ucf['command_options']['insurance_company'])
                    
#            subject = """%s from %s %s has been successfully %s""" % (file_type, employer,
#                         insurance_company if insurance_company != "NA" else "", load_type_string)
#    
#            subject = " ".join(subject.split())

            #generate quality metrics URL
            file_dashboard_url = self.prepare_files_dashboard_url(employer, insurance_company, imported_claim_file_id, file_type_lower, file_status)

            if not file_success:
                if is_multiple_email:
                    if 'export' in load_type_string:
                        subject = "Claims File Export- %s- %s- %s- %s- %s" % (success_string, file_type, employer, \
                                                                              insurance_company, str(processed_date))
                    elif 'load' in load_type_string:
                        subject = "Claims File Load- %s- %s- %s- %s- %s" % (success_string, file_type, employer, \
                                                                              insurance_company, str(processed_date))
                    elif 'stag' in load_type_string:
                        subject = "Claims File Stage- %s- %s- %s- %s- %s" % (success_string, file_type, employer, \
                                                                              insurance_company, str(processed_date))
                    elif 'rehash' in load_type_string:
                        subject = "Claims File Rehash- %s- %s- %s- %s- %s" % (success_string, file_type, employer, \
                                                                              insurance_company, str(processed_date))
                    subject = " ".join(subject.split())

                context_data = {'success': file_success,
                            'name': each_ucf['ucf']['file_name'],
                            'employer': employer,
                            'insurance_company': insurance_company,
                            'type': file_type, 
                            'uploaded_file_id': uploaded_file_id,
                            'imported_claim_file_id': imported_claim_file_id,
                            'file_status': get_proper_casing(each_ucf['status']),
                            'load_type_string': load_type_string,
                            'load_date': processed_date,
                            'error_message': each_ucf['status_message'],
                            'file_dashboard_url': file_dashboard_url,
                            'idc_data': each_ucf.get('idc_data', None),
                            'export_location': each_ucf.get('export_location', None),
                            'failure_jira_ticket': each_ucf.get('failure_jira_ticket', None)
                            }
                context_data_list = [context_data]

                logutil.log(LOG, logutil.INFO, "Email data prepared for uploaded_file_id: %s" % (str(uploaded_file_id),))

                if self.parent_admin_service.test_run or (each_ucf['status_message'] is not None) :
                    self.django_email.send_email_template(template_name, context_data_list, subject, recipients, from_email)
                else:
                    query_str = """INSERT INTO %s.load_failures (name,employer,insurance_company,type,uploaded_file_id,imported_claim_file_id,file_status,load_date,file_dashboard_url,failure_jira_ticket)
                                   VALUES ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")
                                """ % (whcfg.files_dashboard_ui_schema,\
                                    context_data['name'],\
                                    context_data['employer'],\
                                    context_data['insurance_company'],\
                                    context_data['type'],\
                                    context_data['uploaded_file_id'],\
                                    context_data['imported_claim_file_id'],\
                                    context_data['file_status'],\
                                    context_data['load_date'],\
                                    context_data['file_dashboard_url'],\
                                    context_data['failure_jira_ticket'])
                    try:
                        settings_cursor.execute(query_str)
                    except Exception, e:
                        pass

    def __update_google_spreadsheet(self):
#        if self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('google_file_dashboard_key'):
#            t_ucf = dbutils.Table(self.claims_master_conn, UPLOADED_FILES_TABLE)
#            header_list = [c.replace('_','') for c in t_ucf.columns()]
#            num_rows = len(t_ucf)
#            data_dict_list = [{k.replace('_',''):str(v) for k,v in e.iteritems()} for e in t_ucf[:num_rows]]
#            
#            utils.google_worksheet_from_dict(header_list = header_list, 
#                                   data_dict_list = data_dict_list, 
#                                   username = self.parent_admin_service.properties.get('config').get('google_username'), 
#                                   password = self.parent_admin_service.properties.get('config').get('google_password'), 
#                                   spreadsheet_key = self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_manager').get('google_file_dashboard_key'),
#                                   worksheet_name = 'Uploaded Claims File Status',
#                                   replace_worksheet = True)    
            return None
    
    def _run(self, a):
        
        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Polling for new claims to be processed..')
        t = threading.currentThread()
        
        with self.ifc_condition:
            logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Got lock for IFC data by Claims Manger thread.')
            
            conn = None
            refresh_ifc_settings = None
            try:
                conn = self.get_connection()
                settings_cursor = conn.cursor()
                    
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Got DB connection, will check 'refresh_ifc' setting... ")
                
                settings_cursor.execute("""SELECT `key`, value FROM claims_master_admin_service_settings WHERE `key`='refresh_ifc'""")
                refresh_ifc_settings = settings_cursor.fetchall()
                
            finally:
                if conn:
                    conn.close()
            if refresh_ifc_settings and refresh_ifc_settings[0] and not int(refresh_ifc_settings[0]['value']):
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::refresh_ifc' settings is not set(clear)... Waiting for IFC data...")
                self.ifc_condition.wait()
                
                if not self.parent_admin_service.test_run:
                   # self.__process_received()
            
                    #self.__process_staged()
        
                    self.__process_rehash()
                    
                    self.__process_finalization()
                    
                    self.__export_claims()
                    self.__process_staged()
                    self.__export_claims()
                    self.__process_received()
                    
                    self.__drx_export()
                else:
                    logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Service running in test mode. Not acting on any new files uploaded..')
                
    #        self.__update_google_spreadsheet()
                
                self.lc = self.lc + 1
            else:
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::'refresh_ifc' settings is set ... Not acting on any new files uploaded..")

        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Released lock for IFC data by Claims Manger thread.')

    def run_before_expire(self):
        print "Expiring Claims Manager Thread!"


class ClaimsFileManager(BaseManager):
    
    SOURCE_MAPPINGS = {'medical_claims':'insurance_company',
                        'pharmacy_claims':'insurance_company',
                        'eligibility':'employer_key'}
    
    def pre_existing_file_check(self, nf, command_options):
        conn = None
        try:
            conn = self.get_connection()
            fac_ucf = model.ModelFactory.get_instance(self.get_connection(), UPLOADED_FILES_TABLE)      
            
            uf_entry = fac_ucf.find({'source_file_path':nf.get('source_file_path').rstrip('/'),
                                     'source_file_name':nf.get('source_file_name')})
            if uf_entry:
                if uf_entry.get('file_type') == 'eligibility':
                    if uf_entry.get('file_detection_rule'):
                        return True
                else:
                    return True
            elif (command_options.get('destination_folder')
                    and os.path.exists(command_options.get('destination_folder')) 
                    and os.path.exists(command_options['destination_folder'] + '/' + nf['source_file_name'].replace(' ','_'))
                    ):
                return True
            
            return False    
#            return fac_ucf.find({'source_file_path':nf.get('source_file_path'),
#                                      'source_file_name':nf.get('source_file_name')}) or \
#                   (command_options.get('destination_folder')
#                    and os.path.exists(command_options.get('destination_folder')) 
#                    and os.path.exists(command_options['destination_folder'] + '/' + nf['source_file_name'].replace(' ','_'))
#                    )
        finally:
            if conn:
                conn.close()
    
    def resolve_source(self, command_options):
#        source = None
#        if command_options:
#            if command_options.get('file_type') == 'acs_hra':
#                source = 'ACS'
#            else:
#                source = ClaimsFileManager.SOURCE_MAPPINGS.get(command_options.get('file_type'))
#        return source
        return command_options.get('source')
    
    def new_file(self, nf, command_options, dimensions = None):
        
        pre_existing_file = self.pre_existing_file_check(nf, command_options)
        source = self.resolve_source(command_options)
        message = None
        
        if command_options.get('employer_key'):
            if command_options.get('employer_key') <> 'warehouse':
                if not pre_existing_file:
#                    print '''cp '%s/%s' %s/%s''' % (nf['file_path'], nf['file_name'], command_options['destination_folder'], nf['file_name'].replace(' ','_'))
                    
                    if self.parent_admin_service.test_run:
                        logutil.log(LOG, logutil.INFO,"Test Run: File Type: %s. Not creating entry for source: %s/%s, destination: %s/%s in %s.." % (command_options.get('file_type'), nf['source_file_path'], nf['source_file_name'], command_options.get('destination_folder',''), nf['source_file_name'].replace(' ','_'), UPLOADED_FILES_TABLE))
                    else:
                        if command_options.get('file_type') == 'acs_hra':
                            logutil.log(LOG, logutil.INFO,"Copying file from %s/%s to %s/%s" % (nf['source_file_path'], nf['source_file_name'], command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                            shutil.copyfile('%s/%s' % (nf['source_file_path'], nf['source_file_name']), '%s/%s' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                            logutil.log(LOG, logutil.INFO,"Copying file Done!")
                            
                        elif (command_options.get('file_type') == 'medical_claims' or command_options.get('file_type') == 'pharmacy_claims' or command_options.get('file_type') == 'medical_claims_dimension'):   
                            logutil.log(LOG, logutil.INFO,"ifc_id : %s" % (command_options['ifc_id']))
                            logutil.log(LOG, logutil.INFO,"Copying file from %s/%s to %s/%s" % (nf['source_file_path'], nf['source_file_name'], command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                           
                            #if nf['source_file_name'] == "Castlight_Shields_Membership_-en-us-csv_desc_20150226.xml":
                            shutil.copyfile('%s/%s' % (nf['source_file_path'], nf['source_file_name']), '%s/%s' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                            logutil.log(LOG, logutil.INFO,"Copying file Done!")
                        elif command_options.get('destination_folder'):
                            logutil.log(LOG, logutil.INFO,"Copying file from %s/%s to %s/%s" % (nf['source_file_path'], nf['source_file_name'], command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                            shutil.copyfile('%s/%s' % (nf['source_file_path'], nf['source_file_name']), '%s/%s' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
                            logutil.log(LOG, logutil.INFO,"Copying file Done!")
                        
                        #File size in KB
                        file_size = os.stat(os.path.join(nf['source_file_path'], nf['source_file_name']))[6] / 1024
                        
                        #Decrypt PGP files if setting is enabled in the IFC data table
                        decrypt_file_name = nf['source_file_name'].replace(' ', '_')
                        if command_options['is_encrypted'] == 1 and len(decrypt_file_name) > 4 and \
                        (decrypt_file_name[-3:].lower() == 'pgp' or decrypt_file_name[-3:].lower() == 'gpg'):
                            logutil.log(LOG, logutil.INFO,"File %s/%s is PGP encrypted, trying to decrypt it..." % \
                                                                    (command_options['destination_folder'], decrypt_file_name))
                            
                            #remove '.pgp' extension from decrypted file name
                            decrypt_file_name = decrypt_file_name[:-4]
                                
                            decryption_cmd = 'gpg --batch --passphrase-file %s --output %s/%s --decrypt %s/%s' % (PASSPHRASE_FILE, \
                                                                    command_options['destination_folder'], decrypt_file_name, \
                                                                    command_options['destination_folder'], nf['source_file_name'].replace(' ', '_'))
                            
                            p = subprocess.Popen(decryption_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                            cmd_output = p.communicate()[0]
                            p.poll()
                            
                            if p.returncode != 0:
                                logutil.log(LOG, logutil.CRITICAL, cmd_output)
                                logutil.log(LOG, logutil.CRITICAL, "File Decryption failed!")
                                
                                nf['uploaded_file_id'] = -1
                                return "File Decryption failed, File is %s/%s. \n%s" % (command_options['destination_folder'], \
                                                                    nf['source_file_name'].replace(' ', '_'), cmd_output)
                                
                            else:
                                logutil.log(LOG, logutil.INFO, cmd_output)
                                logutil.log(LOG, logutil.INFO, "File Decrypted successfully.")
                                #File size in KB for decrypted file
                                file_size = os.stat(os.path.join(command_options['destination_folder'], decrypt_file_name))[6] / 1024
                                try:
                                    os.remove('%s/%s' % (command_options['destination_folder'], nf['source_file_name'].replace(' ', '_')))
                                    logutil.log(LOG, logutil.INFO, "Removed Encrypted %s/%s file from destination redirectory." \
                                                                    % (command_options['destination_folder'], nf['source_file_name'].replace(' ', '_')))
                                except os.error:
                                    logutil.log(LOG, logutil.INFO, "Error during file deletion: Encrypted %s/%s file from destination redirectory." \
                                                                    % (command_options['destination_folder'], nf['source_file_name'].replace(' ', '_')))
                                 
                        # The date_received column is present in uploaded_files as well as the 
                        # files table for dashboard UI. Discuss and remove the one in files table going ahead
                        # For now, for matching the time in both tables, use this variable
#                        file_received_time = self.get_time()
                        file_received_time = datetime.datetime.fromtimestamp(os.stat(os.path.join(nf['source_file_path'], nf['source_file_name']))[8])
                        insurance_company_id = self.insurance_companies.get(command_options.get('insurance_company','').lower())
                        
                        conn = None
                        uploaded_file_id = None
                        try:
                            conn = self.get_connection()
                            fac_ucf = model.ModelFactory.get_instance(conn, UPLOADED_FILES_TABLE)
                            
                            if command_options.get('file_type') == 'eligibility':
                                uploaded_file = fac_ucf.find({'source_file_name':nf['source_file_name'],
                                                              'source_file_path':nf['source_file_path'].rstrip('/')})
                                
                                if uploaded_file:
                                    logutil.log(LOG, logutil.INFO,"Updating entry for file %s/%s in %s.." % (command_options.get('destination_folder'), nf['source_file_name'].replace(' ','_'), UPLOADED_FILES_TABLE))
                                    uploaded_file_id = uploaded_file['id']
                                    #Trying to find a matching import_file_config for uploaded file
                                    query_str = """    SELECT ifc.`id`, ifc.`file_type`, ifc.`monitor_directory`, ifc.`file_detection_rule`,
                                                       ifc.`does_not_contain`, ifc.`file_extension`
                                                       FROM `import_file_config` ifc
                                                       WHERE ifc.`file_type`='eligibility'
                                                       AND ifc.`monitor_directory`=%s
                                                """
                                    conn_cursor = conn.cursor()
                                    conn_cursor.execute(query_str,(nf['source_file_path'].rstrip('/'),))
                                    ifc_data = conn_cursor.fetchall()
                                    matching_file_config = None
                                    for each_config in ifc_data:
                                        file_detection_rule = {'contains':each_config['file_detection_rule'],
                                                               'does_not_contain':each_config['does_not_contain'],
                                                               'file_extension':each_config['file_extension']}
                                        if self.check_file(nf['source_file_name'], file_detection_rule):
                                            matching_file_config = each_config
                                            break
                                    
                                    if matching_file_config:
                                        fac_ucf.table.update({'id':uploaded_file_id, 'file_size':file_size,
                                                              'import_file_config_id':matching_file_config['id'],
                                                              'file_detection_rule':matching_file_config['file_detection_rule']})
                                    else:
                                        logutil.log(LOG, logutil.INFO,"Matching file config rule not found for uploaded file id = %s" \
                                                                        % (uploaded_file_id,))
                                        fac_ucf.table.update({'id':uploaded_file_id, 'file_size':file_size,
                                                              'file_detection_rule':command_options['file_detection_rule_contains']})
                                else:
                                    logutil.log(LOG, logutil.INFO,"Creating entry for file %s/%s in %s.." % (command_options.get('destination_folder'), decrypt_file_name, UPLOADED_FILES_TABLE))
                                    uploaded_file = fac_ucf.create({'source_file_name':nf['source_file_name'],
                                                         'source_file_path':nf['source_file_path'].rstrip('/'),
                                                         'file_name': decrypt_file_name,
                                                         'file_path':nf['source_file_path'].rstrip('/'),
                                                         'date_received':file_received_time,
                                                         'file_type':command_options.get('file_type'),
                                                         'file_size':file_size,
                                                         'source':source,
                                                         'status':'received',
                                                         'employer_key':command_options.get('employer_key'),
                                                         'insurance_company_name':command_options.get('insurance_company'),
                                                         'employer_id':self.employers.get(command_options.get('employer_key')),
                                                         'insurance_company_id':insurance_company_id,
                                                         'environment':command_options['environment'],
                                                         'file_detection_rule':command_options['file_detection_rule_contains'],
                                                         'parent_uploaded_file_id':command_options.get('parent_uploaded_file_id'),
                                                         'import_file_config_id':command_options.get('ifc_id')})
                                    
                                    uploaded_file_id = uploaded_file['id']                                    
                            else:
                                logutil.log(LOG, logutil.INFO,"Creating entry for file %s/%s in %s.." % (command_options.get('destination_folder'), decrypt_file_name, UPLOADED_FILES_TABLE))
                                uploaded_file = fac_ucf.create({'source_file_name':nf['source_file_name'],
                                                     'source_file_path':nf['source_file_path'].rstrip('/'),
                                                     'file_name': decrypt_file_name,
                                                     'file_path':command_options.get('destination_folder'),
                                                     'date_received':file_received_time,
                                                     'file_type':command_options.get('file_type'),
                                                     'file_size':file_size,
                                                     'source':source,
                                                     'status':'received',
                                                     'employer_key':command_options.get('employer_key'),
                                                     'insurance_company_name':command_options.get('insurance_company'),
                                                     'employer_id':self.employers.get(command_options.get('employer_key')),
                                                     'insurance_company_id':insurance_company_id,
                                                     'environment':command_options['environment'],
                                                     'file_detection_rule':command_options['file_detection_rule_contains'],
                                                     'parent_uploaded_file_id':command_options.get('parent_uploaded_file_id'),
                                                     'import_file_config_id':command_options.get('ifc_id')})
                                
                                uploaded_file_id = uploaded_file['id']
                            
                        finally:
                            if conn:
                                conn.close()
                        
                        if dimensions:
                            for df, df_command_options in dimensions.iteritems():
                                df_command_options['parent_uploaded_file_id'] = uploaded_file_id
                                self.new_file(dict(df), df_command_options)
                        else:
                            nf['uploaded_file_id'] = uploaded_file_id
                            message = "%s file(%s file type) from %s %s is arrived." % (nf['source_file_name'], command_options.get('file_type'), \
                                                            command_options.get('employer_key'), command_options.get('insurance_company'), )

                        if command_options.get('file_type') == 'acs_hra':
                            logutil.log(LOG, logutil.INFO,"Starting to load ancillary data...")
                            acs_options = """-i %s  -t acs_hra -e %s -f %s/%s""" % (command_options.get('insurance_company'), command_options.get('employer_key'), command_options['destination_folder'], nf['source_file_name'].replace(' ','_'))
                            load_ancillary_data.main(acs_options.split(' '))
                            logutil.log(LOG, logutil.INFO,"Ancillary data load done.")

                            if message:
                                message += '\nACS HRA file %s/%s loaded successfully!' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_'))
#                            self._send_generic_email(body = 'ACS HRA file %s/%s loaded successfully!' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))

            else:
                # This is the elig dump
                if command_options.get('file_type') == 'eligibility_dump':
                    lock_file = None
                    lock_file = claims_util.ClaimsLockFileHelper.get_instance()
                    if lock_file.acquire_lock():
                        logutil.log(LOG, logutil.INFO,"Loading eligibility tables from %s" % nf['source_file_name'])
                        os.system('''mysql -u %s -p'%s' %s < %s/%s''' % (whcfg.claims_master_user, whcfg.claims_master_password,whcfg.claims_master_schema, nf['source_file_path'], nf['source_file_name']))
                        backup_filename = command_options['destination_folder'] + "/" + nf['source_file_name'] + "_%s" % datetime.datetime.now().strftime("%Y%m%d")
                        logutil.log(LOG, logutil.INFO,"Creating backup of %s in %s" % (nf['source_file_name'], backup_filename))
    #                    os.system('cp %s/%s %s' % (nf['file_path'], nf['file_name'], backup_filename))
                        logutil.log(LOG, logutil.INFO,"Copying file from %s/%s to %s" % (nf['source_file_path'], nf['source_file_name'], backup_filename))
                        shutil.copyfile('%s/%s' % (nf['source_file_path'], nf['source_file_name']), backup_filename)
                        logutil.log(LOG, logutil.INFO,"Copying file Done!")
                        
#                        logutil.log(LOG, logutil.INFO,"Regenerating Employee Geographies...")
#                        conn = None
#                        try:
#                            conn = self.get_connection()
#                            claims_util.regenerate_employee_geographies(conn, LOG)
#                            
#                            # Dump this table as a csv
#                            utils.dump_to_csv(conn, whcfg.claims_master_schema, 'employee_geographies', '/share/whdata/backup/employee_geographies/employee_geographies.csv', True)
#                            
#                            utils.dump_to_csv(conn, whcfg.claims_master_schema, 'employee_geographies_summary', '/share/whdata/backup/employee_geographies/employee_geographies_summary.csv', True)
#                        finally:
#                            if conn:
#                                conn.close()    
#                        # TODO: Can we trap an error if one occurs?
#                        message = 'Eligibility tables dump file %s restored successfully!' % nf['source_file_name']
#                        self._send_generic_email(body = 'Eligibility tables dump file %s restored successfully!' % nf['source_file_name'])
                    else:
                        status_message = "Unable to acquire lock file: %s." % lock_file.lock_file_location
                        logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
                    
                    if lock_file:
                        if not lock_file.release_lock():
                            logutil.log(LOG, logutil.WARNING, "Unable to release lock file: %s." % lock_file.lock_file_location)
                        else:
                            logutil.log(LOG, logutil.INFO, "Successfully released lock file: %s." % lock_file.lock_file_location)
#                elif command_options.get('file_type') == 'eligibility_control':
#                    if not pre_existing_file:
#                        logutil.log(LOG, logutil.INFO,"Received Eligibility Control File %s" % nf['source_file_name'])
#                        logutil.log(LOG, logutil.INFO,"Copying file from %s/%s to %s/%s" % (nf['source_file_path'], nf['source_file_name'], command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
#                        shutil.copyfile('%s/%s' % (nf['source_file_path'], nf['source_file_name']), '%s/%s' % (command_options['destination_folder'], nf['source_file_name'].replace(' ','_')))
#                        logutil.log(LOG, logutil.INFO,"Copying file Done!")
#                        eligibility_file_location = self._update_eligibility_status(nf)
#                        if eligibility_file_location:
#                            message = 'Updated successful load status of Eligibility file: %s' % eligibility_file_location
        return message
    
    def _update_eligibility_status(self, nf):      
        import_file_name = nf['source_file_name'] 
        import_file_path = nf['source_file_path']
        
        elig_file = open(import_file_path + '/' + import_file_name)
        if elig_file:
            file_entry = elig_file.readline()
            if file_entry: 
                eligibility_file_location = file_entry.rstrip('\n')
                ix = eligibility_file_location.rfind('/')
                if ix > 0:
                    eligibility_file_name = eligibility_file_location[ix+1:]
                    eligibility_file_path = eligibility_file_location[:ix]
                    u_query = """UPDATE %s.uploaded_files 
                                    SET %s_status = 'loaded-production',
                                        %s_date_processed = '%s'
                                  WHERE source_file_path = '%s'
                                    AND source_file_name = '%s'""" % (whcfg.claims_master_schema,
                                                                      self.parent_admin_service.environment,
                                                                      self.parent_admin_service.environment,
                                                                      self.get_time(),
                                                                      eligibility_file_path,
                                                                      eligibility_file_name)
                    conn = None
                    try:
                        conn = self.get_connection()
                        conn.cursor().execute(u_query)
                    finally:
                        if conn:
                            conn.close()
                    return eligibility_file_location
                
    def _get_file_skip_extensions(self):
        self.skip_file_extensions = []
        self.is_skip_some_file_extensions = (self.parent_admin_service.properties.get(self.parent_admin_service.environment)
                                             .get('claims_file_manager').get('is_skip_some_file_extensions', True))
        logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Skip some file extensions = %s" % (str(self.is_skip_some_file_extensions),))
        
        if self.is_skip_some_file_extensions:
            skip_extensions = (self.parent_admin_service.properties.get(self.parent_admin_service.environment)
                               .get('claims_file_manager').get('skip_file_extensions', SKIP_FILE_EXTENSIONS))
            self.skip_file_extensions = [item.lower() for item in skip_extensions.split(', ')]
            logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Skip files with extensions = %s" % (str(self.skip_file_extensions),))
            
    def _run(self, a):
        #print repr(a)
        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Polling for new Files to be processed...')
        t = threading.currentThread()
        
        with self.ifc_condition:
            logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Got lock for IFC data by Claims Files Manger thread.')
            
            conn = None
            refresh_ifc_settings = None
            try:
                conn = self.get_connection()
                settings_cursor = conn.cursor()
                    
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Got DB connection, will check 'refresh_ifc' setting... ")
                
                settings_cursor.execute("""SELECT `key`, value FROM claims_master_admin_service_settings WHERE `key`='refresh_ifc'""")
                refresh_ifc_settings = settings_cursor.fetchall()
                
            finally:
                if conn:
                    conn.close()

            if refresh_ifc_settings and refresh_ifc_settings[0] and not int(refresh_ifc_settings[0]['value']):
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::refresh_ifc' settings is not set(clear)... Waiting for IFC data...")

                self.ifc_condition.wait()
                
                #update monitor directories after IFC data refresh
                self.monitor_directories = list(set([v.get('monitor_directory') for v in self.imported_claim_tables.values() if v.get('monitor_directory')]))
                
                file_modification_max_seconds = int(self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_file_manager').get('file_modification_max_seconds','3600'))
                file_modification_min_seconds = int(self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_file_manager').get('file_modification_min_seconds','300'))
                if self.lc == 0:
                    first_time_file_modification_max_days = int(self.parent_admin_service.properties.get(self.parent_admin_service.environment).get('claims_file_manager').get('first_time_file_modification_max_days','0'))
                    if first_time_file_modification_max_days > 0:
                        file_modification_max_seconds = 60 * 60 * 24 * first_time_file_modification_max_days
                logutil.log(LOG, logutil.INFO, "%s::Monitoring the following directories: \n%s" % (str(self.t_name), ',\n'.join(self.monitor_directories)))
        #        print file_modification_max_seconds
        #        print file_modification_min_seconds        
                new_file_list = []
                # getting file extensions which we want to skip
                self._get_file_skip_extensions()
                for md in self.monitor_directories:
                    subdir_search = any(v.get('subdir_search') == 1 and v.get('monitor_directory') == md for v in self.imported_claim_tables.values())
                    for root, dirs, files in os.walk(md):
                        for file_name in fnmatch.filter(files,"*"):
                            file=os.path.join(root,file_name)
                            stats = os.stat(file)
                            current_time = time.time()
                            delta = current_time - stats[8]
                            if file[file.rfind('/')+1:] == 'a_p_po_pc.dmp':
                                last_load_time = self.internal_properties.get('last_elig_load_time', 0)
                                ll_delta = current_time - last_load_time
                                # Only load it if the file is less than a day old and the last file loaded was more than a day ago
                                if delta < 3600 and ll_delta > 86400:
                                    # Add eligibility file to the list if it is less than a day old
                                    new_file_list.append({'source_file_path':root , 'source_file_name':file[file.rfind('/')+1:]})
                                    self.internal_properties['last_elig_load_time'] = current_time
                                    
                            elif self.is_skip_some_file_extensions and any([file.lower().endswith(skip_ext) for skip_ext in self.skip_file_extensions]):
                                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::Skip file: " + file)
                                continue
                            elif delta > file_modification_min_seconds and delta < file_modification_max_seconds:
        #                        print folder, file[file.rfind('/')+1:]
                                new_file_list.append({'source_file_path':root , 'source_file_name':file[file.rfind('/')+1:]})      
                        if subdir_search == False:
                            break
                
        #        print new_file_list, delta, " ", file_modification_min_seconds, " ", file_modification_max_seconds
                email_message = []
                
                new_parent_file_list = []
                new_standalone_dimension_file_list = []
                new_file_map = {}
                new_file_keys = {}
                
                all_referenced_dimensions = self.all_referenced_dimensions()
                
                for nf in new_file_list:
                    command_options = self.resolve_command_options(nf)
                    if command_options:
                        nf_tuple = tuple(nf.items())
                        new_file_map[nf_tuple] = command_options
                        new_file_keys['%s-%s' % (command_options.get('yml_entry'), command_options.get('employer_key'))] = nf_tuple
                        if command_options.get('file_type') <> 'medical_claims_dimension':
                            new_parent_file_list.append(nf)
                        elif command_options.get('yml_entry') not in all_referenced_dimensions:
                            # Only add the file to the new_standalone_dimension_file_list
                            # if it is not in the list of referenced dimensions.
                            # A standalone dimension is one that is not referenced by any 
                            # other entry in the imported_claim_tables.yml file
                            new_standalone_dimension_file_list.append(nf)
        
                err_msg = "SEVERE: Skipping file"
                for nf in new_parent_file_list:
                    command_options = new_file_map.get(tuple(nf.items()))
                    dimensions = command_options.get('dimensions', None)
                    dimensions_map = {}
                    all_dependencies_met = True
                    
                    if dimensions:
                        for dimension in dimensions:
                            dimension_file = new_file_keys.get('%s-%s' % (dimension, command_options.get('employer_key')))
                            if dimension_file:
                                dimensions_map[dimension_file] = new_file_map.get(dimension_file)
                            else:
                                all_dependencies_met = False
                    if command_options:
                        if all_dependencies_met:
                            message = self.new_file(nf, command_options, dimensions_map)
                            if message:
                                email_message.append({nf['uploaded_file_id']: message})
        #                        email_message = email_message + message + '\n'
                        else:
                            email_message.append({'%s' % nf: err_msg + ': %s, since all dependencies not met.\n' % nf})
        #                    email_message = email_message + 'SEVERE: Skipping file: %s, since all dependencies not met.\n' % nf
        
                for nf in new_standalone_dimension_file_list:
                    command_options = new_file_map.get(tuple(nf.items()))
        
                    if command_options:
                        message = self.new_file(nf, command_options, None)
                        if message:
                            email_message.append({nf['uploaded_file_id']: message})
        #                    email_message = email_message + message + '\n'
                        
        #        for nf in new_file_list:
        #            command_options = self.resolve_command_options(nf)
        #            if command_options:
        #                message = self.new_file(nf, command_options)
        #                if message:
        #                    email_message = email_message + message + '\n'
                
                if len(email_message) > 0:
                    email_style = self.parent_admin_service.properties.get('prod').get('claims_file_manager').get('bulk_email_style')
                    
                    if email_style == 'old':
                        text_body = "\n" + "\n\n".join([str(each_msg.values()[0]) for each_msg in email_message]) + "\n"
                        self._send_generic_email(text_body)
                    elif email_style == 'new_multiple_email':
                        self.send_html_email(email_message, FILE_ARRIVAL_NOTIFICATION_EMAILS, err_msg)
                    
                self.lc = self.lc + 1
            
            else:
                logutil.log(LOG, logutil.INFO, str(self.t_name) + "::'refresh_ifc' settings is set ... Not acting on any new files...")

        tk = dbutils.connection_registry.keys()
        s = 0
        for t in tk:
            try:
                if dbutils.connection_registry.get(t).open:
                    s = s + 1
                else:
                    dbutils.connection_registry.pop(t)             
            except:
                dbutils.connection_registry.pop(t)
                    
        logutil.log(LOG, logutil.INFO, str(self.t_name) + '::Released lock for IFC data by Claims Files Manger thread.')
        logutil.log(LOG, logutil.INFO, "%s::Total Number of open DB connections: %s" % (self.t_name, s)) 
        
    def send_html_email(self, messages, recipients, err_msg):
        """ Sends user alerts via email messages.
        """
        uploaded_file_ids = [each_msg.keys()[0] for each_msg in messages]

        logutil.log(LOG, logutil.INFO, "Sending File Arrival email notification for uploaded_file_ids=%s, recipients=%s" \
                                        % (str(uploaded_file_ids), str(recipients)))

        hostname = socket.gethostname()
        
        if self.parent_admin_service.test_run:
            try:
                username = os.getlogin()
            except OSError:
                # some terminal emulators do not update utmp, which is needed by getlogin()
                import pwd
                username = pwd.getpwuid(os.geteuid())[0]
                
            recipients = [username + '@castlighthealth.com']
        else:
            recipients = [each_user.strip() for each_user in recipients.split(',')]
            
        timestamp = datetime.datetime.now()
        from_email = 'claims_master_admin_service <wh_ops@castlighthealth.com>'

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
    
            for each_msg in messages:
                subject = "File Arrival - %s claims_master_admin_service: File Arrival email notification - %s" % (hostname, str(timestamp))
                subject = " ".join(subject.split())

                uploaded_file_id = each_msg.keys()[0]
                text_body = """%s""" % str(each_msg.values()[0])
                
                context_data = [{'message': text_body}]
                send_to = recipients
                
                if err_msg not in text_body:
                    sql_string = """SELECT temp.*, alspec.email_addresses 
                                        FROM (
                                            SELECT ifc.id, source_file_name, source_file_path, uf.file_type, e.name as employer, 
                                            ic.name as insurance_company, uf.source, date_received, uf.file_size
                                            FROM `import_file_config` as ifc 
                                            LEFT JOIN `uploaded_files` as uf ON ifc.`file_detection_rule`=uf.`file_detection_rule`
                                            LEFT JOIN employers as e ON e.id=uf.employer_id
                                            LEFT JOIN insurance_companies as ic ON ic.id=uf.insurance_company_id
                                            WHERE uf.id=%s) 
                                        as temp
                                        LEFT JOIN `alert_specification` as alspec ON alspec.`config_id`=temp.id
                                        AND alspec.`alert_type`='arrival'""" % (uploaded_file_id,)
                    try:
                        cursor.execute(sql_string)
                        file_data = cursor.fetchall()

                        email_addresses = []
                        for each_entry in file_data:
                            if each_entry['email_addresses'] and type(each_entry['email_addresses']) == str:
                                mail_list = each_entry['email_addresses'].split(',')
                                email_addresses.extend([mail.strip() + '@castlighthealth.com' if mail.find('@') == -1 \
                                                        else mail.strip() for mail in mail_list ])

                        if email_addresses:
                            send_to = email_addresses
                            
                        context_data[0].update({'source_path': file_data[0]['source_file_path'],
                                'source_name': file_data[0]['source_file_name'],
                                'employer': file_data[0]['employer'],
                                'insurance_company': file_data[0]['insurance_company'],
                                'file_type': file_data[0]['file_type'], 
                                'file_size': utils.filesize_formater(file_data[0]['file_size']) if file_data[0]['file_size'] \
                                             else file_data[0]['file_size'],
                                'uploaded_file_id': uploaded_file_id,
                                'date_received': file_data[0]['date_received'],
                                'source': file_data[0]['source'],
                                })
                        
                        subject = "File Arrival - %s - %s - %s" % \
                                            (get_proper_casing(file_data[0]['file_type']), get_proper_casing(file_data[0]['source']), \
                                            get_proper_casing(file_data[0]['employer']))
                        subject = " ".join(subject.split())

                    except:
                        send_to = recipients
                
                logutil.log(LOG, logutil.INFO, "File Arrival Email data prepared for uploaded_file_id: %s" % (str(uploaded_file_id),))
                
                self.django_email.send_email_template('file_arrival_email', context_data, subject, send_to, from_email)

        finally:
            if conn:
                conn.close()

    def run_before_expire(self):
        print "Expire: Hello 123"
            
class SocketConnectionHandler(asyncore.dispatcher_with_send):

    SUPPORTED_COMMANDS = {'properties.refresh':'Refresh Claims Admin Service Properties',
                          'restart.all':'Restart all Management threads'}
    
    def __init__(self, sock, parent_admin_server):
        asyncore.dispatcher_with_send.__init__(self, sock)
        self.c = 0
        self.parent_admin_server = parent_admin_server
        
    def handle_read(self):
        data = self.recv(8192)
        if data:
            if data.strip()=='properties.refresh':
                self.parent_admin_server.refresh_properties()
            elif data.strip()=='restart.all': 
                self.parent_admin_server.restart_all()
            elif data.strip()=='help':
                self.__print_help()
#            elif data.strip()[0:8]=="new.file":
#                print data.strip()[9:]
#                self.parent_admin_server.new_file(data.strip()[9:])
                  
    def hello(self):
        self.c = self.c + 1
        self.send("Type 'help' for available commands, ^] to exit.\n")

    def __print_help(self):
        self.send("""Available commands are: %s
Enter ^] to exit.
""" % SocketConnectionHandler.SUPPORTED_COMMANDS)
                
class AdminServer(asyncore.dispatcher):

    def __init__(self, host, port, environment, properties_file, test_run, test_jira):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)
        self.environment = environment
        self.properties_file = properties_file
        self.test_run = test_run
        self.test_jira = test_jira
        
        if self.test_run:
            print "Running in test mode"
        self.properties = yaml.load(open(self.properties_file))
        
        #create condition object which will be used for thread sync
        ifc_data_condition = threading.Condition()
        self.ifc_data = ImportFileConfig()
        self.ifc_data.update_ifc_data(self.properties)
        
        # Start background thread for Claims
        self.claims_manager = ClaimsManager(self)
        self.claims_timer_thread = ResettableTimer(maxtime=None, expire=self.claims_manager.run_before_expire, \
                                            ifc_condition=ifc_data_condition, ifc_data=self.ifc_data, \
                                            inc=int(self.properties.get(self.environment).get('claims_manager').get('polling_interval')), \
                                            update=self.claims_manager.run, properties=self.properties.get(self.environment).get('claims_manager'), \
                                            t_name='claims_manager')
        self.claims_timer_thread.start()
        
        self.claims_file_manager = ClaimsFileManager(self)
        self.claims_file_timer_thread = ResettableTimer(maxtime=None, expire=self.claims_file_manager.run_before_expire, \
                                            ifc_condition=ifc_data_condition, ifc_data=self.ifc_data, \
                                            inc=int(self.properties.get(self.environment).get('claims_file_manager').get('polling_interval')), \
                                            update=self.claims_file_manager.run, \
                                            properties=self.properties.get(self.environment).get('claims_file_manager'), t_name='claims_file_manager')
        #self.claims_file_timer_thread.start()
        
        #Import Claims File config producer thread
        self.ifc_manager = ImportFileConfigManager(self)
        self.ifc_timer_thread = ResettableTimer(maxtime=None, expire=self.ifc_manager.run_before_expire, ifc_condition=ifc_data_condition, \
                                            ifc_data=self.ifc_data, inc=int(self.properties.get(self.environment).get('ifc_manager').get('polling_interval')), \
                                            update=self.ifc_manager.run, properties=self.properties.get(self.environment).get('ifc_manager'), \
                                            t_name='ifc_manager')
        self.ifc_timer_thread.start()

#        self.sftp_manager = SftpManager(self.environment)
#        self.sftp_timer_thread = ResettableTimer(maxtime=None, expire=self.sftp_manager.run_before_expire, inc=20, update=self.sftp_manager.run)
#        self.sftp_timer_thread.start()

    
    def refresh_properties(self):
        self.properties = yaml.load(open(self.properties_file))
        self.claims_manager.refresh_properties()
        self.claims_timer_thread.refresh_properties(self.properties.get(self.environment).get('claims_manager'))
        self.claims_file_manager.refresh_properties()
        self.claims_file_timer_thread.refresh_properties(self.properties.get(self.environment).get('claims_file_manager'))

    def restart_all(self):
        self.refresh_properties()
        self.claims_timer_thread.interrupt_thread()
        self.claims_file_timer_thread.interrupt_thread()
                
    def new_file(self, file_location):
        if os.path.exists(file_location):
            self.claims_file_manager.new_file({'source_file_name':file_location[file_location.rfind('/')+1:],
                                          'source_file_path':file_location[:file_location.rfind('/')+1]})
              
                    
    def handle_accept(self):
        pair = self.accept()
        if pair is None:
            pass
        else:
            sock, addr = pair
            s_addr = repr(addr)
            print 'Incoming connection from %s' % s_addr
            handler = SocketConnectionHandler(sock, self)

if __name__ == "__main__":
    usage="""claims_master_admin_service.py
            -e <environment> e.g. prod/preprod. This is a required option.
            [-p] <claims_master_admin_service.yml>
                e.g. contents: 
            
                environment: prod
                claims_manager:
                  capabilities: ['stage', 'load', 'idc']
                  polling_interval: 20
                  temp_directory: /tmp
            [-t] Test run"""
    
    default_properties_file_location = whcfg.providerhome + '/claims/import/common/claims_master_admin_service.yml'
            
    parser = OptionParser(usage=usage)
    parser.add_option("-e", "--environment", type="string",
                      dest="environment",
                      help="Environment name. e.g. prod/preprod. This is a required option.")    
    parser.add_option("-p", "--properties_file", type="string",
                      dest="properties_file",
                      help = """e.g. contents: 
                environment: prod
                claims_manager:
                  capabilities: ['stage', 'load', 'idc']
                  polling_interval: 20
                  temp_directory: /tmp""",
                      default = default_properties_file_location)
    parser.add_option("-t", "--test_run",
                      action="store_true",
                      dest="test_run",
                      default=False,
                      help="Run in test mode")
    parser.add_option("-j", "--test_jira",
                      action="store_false",
                      dest="test_jira",
                      default=True,
                      help="This will create real Jira Tickets.")

    (claims_admin_options, args) = parser.parse_args()
    if not claims_admin_options.environment:
        print usage
        sys.exit(0)
    logutil.log(LOG, logutil.INFO, "Claims Master Admin Service properties file location: %s" % claims_admin_options.properties_file)
    
    server = AdminServer('localhost', 8081, claims_admin_options.environment, claims_admin_options.properties_file, \
                                        claims_admin_options.test_run, claims_admin_options.test_jira)   
    asyncore.loop()

#if __name__ == "__main__":
#    claims_manager = ClaimsManager('prod')
#    claims_manager.update_google_spreadsheet()

