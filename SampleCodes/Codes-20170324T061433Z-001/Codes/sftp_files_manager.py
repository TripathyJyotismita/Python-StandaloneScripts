"""
Date: March 25, 2013
author: mmane

Monitors configured SFTP sites(and directories) and performs upload/download operations.
"""

import os, sys, re

#Patch to use different Paramiko libs
PARAMIKO_LIB_NAME = 'paramiko-1.7.7.1-py2.7.egg'
sys.path = [e_p for e_p in sys.path if PARAMIKO_LIB_NAME not in e_p]
sys.path.append('/home/whops/python-extra-libs')
##print '\npythonpath', sys.path

import shutil
import datetime
import yaml
import traceback
import stat
import socket
import argparse
import paramiko
import gnupg
import errno

import whcfg
from dbutils import getDBConnection
from jira_rest import JiraRest
from django_email import DjangoEmail
from cleanse_utils import get_proper_casing

import logutil
import urllib2

#initialize logger
SFTP_LOGGER_NAME = 'sftp_files_manager'
LOG = logutil.initlog(SFTP_LOGGER_NAME)

#configure DJango template path to use in the email templates
TEMPLATE_DIR = '%s/claims/import/common/templates' % whcfg.providerhome
DJANGO_EMAIL = DjangoEmail(TEMPLATE_DIR, logutil, LOG)


#Default values
#default values are override by property file configuration

GNUPG_HOME_DIR = '~/.gnupg'
FILES_EXCEPTIONS = '.bashrc, .sftp, .ssh, .profile, .vimrc, .bash_logout, .cache'
IS_PROCESS_HIDDEN_FILES = True

USER_NOTIFICATION_EMAILS = 'mmane@castlighthealth.com'
SCRIPT_FAILURE_NOTIFICATION_EMAILS = 'mmane@castlighthealth.com'

SUPPORTED_CONNECTION_PROTOCOL = ['sftp', 'http']
ENABLED_CONNECTION_PROTOCOL = ['sftp', 'http']
FILE_CHUNK_SIZE = 2621440
DATE_ABBR_LIST = {'mm': '%m', 'yy': '%y', 'dd': '%d', 'month': '%B', 'yyyy': '%Y', 'mon': '%b'}

#Property file having different configuration
DEFAULT_PROPERTY_FILE = whcfg.providerhome + '/claims/import/common/claims_master_admin_service.yml'

LOGGED_USERNAME = 'wh_ops'


def get_logged_username():
    """ Updates LOGGED_USERNAME variable
    """
    global LOGGED_USERNAME
    try:
        LOGGED_USERNAME = os.getlogin()
    except OSError:
        # some terminal emulators do not update utmp, which is needed by getlogin()
        import pwd
        LOGGED_USERNAME = pwd.getpwuid(os.geteuid())[0]
    except:
        LOGGED_USERNAME = 'wh_ops'
    
    if LOGGED_USERNAME == 'whops':
        LOGGED_USERNAME = 'wh_ops'
        
#    LOGGED_USERNAME = 'vshah, mmane'
    return LOGGED_USERNAME

def _send_generic_email(subject, body, recipients, is_test):
    """ Sends text email with given body to configured email ids. If is_test is set then sends email to current running user. 
        Sender will be current running user.
    """
    if is_test:
        recipients = [each_user.strip() + '@castlighthealth.com' for each_user in LOGGED_USERNAME.split(',')]
    else:
        recipients = [each_user.strip() for each_user in recipients.split(',')]

    logutil.log(LOG, logutil.INFO, "Sending email to %s" % (recipients,))

#    hostname = socket.gethostname()

    subject = '%s-%s' % (subject, str(datetime.date.today()))
    sender = 'SFTP_files_manager <%s@castlighthealth.com>' % (LOGGED_USERNAME,)

    DJANGO_EMAIL.send_email(body, subject, recipients, sender)


#TODO: 
#1.Handle connection time out and other connection errors
#2.SFTP List dir command with filter options 

class SFTPConnection(object):
    """Connects and logs into the specified hostname. Returns a connection to the requested machine.
    """ 

    def __init__(self, host, username, password = None, port = 22, private_key = None, private_key_pass = None,
                 logger_name = False, keep_alive = 1):
        self._sftp_live = False
        self._transport_live = False
        self._sftp = None
        self._transport = None
        if keep_alive == 0:
            self._keep_alive = False
        else:
            self._keep_alive = True

        if logger_name:
            paramiko.util.log_to_file(logger_name)
        else:
            paramiko.util.log_to_file(SFTP_LOGGER_NAME)

        # Begin the SSH transport.
        self._transport = paramiko.Transport((host, int(port)))

        # Authenticate the transport
        pkey = None
        if password != None and password.strip() == '':
            password = None
        if private_key != None and private_key.strip() == '':
            private_key = None
        if private_key:
            private_key_file = os.path.expanduser(private_key)
            pkey = paramiko.RSAKey.from_private_key_file(private_key_file, private_key_pass)
        elif not password:
            raise TypeError, "You have not specified a password or key."
        
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.pkey = pkey
        self._transport.connect(username = self.username, password = self.password, pkey = self.pkey)
        self._transport_live = True

    def _refresh_transport(self):
        """ Refreshes Transport connection object.
        """
        if not self._transport_live:
            self._transport = paramiko.Transport((self.host, int(self.port)))
            self._transport.connect(username = self.username, password = self.password, pkey = self.pkey)
            self._transport_live = True
            self._sftp_live = False
        elif not self._transport.is_alive() or not self._transport.is_active():
            self._transport = paramiko.Transport((self.host, int(self.port)))
            self._transport.connect(username = self.username, password = self.password, pkey = self.pkey)
            self._transport_live = True
            self._sftp_live = False

        return self._transport

    def get_sftp_client(self):
        """ Connections to SFTP server and returns SFTP client.
        """
        self._refresh_transport()
        
        if not self._sftp_live:
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._sftp_live = True
        return self._sftp
        
    def close(self):
        """ Closes the connection and cleans up
        """
        # Close SFTP Connection.
        if self._sftp_live:
            self._sftp.close()
            self._sftp_live = False

        # Close the SSH Transport.
        if self._transport_live:
            self._transport.close()
            self._transport_live = False

    def cleanup(self):
        """ Closes _sftp and _transport if keep_alive is set to false
        """
        if self._keep_alive == False:
            self.close()

    def __del__(self):
        """ Attempt to clean up if not explicitly closed
        """
        self.close()


class SFTP_Files_Manager(object):
    """ Monitors configured SFTP sites and directories in the SFTP_file_config table. 
        Performs the upload/download operations and updates sftp_files table.
    """
    def __init__(self, properties_file_location, is_test, is_jira):
        """ 
            @param properties_file_location: file path with name of properties/configuration file. 
        """
        #get default configuration file
        logutil.log(LOG, logutil.INFO, "Started SFTP Files Manager")

        logutil.log(LOG, logutil.INFO, "START: Initializing SFTP Files Manager")

        self.properties_file_location = properties_file_location

        global SCRIPT_FAILURE_NOTIFICATION_EMAILS
        self.script_failure_notification_emails = SCRIPT_FAILURE_NOTIFICATION_EMAILS

        global USER_NOTIFICATION_EMAILS
        self.user_notification_emails = USER_NOTIFICATION_EMAILS

        #set test and jira mode
        self.is_test = is_test
        self.is_jira = is_jira
        
        #read property file
        logutil.log(LOG, logutil.INFO, "START: Reading property configuration file, location: %s" % self.properties_file_location)
        try:
            self.properties = yaml.load(open(self.properties_file_location))
        except Exception as error:
            error_message = str(traceback.format_exc())
            logutil.log(LOG, logutil.CRITICAL, "Error in reading property configuration file: %s" % (error,))
            logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
            logutil.log(LOG, logutil.CRITICAL, error_message)
            
            subject = "[SFTP Files Monitor] Property configuration file Error"
            body = 'Error in reading property configuration file: %s \n%s' % (self.properties_file_location, error_message)
            _send_generic_email(subject, body, self.script_failure_notification_emails, self.is_test)
            
            logutil.log(LOG, logutil.CRITICAL, 'SFTP Files Manager stopped')
            sys.exit(0)

        #read configuration from property file or use default values
        self.file_exceptions = [each_file_name.strip() for each_file_name in (self.properties.get('prod')\
                                                    .get('sftp_file_manager').get('file_exceptions', FILES_EXCEPTIONS)).split(',')]
        
#        self.file_substring_exceptions = [each_file_name.strip() for each_file_name in (self.properties.get('prod')\
#                                                    .get('sftp_file_manager').get('file_substring_exceptions')).split(',')]

        self.is_process_hidden_files = self.properties.get('prod').get('sftp_file_manager').get('is_process_hidden_files', IS_PROCESS_HIDDEN_FILES)
        
        self.enabled_connection_protocol = [each_protocol_name.strip().lower() for each_protocol_name in (self.properties.get('prod')\
                                                    .get('sftp_file_manager').get('enabled_connection_protocol', ENABLED_CONNECTION_PROTOCOL)).split(',')]

        self.script_failure_notification_emails = self.properties.get('prod').get('sftp_file_manager')\
                                                    .get('dev_notification_emails', SCRIPT_FAILURE_NOTIFICATION_EMAILS)

        SCRIPT_FAILURE_NOTIFICATION_EMAILS = self.script_failure_notification_emails
        
        self.user_notification_emails = self.properties.get('prod').get('sftp_file_manager')\
                                                    .get('user_notification_emails', USER_NOTIFICATION_EMAILS)

        USER_NOTIFICATION_EMAILS = self.user_notification_emails
        
        logutil.log(LOG, logutil.INFO, "Configuration are :\n file_exceptions=%s \
                                                    \n user_notification_emails=%s \n script_failure_notification_emails=%s" % \
                                                    (self.file_exceptions, 
                                                    self.user_notification_emails, self.script_failure_notification_emails))
        logutil.log(LOG, logutil.INFO, "END: Completed reading property configuration file")

        #create DB connection
        self.claims_master_conn = getDBConnection(
                                                    dbname = self.properties.get('prod').get('sftp_file_manager').get('dbschema'),
                                                    host = self.properties.get('config').get('admin_server_dbhost'),
                                                    user = self.properties.get('config').get('admin_server_dbuser'),
                                                    passwd = self.properties.get('config').get('admin_server_dbpassword'),
                                                    useDictCursor = True)  
        
        self.claims_master_cursor = self.claims_master_conn.cursor()
    
        self.gpg = gnupg.GPG(gnupghome = GNUPG_HOME_DIR)
        
        self.jira_rest = JiraRest(self.properties.get('JIRA').get('jira_server'), \
                                self.properties.get('JIRA').get('jira_user'), \
                                self.properties.get('JIRA').get('jira_password'))
        
        logutil.log(LOG, logutil.INFO, "END: Completed Initialization of SFTP Files Manager")

    def get_now_time(self):
        timevalue = datetime.datetime.now()
        now = timevalue.isoformat(' ').split('.')[0]
        return now

    def notify_users(self, subject, all_sftp_files, is_multiple_email=True):
        """ Sends user alerts via email messages 
        """
        sftp_file_ids = [each_file['file_id'] for each_file in all_sftp_files]
    
        logutil.log(LOG, logutil.INFO, "Sending email notification for sftp_file_ids=%s, recipients=%s" % \
                                                        (str(sftp_file_ids), str(self.user_notification_emails)))
        
        if self.is_test:
            recipients = [each_user.strip() + '@castlighthealth.com' for each_user in LOGGED_USERNAME.split(',')]
        else:
            recipients = [each_user.strip() for each_user in self.user_notification_emails.split(',')]
            
        timestamp = datetime.datetime.now()
        
        subject = '%s-%s' % (subject, str(datetime.date.today()))
        subject = " ".join(subject.split())
        
        from_email = 'SFTP_files_manager <%s@castlighthealth.com>' % (LOGGED_USERNAME,)
        template_name = 'sftp_file_monitor'
        
        context_data = []
        for each_file in all_sftp_files:
            context_data.append(each_file)
            
            if is_multiple_email:
                logutil.log(LOG, logutil.INFO, "Email data prepared for sftp_file_id: %s" % (str(each_file['file_id']),))
                
                DJANGO_EMAIL.send_email_template(template_name, context_data, subject, recipients, from_email)
                context_data = []

        if not is_multiple_email:
            logutil.log(LOG, logutil.INFO, "Email data prepared for sftp_file_ids: %s" % (str(sftp_file_ids),))
            
            DJANGO_EMAIL.send_email_template(template_name, context_data, subject, recipients, from_email)

    def notify_stakeholders(self, subject, all_sftp_files):
        """ Sends download/upload alerts to respective stakeholders via email messages
        """
        subject = '%s-%s' % (subject, str(datetime.date.today()))
        subject = " ".join(subject.split())
        
        from_email = 'SFTP_files_manager <%s@castlighthealth.com>' % (LOGGED_USERNAME,)
        template_name = 'sftp_file_monitor'
        
        for each_file in all_sftp_files:
            if each_file.get('email_notification_id'):
                logutil.log(LOG, logutil.INFO, "Email data prepared for sftp_file_id: %s" % (str(each_file['file_id']),))
                if self.is_test:
                    recipients = [each_user.strip() + '@castlighthealth.com' for each_user in LOGGED_USERNAME.split(',')]
                else:
                    recipients = [each_user.strip() for each_user in each_file.get("email_notification_id").split(',')]
                DJANGO_EMAIL.send_email_template(template_name, [each_file], subject, recipients, from_email)

    def notify_users_no_files(self, sftp_file_config):
        """ Sends No file available to process alerts via email messages 
        """
        if self.is_test:
            recipients = [each_user.strip() + '@castlighthealth.com' for each_user in LOGGED_USERNAME.split(',')]
        else:
            recipients = [each_user.strip() for each_user in self.user_notification_emails.split(',')]
        
        subject = '[SFTP Files Monitor] No File present to process-%s' % (str(datetime.date.today()))
        subject = " ".join(subject.split())
        
        from_email = 'SFTP_files_manager <%s@castlighthealth.com>' % (LOGGED_USERNAME,)
        template_name = 'sftp_monitor_no_file_found'
        
        for each_config in sftp_file_config:
            all_recipients = recipients
            if not self.is_test and each_config.get("email_notification_id"):
                all_recipients = [each_user.strip() for each_user in each_config.get("email_notification_id").split(',')]
            logutil.log(LOG, logutil.INFO, "No file available to process For SFTP Config ID:%s Sending email to %s" \
                        % (each_config.get("id"), all_recipients))
            DJANGO_EMAIL.send_email_template(template_name, each_config, subject, all_recipients, from_email)

    def is_no_file_available_email(self, sftp_config):
        """ Returns true if No File Available Email configuration is enabled and 
            for full_day activities file is not processed today.
        """
        return sftp_config['no_file_available_email'] == 1 and not (sftp_config['activity_frequency'].lower() == "full_day"
                                                                    and sftp_config['last_process_date'] == datetime.date.today())

    def get_jira_session_and_params(self):
        """ Gets Jira server settings for property file and create jira session.
            @param is_jira: this is test mode and will not create real Jira Tickets.
        """
        jira_session = self.jira_rest.get_session()
        
        #Jira issue data
        jira_timetracking = self.properties.get('JIRA').get('jira_timetracking')
        
        jira_timetracking_details = {}

        if jira_timetracking:
            detail_time_data = re.match(r'(\d+)d (\d+)h (\d+)m', jira_timetracking)

            if detail_time_data:
                detail_time_data = detail_time_data.groups()
                jira_timetracking_details.update({'days': int(detail_time_data[0]), \
                                        'hours': int(detail_time_data[1]), \
                                        'mins': int(detail_time_data[2])})
            
        #prepare list of watchers
        watchers = self.properties.get('JIRA').get('watchers')
        if watchers:
            watchers = [each_user.strip() for each_user in watchers.split(',')]

        jira_params = {'jira': jira_session,
                        'is_test': self.is_jira,
                        'jira_project': self.properties.get('prod').get('sftp_file_manager').get('sftp_jira_project'), 
                        'jira_issuetype': self.properties.get('prod').get('sftp_file_manager').get('sftp_jira_issuetype'), 
                        'jira_assignee': self.properties.get('prod').get('sftp_file_manager').get('sftp_jira_assignee'),
                        'jira_timetracking': jira_timetracking,
                        'components': self.properties.get('prod').get('sftp_file_manager').get('sftp_jira_components'),
                        'watchers': watchers,
                        'priority': self.properties.get('prod').get('sftp_file_manager').get('sftp_jira_priority'),
                        'severity_value': self.properties.get('JIRA').get('severity_value'),
                        }
        
        if jira_params['jira_project'] != 'DOPS':
            jira_params.update({'environment_value': self.properties.get('JIRA').get('environment_value') })

        logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(jira_params,)))
        
        return jira_params, jira_timetracking_details

    def _create_jira_update_db(self, all_files_data, jira_params, jira_timetracking_details, is_jira_duedate = True):
        """ Creates JIRA ticket for each files and updates corresponding entry in the DB.
        """
        column_names = ['file_id', 'file_name', 'file_path', 'file_datetime', 'status', 'source', \
                                'sftp_files_config_id', 'site', 'port', 'username']
        incentive_column_names = ['file_name', 'file_path', 'file_datetime', 'source'] 
        query_str = """        SELECT sf.id as file_id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, jira_ticket,
                                      `status`, `process_datetime`, `site`, `port`, `connection_protocol`, `username`,
                                      `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `day_to_process`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE file_name = %s AND file_path = %s 
                                      AND file_datetime = %s AND sfc.id = %s
                                      AND sf.sftp_files_config_id = sfc.id 
                                      AND sf.jira_ticket IS NULL
                    """
        incentive_jira_params, incentive_jira_timetracking_details = self.get_jira_session_and_params()
        incentive_jira_params['jira_project'] = self.properties.get('prod').get('sftp_file_manager').get('incentive_jira_project')
        incentive_jira_params['components'] = self.properties.get('prod').get('sftp_file_manager').get('incentive_jira_component')
        incentive_jira_params['jira_issuetype'] = self.properties.get('prod').get('sftp_file_manager').get('incentive_jira_issuetype')
        incentive_jira_params['jira_assignee'] = self.properties.get('prod').get('sftp_file_manager').get('incentive_jira_assignee')
        incentive_jira_params['priority'] = self.properties.get('prod').get('sftp_file_manager').get('incentive_jira_priority')
        for each_file in all_files_data:
            
            self.claims_master_cursor.execute(query_str, (each_file['file_name'], each_file['file_path'], \
                                                                each_file['file_datetime'], each_file['sftp_files_config_id']))
            sftp_file_data = self.claims_master_cursor.fetchall()
            
            if not sftp_file_data or len(sftp_file_data) > 1:
                continue
            
            sftp_file_data = sftp_file_data[0]
            
            jira_duedate = None
            if jira_params['jira_timetracking'] and is_jira_duedate:
                incentive_jira_params['jira_duedate'] = jira_params['jira_duedate'] = (datetime.timedelta(days = jira_timetracking_details['days'], \
                                              hours = jira_timetracking_details['hours'], \
                                              minutes = jira_timetracking_details['mins']) + datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            if each_file.get('description') and each_file['description'].lower() == 'incentive':
                incentive_time_period_end = sftp_file_data['file_datetime']-datetime.timedelta(days=1)
                incentive_time_period_end = incentive_time_period_end.date()
                incentive_time_period_start =  None
                if sftp_file_data['day_to_process']:
                    incentive_time_period_start = datetime.date(sftp_file_data['file_datetime'].year -1 if sftp_file_data['file_datetime'].month == 1 else\
                                                  sftp_file_data['file_datetime'].year ,12 if sftp_file_data['file_datetime'].month == 1 else sftp_file_data['file_datetime'].month-1,sftp_file_data['file_datetime'].day)
                elif sftp_file_data['frequency']:
                    incentive_time_period_start = sftp_file_data['file_datetime']-datetime.timedelta(days=sftp_file_data['frequency'])
                    incentive_time_period_start = incentive_time_period_start.date()
                summary = "Create and upload incentive file - %s - %s" % (sftp_file_data['source'], each_file['file_name'])
                description = "h5. File details are:\n|" + '|\n|'.join(\
                                            ['*' + each_column + '*|' + str(sftp_file_data[each_column]) for each_column in incentive_column_names]) + '|'
                description = description + "\n|*Period*|%s to %s|" %(incentive_time_period_start,incentive_time_period_end)
                incentive_jira_params.update({'summary': summary, 'description': description})
                logutil.log(LOG, logutil.INFO, '\n')
                logutil.log(LOG, logutil.WARNING, "Creating JIRA ticket for STFP file_name=%s, file_id=%s" % \
                                                        (sftp_file_data['file_name'], sftp_file_data['file_id']))
                logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(incentive_jira_params,)))
                jira_ticket = self.jira_rest.create_jira_issue(**incentive_jira_params)
                logutil.log(LOG, logutil.WARNING, "JIRA ticket '%s' created for %s%s file" % (jira_ticket.key, \
                                                        sftp_file_data['file_path'], sftp_file_data['file_name']))
            else:
                summary = "SFTP File: %s file name - %s activity" % (each_file['file_name'], sftp_file_data['activity'])
                description = "h5. File details are:\n|" + '|\n|'.join(\
                                            ['*' + each_column + '*|' + str(sftp_file_data[each_column]) for each_column in column_names]) + '|'
                jira_params.update({'summary': summary, 'description': description})
    
                logutil.log(LOG, logutil.INFO, '\n')
                logutil.log(LOG, logutil.WARNING, "Creating JIRA ticket for STFP file_name=%s, file_id=%s" % \
                                                        (sftp_file_data['file_name'], sftp_file_data['file_id']))
                logutil.log(LOG, logutil.INFO, "JIRA parameters are\n%s" % (str(jira_params,)))
    
                jira_ticket = self.jira_rest.create_jira_issue(**jira_params)
            
                logutil.log(LOG, logutil.WARNING, "JIRA ticket '%s' created for %s%s file" % (jira_ticket.key, \
                                                        sftp_file_data['file_path'], sftp_file_data['file_name']))
            
            update_query = """    UPDATE sftp_files 
                                     SET jira_ticket = %s
                                   WHERE id=%s
                           """

            self.claims_master_cursor.execute(update_query, (jira_ticket.key, sftp_file_data['file_id']))

    def _resolve_jira(self, jira_key, jira_params, comment, root_cause = " "):
        """ Resolves given JIRA ticket.
        """
        if not jira_key or jira_key == 'NA' or self.is_jira:
            return
        
        resolving_options = None
        try:
            issue = jira_params['jira'].issue(jira_key)
            if issue.fields.project.key == 'DOPS':
                # root_cause field is mandatory while resolving DOPS tickets
                # customfield_12090: root_cause
                resolving_options = {'customfield_12090': root_cause}
            transitions = jira_params['jira'].transitions(issue)
            
            if '5' in [t['id'] for t in transitions] or 'Resolve Issue' in [t['name'] for t in transitions]:
                self._comment_jira(jira_key, jira_params, comment)
                jira_params['jira'].transition_issue(issue, '5', fields = resolving_options)
        except:
            pass
        
    def _comment_jira(self, jira_key, jira_params, comment):
        """ Updates given JIRA ticket with the given comment.
        """
        if not jira_key or jira_key == 'NA' or self.is_jira:
            return
        
        try:
            jira_params['jira'].add_comment(jira_key, comment)
        except:
            pass
    
    def _get_activities(self):
        """ This method determines the site to which needs to login and perform the configured activity.
            It uses 'sftp_files_config' table to detect these activities. 
        """
        logutil.log(LOG, logutil.INFO, '\n')
        logutil.log(LOG, logutil.INFO, "START: Determine Activities - Checking SFTP files config")

        #get sftp files config to determine the activities which needs to be perform
        query_str = """SELECT sfc.* FROM(SELECT id, `site`, `port`, `connection_protocol`, `username`, `password`,
                                                `file_key`, `cryptography_type`, `activity`, `expected_date`, `frequency`, `source`, `file_name_format`, `day_to_process`,
                                                `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `description`, `activity_frequency`,
                                                `auth_key_file`, `use_current_month`, `login_url`, `email_notification_id`, `no_file_available_email`,
                                                `keep_alive`
                                           FROM sftp_files_config 
                                          WHERE is_active = 1 AND ((expected_date <= NOW() AND MOD(DATEDIFF(expected_date, NOW()), frequency) = 0)
                                             OR DATE_ADD(last_process_date, INTERVAL frequency + 
                                                MOD(DATEDIFF(expected_date, last_process_date), frequency) DAY) <= NOW()
                                             OR (expected_date <= NOW() AND (last_process_date IS NULL OR last_process_date = '0000-00-00'))
                                             OR (day_to_process IS NOT NULL AND extract(DAY FROM now())-day_to_process =0
                                            AND extract(MONTH FROM now())-extract(MONTH FROM last_process_date) != 0)
                                             OR (day_to_process IS NOT NULL AND 
                                                 ((11 mod abs(extract(MONTH FROM now())-extract(MONTH FROM last_process_date)) = 0
                                                AND extract(DAY FROM now()) > day_to_process)
                                                 OR (11 mod abs(extract(MONTH FROM now())-extract(MONTH FROM last_process_date)) > 0))
                                            )
                                             OR (day_to_process IS NOT NULL AND extract(DAY FROM now())-day_to_process =0 AND last_process_date IS NULL))
                                        ) AS sfc LEFT JOIN `sftp_files` AS sf ON (sfc.`id`=sf.`sftp_files_config_id`)
                        WHERE
                            CASE sfc.`activity_frequency` 
                                WHEN 'one_time' THEN
                                IF(DATE(sf.`process_datetime`) = CURRENT_DATE()
                                OR DATE(sfc.`last_process_date`) = CURRENT_DATE()
                                , FALSE, TRUE)
                            ELSE TRUE
                            END
                        GROUP BY sfc.`id`
                    """
#                           OR frequency is NULL
#                    """
        
        logutil.log(LOG, logutil.INFO, "SQL to determine the SFTP activities which needs to be perform is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        self.sftp_file_configs = self.claims_master_cursor.fetchall()
        
        logutil.log(LOG, logutil.INFO, "SFTP activities which needs to be perform are\n%s" % (str(\
                                                    [{'id': each_conf['id'], 'site': each_conf['site']} for each_conf in self.sftp_file_configs],)))
        logutil.log(LOG, logutil.INFO, "END: Determine Activities - Checking SFTP files config")

    def extract_date_from_filename(self, filename, file_name_format):
        """ Returns date from last successfully downloaded file
        """
        filename_split = filename.split("/")
        file_name_format_split = file_name_format.split("/")
        fn = ""
        ff = ""
        for i in range(len(file_name_format_split)):
            if not any([abbr.upper() in file_name_format_split[i] for abbr in DATE_ABBR_LIST]):
                fn = fn + filename_split[i]
                ff = ff + file_name_format_split[i]
        filename = fn
        file_name_format = ff
        is_previous_month_file = any([abbr + "-1" in file_name_format for abbr in DATE_ABBR_LIST])
        if is_previous_month_file:
            # If file_name_format contains format like dd-1, mm-1 replace them with mm, dd
            for eachkey in DATE_ABBR_LIST:
                file_name_format = file_name_format.replace(eachkey + "-1", eachkey)
        for eachkey in DATE_ABBR_LIST:
            file_name_format = file_name_format.replace(eachkey, DATE_ABBR_LIST[eachkey])
        
        last_file_date = datetime.datetime.strptime(filename, file_name_format)
        if is_previous_month_file:
            last_file_date = last_file_date + datetime.timedelta(days=1)
        return last_file_date

    def get_filename(self, file_name_format, expected_date):
        """ Returns new file name generated by replacing string in file_name_format
            by using expected_date
        """
        last_day_of_prev_month = expected_date - datetime.timedelta(days=expected_date.day)
        replacelist = []
        for eachkey in DATE_ABBR_LIST:
            replacelist.append((eachkey + "-1", last_day_of_prev_month.strftime(DATE_ABBR_LIST[eachkey])))
            replacelist.append((eachkey, expected_date.strftime(DATE_ABBR_LIST[eachkey])))
            replacelist.append((eachkey.upper() + "-1", last_day_of_prev_month.strftime(DATE_ABBR_LIST[eachkey])))
            replacelist.append((eachkey.upper(), expected_date.strftime(DATE_ABBR_LIST[eachkey])))
        
        new_file_name = file_name_format
        for each in replacelist:
            new_file_name = new_file_name.replace(each[0], each[1])
        return new_file_name

    def _scans_sites(self):
        """ Logs-in to each sites to check files/directories and then prepares list of files which needs to be download/upload.
        """
        self.connected_sites = {}
        all_files_data = []
        all_error_message = []
        no_file_found_for_configs = []
        
        logutil.log(LOG, logutil.INFO, "START: Scanning Sites- Checking SFTP files needs to be download/upload.")

        #login to each site and scan each directory
        for each_site_data in self.sftp_file_configs:
            if each_site_data['connection_protocol'].lower() not in SUPPORTED_CONNECTION_PROTOCOL:
                logutil.log(LOG, logutil.WARNING, "Unknown Connection Protocol configured, sftp_config_id=%s" % (each_site_data['id'],))
                continue

            if each_site_data['connection_protocol'].lower() not in self.enabled_connection_protocol:
                logutil.log(LOG, logutil.WARNING, "%s Connection Protocol is disabled" % (each_site_data['connection_protocol'],))
                continue

            if each_site_data['connection_protocol'].lower() == 'sftp':
                #login and get sftp connection client
                #check if we have already SFTP connection
                logutil.log(LOG, logutil.INFO, '')
                sftp_client = None
                try:
                    if self.connected_sites and self.connected_sites.get(each_site_data['site'] + str(each_site_data['port']) + each_site_data['username']):
                        logutil.log(LOG, logutil.INFO, "Already Connected to '%s:%s' site, using '%s' Protocol" % \
                                                            (each_site_data['site'], each_site_data['port'], each_site_data['connection_protocol'],))
                        sftp_con = self.connected_sites.get(each_site_data['site'] + str(each_site_data['port']) + each_site_data['username'])
                    else:
                        logutil.log(LOG, logutil.INFO, "Connecting to '%s:%s' site, using '%s' Protocol" % \
                                                            (each_site_data['site'], each_site_data['port'], each_site_data['connection_protocol'],))
                        sftp_con = SFTPConnection(each_site_data['site'], each_site_data['username'], each_site_data['password'], each_site_data['port'],
                                                  each_site_data['auth_key_file'], keep_alive = each_site_data['keep_alive'])
                        self.connected_sites[each_site_data['site'] + str(each_site_data['port']) + each_site_data['username']] = sftp_con
                        
                    sftp_client = sftp_con.get_sftp_client()
                except Exception as error:
                    error_traceback = str(traceback.format_exc())
                    error_message = "Connecting to '%s:%s' site failed. Username is '%s', sftp_files_config_id=%s. Skipping it..." \
                                                                % (each_site_data['site'], each_site_data['port'], \
                                                                each_site_data['username'], each_site_data['id'])
                    all_error_message.append(error_message + '\n' + error_traceback)
                    
                    logutil.log(LOG, logutil.CRITICAL, error_message)
                    logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                    logutil.log(LOG, logutil.CRITICAL, error_traceback)
    
                    continue
    
                if each_site_data['activity'].lower() == 'download':
                    #get list of sftp directory
                    dir_data = None
                    try:
                        sftp_client.stat(each_site_data['remote_dir'])
                        dir_data = sftp_client.listdir(each_site_data['remote_dir'])
                    except (OSError, IOError) as error:
                        error_traceback = str(traceback.format_exc())
                        error_message = "Directory %s is not present OR is inaccessible. sftp_files_config_id=%s. Skipping it..." \
                                                                % (each_site_data['remote_dir'], each_site_data['id'])
                        all_error_message.append(error_message + '\n' + error_traceback)
                        
                        logutil.log(LOG, logutil.CRITICAL, error_message)
                        logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                        logutil.log(LOG, logutil.CRITICAL, error_traceback)
                        sftp_con.cleanup()
                        continue
                    
                    if not dir_data:
                        logutil.log(LOG, logutil.WARNING, "Remote directory %s is empty. Nothing to be download." % (each_site_data['remote_dir'],))
                        sftp_con.cleanup()
                        if self.is_no_file_available_email(each_site_data):
                            # Add config to notification list only if notification is enabled
                            logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                            no_file_found_for_configs.append(each_site_data)
                        continue
                    
                    query_str = """        SELECT `id`, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, 
                                                  `status`, `process_datetime`
                                             FROM sftp_files 
                                            WHERE file_name = %s AND file_path = %s AND file_datetime = %s AND activity = %s
                                """
    
                    logutil.log(LOG, logutil.INFO, "SQL to match SFTP files which are already downloaded... \n%s" % (query_str,))
                    
                    file_found_to_process = False
                    for each_dir_item in dir_data:
                        #get SFTP file/dir stats
                        file_full_path = each_site_data['remote_dir'] + '/' + each_dir_item
                        try:
                            each_item_stat = sftp_client.stat(file_full_path)
                        except (OSError, IOError) as error:
                            error_traceback = str(traceback.format_exc())
                            error_message = "Stats error for %s file. Might be inaccessible. sftp_files_config_id=%s. Skipping it." \
                                                                % (file_full_path, each_site_data['id'])
                            all_error_message.append(error_message + '\n' + error_traceback)
                            
                            logutil.log(LOG, logutil.CRITICAL, error_message)
                            logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                            logutil.log(LOG, logutil.CRITICAL, error_traceback)
    
                            continue
                        
                        #check each_dir_item is a directory or not
                        if not stat.S_ISDIR(each_item_stat.st_mode):
                            #check 1: Filter out configured File Exceptions. And also Hidden files if is_process_hidden_files is disabled 
                            if not self.is_process_hidden_files and each_dir_item.startswith('.'):
                                logutil.log(LOG, logutil.INFO, "Hidden File %s Detected. is_process_hidden_files is disabled... Skipping it." \
                                                                % (file_full_path,))
                                continue
    
                            if self.file_exceptions and each_dir_item in self.file_exceptions:
                                logutil.log(LOG, logutil.INFO, "File %s in the File Exception list... Skipping it." % (file_full_path,))
                                continue
    
                            #check 2: determine file which needs to be download as per the file_detection_rule.
                            # file_detection_rule is None/NULL or '*' then proceed.
                            # If it is other value and not matching with file name then skip file. Do substring macthing.
                            if each_site_data['file_detection_rule'] and each_site_data['file_detection_rule'].strip() != '*' and \
                            each_site_data['file_detection_rule'].strip().lower() not in each_dir_item.lower():
                                logutil.log(LOG, logutil.INFO, "File %s is not matching as per the file_detection_rule %s. Skipping it." \
                                                                % (each_dir_item, each_site_data['file_detection_rule']))
                                continue
                            
                            #check 3: is file downloaded or not? - verify filename, dir, st_mtime and activity fields
                            #get matching sftp files from sftp_files table
                            file_mtime = datetime.datetime.fromtimestamp(each_item_stat.st_mtime)
                            
                            self.claims_master_cursor.execute(query_str, (each_dir_item, each_site_data['remote_dir'], \
                                                                file_mtime, each_site_data['activity']))
                            sftp_files = self.claims_master_cursor.fetchall()
                            
                            if len(sftp_files):
                                logutil.log(LOG, logutil.INFO, "File %s is already in the sftp_files table. Skipping it." % (each_dir_item,))
                                continue
                            
                            file_data = {'sftp_files_config_id': each_site_data['id'], 'process_datetime': self.get_now_time(),
                                                                'file_path': each_site_data['remote_dir'], 'file_name': each_dir_item,
                                                                'status': 'needs-to-download', 'activity': each_site_data['activity'].lower(), 
                                                                'file_datetime': file_mtime}
                            
                            #file is not downloaded, try to download it
                            all_files_data.append(file_data)
                            file_found_to_process = True
                        else:
                            logutil.log(LOG, logutil.INFO, "'%s' is a directory, skipping from download." % (each_dir_item,))
                    if not file_found_to_process and self.is_no_file_available_email(each_site_data):
                            # Add config to notification list only if notification is enabled:
                            logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                            no_file_found_for_configs.append(each_site_data)
                    sftp_con.cleanup()
                elif each_site_data['activity'].lower() == 'upload':
                    file_list = None
                    if os.path.isdir(each_site_data['local_dir']):
                        file_list = [ f for f in os.listdir(each_site_data['local_dir']) if os.path.isfile(os.path.join(each_site_data['local_dir'],f)) ]
                    else:
                        logutil.log(LOG, logutil.INFO, "The configured local directory path %s is not valid directory." % (each_site_data['local_dir'],))
                        sftp_con.cleanup()
                        continue
                    if not file_list:
                        logutil.log(LOG, logutil.INFO, "There are no files to upload in the directory: %s" % (each_site_data['local_dir'],))
                        sftp_con.cleanup()
                        if self.is_no_file_available_email(each_site_data):
                            # Add config to notification list only if notification is enabled
                            logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                            no_file_found_for_configs.append(each_site_data)
                        continue
                    
                    query_str = """        SELECT `id`, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, 
                                                  `status`, `process_datetime`
                                             FROM sftp_files 
                                            WHERE file_name = %s AND file_path = %s AND file_datetime = %s AND activity = %s
                                """
                    file_found_to_process = False
                    for file_name in file_list:
                        file_full_path = os.path.join(each_site_data['local_dir'], file_name)
                        file_stat = os.stat(file_full_path)
                        
                        if not self.is_process_hidden_files and file_name.startswith('.'):
                            logutil.log(LOG, logutil.INFO, "Hidden File %s Detected. is_process_hidden_files is disabled... Skipping it." \
                                                                % (file_full_path,))
                            continue
    
                        if self.file_exceptions and file_name in self.file_exceptions:
                            logutil.log(LOG, logutil.INFO, "File %s in the File Exception list... Skipping it." % (file_full_path,))
                            continue
    
                        #check 2: determine file which needs to be uploaded as per the file_detection_rule.
                        # file_detection_rule is None/NULL or '*' then proceed.
                        # If it is other value and not matching with file name then skip file. Do substring macthing.
                        if each_site_data['file_detection_rule'] and each_site_data['file_detection_rule'].strip() != '*' and \
                        each_site_data['file_detection_rule'].strip().lower() not in file_name.lower():
                            logutil.log(LOG, logutil.INFO, "File %s is not matching as per the file_detection_rule %s. Skipping it." \
                                                                % (file_name, each_site_data['file_detection_rule']))
                            continue
                        
                        file_mtime = datetime.datetime.fromtimestamp(file_stat.st_mtime)
                        self.claims_master_cursor.execute(query_str, (file_name, each_site_data['local_dir'], \
                                                                file_mtime, each_site_data['activity']))
                        sftp_files = self.claims_master_cursor.fetchall()
                            
                        if len(sftp_files):
                            logutil.log(LOG, logutil.INFO, "File %s is already in the sftp_files table. Skipping it." % (file_name,))
                            continue
                            
                        file_data = {'sftp_files_config_id': each_site_data['id'], 'process_datetime': self.get_now_time(),
                                     'file_path': each_site_data['local_dir'], 'file_name': file_name,
                                     'status': 'need-to-upload', 'activity': each_site_data['activity'].lower(), 
                                     'file_datetime': file_mtime}
                        
                        if each_site_data['description']:
                            file_data['description'] = each_site_data['description']
                            
                        #file is not uploaded, try to upload it
                        all_files_data.append(file_data)
                        file_found_to_process = True
                    
                    sftp_con.cleanup()
                    if not file_found_to_process and self.is_no_file_available_email(each_site_data):
                        # Add config to notification list only if notification is enabled:
                        logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                        no_file_found_for_configs.append(each_site_data)
                else:
                    logutil.log(LOG, logutil.INFO, "Unknown '%s' activity found, skipping it." % (each_site_data['activity'],))
        
            elif each_site_data['connection_protocol'].lower() == 'http':
                #login and get http connection client
                #check if connection is ok.
                logutil.log(LOG, logutil.INFO, '')
                
                if each_site_data['activity'].lower() == 'download':
                    #generate whole url
                    full_file_name = None
                    file_mtime = None
                    file_name = None
                    expected_date = None
                    last_downloaded_file_query = """    SELECT file_name FROM sftp_files
                                                        WHERE id =
                                                        (SELECT max(sf.id) FROM sftp_files sf
                                                        WHERE sf.sftp_files_config_id = %s
                                                        AND sf.status = "download-completed")
                                                 """
                    
                    #for frequency based files
                    if each_site_data['frequency']:
                        if each_site_data['last_process_date']:
                            self.claims_master_cursor.execute(last_downloaded_file_query, (each_site_data['id'],))
                            last_downloaded_file = self.claims_master_cursor.fetchall()
                            
                            if len(last_downloaded_file) > 0:
                                #Get last successfully downloaded file name
                                file_name_template = each_site_data['file_name_format']
                                if each_site_data['file_detection_rule']:
                                    file_name_template = each_site_data['file_detection_rule'] + each_site_data['file_name_format']
                                
                                last_downloaded_file_date = self.extract_date_from_filename(last_downloaded_file[0]['file_name'], file_name_template)
                                expected_date = last_downloaded_file_date + datetime.timedelta(days=each_site_data['frequency'])
                            else:
                                #If no file is successfully download then use expected date directly
                                expected_date = each_site_data['expected_date']
                        else:
                            expected_date = each_site_data['expected_date']
                    elif each_site_data['day_to_process']:
                        #for file based on day_to_process
                        #TODO: Cases to handle
                        #      1. If a file gets skipped (i.e. A file for a month is not created) 
                        #      2. Late file arrival which causes last_process_date to be out of cycle.
                        #      Those cases will occur rarely and can be added when they occur.
                        if each_site_data['last_process_date']:
                            self.claims_master_cursor.execute(last_downloaded_file_query, (each_site_data['id'],))
                            last_downloaded_file = self.claims_master_cursor.fetchall()
                            
                            if len(last_downloaded_file) > 0:
                                #Get last successfully downloaded file name
                                file_name_template = each_site_data['file_name_format']
                                if each_site_data['file_detection_rule']:
                                    file_name_template = each_site_data['file_detection_rule'] + each_site_data['file_name_format']

                                expected_date = self.extract_date_from_filename(last_downloaded_file[0]['file_name'], file_name_template)
                                if expected_date.month == 12:
                                    expected_date = datetime.date(expected_date.year + 1, 1, each_site_data['day_to_process'])
                                else:
                                    expected_date = datetime.date(expected_date.year, expected_date.month + 1, each_site_data['day_to_process'])
                            else:
                                #If no file is successfully download then use expected date directly
                                expected_date = each_site_data['expected_date']
                        else:
                            expected_date = each_site_data['expected_date']
                    
                    #generate file name
                    file_name = self.get_filename(each_site_data['file_name_format'], expected_date)
                    if each_site_data['file_detection_rule']:
                        file_name = each_site_data['file_detection_rule'] + file_name

                    full_file_name = each_site_data['site']
                    if each_site_data['port'] > 0:
                        full_file_name = full_file_name + ":" + str(each_site_data['port'])
                    if each_site_data['remote_dir'] != '':
                        full_file_name = full_file_name + '/' + each_site_data['remote_dir']
                    full_file_path = full_file_name
                    full_file_name = full_file_name + '/' + file_name
                    
                    http_client = None
                    try:
                        if full_file_name.startswith("https") or full_file_name.startswith("HTTPS"):
                            # Use username and password to connect
                            login_url = each_site_data['site']
                            if each_site_data.get("login_url"):
                                login_url = each_site_data['login_url']
                            password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
                            password_mgr.add_password(None, login_url, each_site_data['username'], each_site_data['password'])
                            handler = urllib2.HTTPBasicAuthHandler(password_mgr)
                            opener = urllib2.build_opener(handler)
                            http_client = opener.open(full_file_name)
                        else:
                            # Try to connect directly
                            http_client = urllib2.urlopen(full_file_name)
                        
                        logutil.log(LOG, logutil.INFO, 'Connected to site %s Successfully.' % (full_file_name,))
                        file_last_modified = http_client.info().getheader('Last-Modified')
                        file_mtime = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %X %Z')
                    except Exception as error:
                        if each_site_data['use_current_month'] == 1 and (type(error) == urllib2.HTTPError and error.getcode() == 404):
                            #use current month
                            logutil.log(LOG, logutil.CRITICAL, 'Connecting to site %s Failed.' % (full_file_name,))
                            logutil.log(LOG, logutil.CRITICAL, 'Retrying using current month and year.')
                            expected_date = expected_date.replace(month=datetime.datetime.now().month,
                                                                  year=datetime.datetime.now().year)
                            #generate file name
                            file_name = self.get_filename(each_site_data['file_name_format'], expected_date)
                            if each_site_data['file_detection_rule']:
                                file_name = each_site_data['file_detection_rule'] + file_name
                            
                            full_file_name = full_file_path + '/' + file_name
                            
                            try:
                                if full_file_name.startswith("https") or full_file_name.startswith("HTTPS"):
                                    # Use username and password to connect
                                    login_url = each_site_data['site']
                                    if each_site_data.get("login_url"):
                                        login_url = each_site_data['login_url']
                                    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
                                    password_mgr.add_password(None, login_url, each_site_data['username'], each_site_data['password'])
                                    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
                                    opener = urllib2.build_opener(handler)
                                    http_client = opener.open(full_file_name)
                                else:
                                    # Try to connect directly
                                    http_client = urllib2.urlopen(full_file_name)
                                
                                logutil.log(LOG, logutil.INFO, 'Connected to site %s Successfully.' % (full_file_name,))
                                file_last_modified = http_client.info().getheader('Last-Modified')
                                file_mtime = datetime.datetime.strptime(file_last_modified, '%a, %d %b %Y %X %Z')
                            except Exception as error:
                                error_traceback = str(traceback.format_exc())
                                error_message = "Connecting to '%s' site failed. sftp_files_config_id=%s. \nFull url %s \n Skipping it..." \
                                                                        % (each_site_data['site'], each_site_data['id'], full_file_name)
                                if self.is_no_file_available_email(each_site_data):
                                    # Add config to notification list only if notification is enabled
                                    logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                                    no_file_found_for_configs.append(each_site_data)
                                else:
                                    all_error_message.append(error_message + '\n' + error_traceback)
                                 
                                logutil.log(LOG, logutil.CRITICAL, error_message)
                                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                                logutil.log(LOG, logutil.CRITICAL, error_traceback)
                                continue
                        else:
                            error_traceback = str(traceback.format_exc())
                            error_message = "Connecting to '%s' site failed. sftp_files_config_id=%s. \nFull url %s \n Skipping it..." \
                                                                        % (each_site_data['site'], each_site_data['id'], full_file_name)
                            if self.is_no_file_available_email(each_site_data):
                                # Add config to notification list only if notification is enabled
                                logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                                no_file_found_for_configs.append(each_site_data)
                            else:
                                all_error_message.append(error_message + '\n' + error_traceback)
                             
                            logutil.log(LOG, logutil.CRITICAL, error_message)
                            logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                            logutil.log(LOG, logutil.CRITICAL, error_traceback)
                            continue
                    
                    #checking if file is already downloaded or not
                    query_str = """        SELECT `id`, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, 
                                                  `status`, `process_datetime`
                                             FROM sftp_files 
                                            WHERE file_name = %s AND file_path = %s AND file_datetime = %s AND activity = %s
                                """
    
                    logutil.log(LOG, logutil.INFO, "SQL to match SFTP files which are already downloaded... \n%s" % (query_str,))
                    
                    #check: If this file exists check if file downloaded or not? - verify filename, dir, st_mtime and activity fields
                    #get matching sftp files from sftp_files table
                    self.claims_master_cursor.execute(query_str, (file_name, full_file_path, \
                                                        file_mtime, each_site_data['activity']))
                    sftp_files = self.claims_master_cursor.fetchall()
                    
                    if len(sftp_files):
                        logutil.log(LOG, logutil.INFO, "File %s is already in the sftp_files table. Skipping it." % (file_name,))
                        if self.is_no_file_available_email(each_site_data):
                            # Add config to notification list only if notification is enabled
                            logutil.log(LOG, logutil.INFO, "No File found to process for SFTP Config ID:%s. Sending email notification." \
                                        % (each_site_data['id'],))
                            no_file_found_for_configs.append(each_site_data)
                        continue
                    
                    file_data = {'sftp_files_config_id': each_site_data['id'], 'process_datetime': self.get_now_time(),
                                                        'file_path': full_file_path, 'file_name': file_name,
                                                        'status': 'needs-to-download', 'activity': each_site_data['activity'].lower(), 
                                                        'file_datetime': file_mtime}
                    
                    #file is not downloaded, try to download it
                    all_files_data.append(file_data)
                    logutil.log(LOG, logutil.INFO, "File data %s." % (file_data,))
                else:
                    logutil.log(LOG, logutil.INFO, "Unknown '%s' activity found, skipping it." % (each_site_data['activity'],))
            
            else:
                logutil.log(LOG, logutil.INFO, "Unknown connection protcol '%s' found for activity id %s, skipping it." \
                            % (each_site_data['connection_protocol'], each_site_data['id']))
        
        logutil.log(LOG, logutil.INFO, "SFTP files which needs to be download/upload are\n%s" % (str(all_files_data,)))
        
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()
        
        if all_files_data:
            is_files_insertd = False
            try:
                #save data to 'sftp_files' table
                self._insert_files_data(all_files_data, True)
                is_files_insertd = True
                
                #create jira ticket and update DB.
                self._create_jira_update_db(all_files_data, jira_params, jira_timetracking_details)
            except Exception as error:
                error_traceback = str(traceback.format_exc())

                error_message = "Error during file data insertion: %s" % (error,)
                if is_files_insertd:
                    error_message = "Error during Jira ticket creation: %s" % (error,)
                    
                all_error_message.append(error_message + '\nFiles Data\n%s\n%s' % (str(all_files_data), error_traceback))
                
                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, '%s' % (str(all_files_data),))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, error_traceback)
                
        if all_error_message:
            logutil.log(LOG, logutil.CRITICAL, "Scanning Sites- Errors/Failures occured... Sending Error/Failure Email message...")
            subject = "[SFTP Files Monitor] Scanning SFTP Sites: Errors/Failures occured in dirs/files checking"
            body = "Following Errors/Failures occured in dirs/files checking " + \
                                                        "(dirs/files which needs to be process):\n\n%s" % ('\n\n'.join(all_error_message),)
            _send_generic_email(subject, body, self.script_failure_notification_emails, self.is_test)
        
        if no_file_found_for_configs:
            logutil.log(LOG, logutil.INFO, "Send email for No files found for activities")
            self.notify_users_no_files(no_file_found_for_configs)
        logutil.log(LOG, logutil.INFO, "END: Scanning Sites- Checking SFTP files needs to be download/upload.")

    def _insert_files_data(self, files_data, update_on_duplicate = False):
        """ Insert given data into 'sftp_files' tables.
            @param update_on_duplicate: Ignores duplicate key error 
        """
        logutil.log(LOG, logutil.INFO, '\n')
        logutil.log(LOG, logutil.INFO, "START: Inserting SFTP files data AND updating SFTP Files config, " + \
                                                                    "update_on_duplicate=%s" % (update_on_duplicate,))

        insert_query = """INSERT INTO sftp_files """

        column_names = files_data[0].keys()
        column_names = list(column_names)
        if "description" in column_names: column_names.remove("description")
        column_names.sort()

        column_part = " (%s) " % ", ".join(column_names)
        value_part = '%s, ' * len(column_names)
        value_part = " VALUES (" + value_part[:-2] + ") "

        complete_insert_query = insert_query + column_part + value_part

        if update_on_duplicate:
            update_query = ' ON DUPLICATE KEY UPDATE id=id'
            complete_insert_query = complete_insert_query + update_query

        all_data = []
        sftp_files_config_ids = []
        for each_row in files_data:
            row_tuple = [each_row[each_column] for each_column in column_names]
            all_data.append(row_tuple)
            sftp_files_config_ids.append((each_row['sftp_files_config_id'],))

        logutil.log(LOG, logutil.INFO, "Complete_insert_query is \n%s" % (complete_insert_query,))

        self.claims_master_cursor.executemany(complete_insert_query, all_data)
        
        update_query = """    UPDATE sftp_files_config 
                                 SET last_process_date = CURDATE()
                               WHERE id=%s
                       """

        logutil.log(LOG, logutil.INFO, "Updating SFTP Files config, sftp_files_config_ids=%s ... Update SQL is \n%s" % \
                                                    (str(sftp_files_config_ids), update_query,))

        self.claims_master_cursor.executemany(update_query, sftp_files_config_ids)

        logutil.log(LOG, logutil.INFO, "END: Inserting SFTP Files data AND updating SFTP Files config are completed")
        
    def update_message_list(self, all_message, message, extra_error_message, file_data, status, process_datetime):
        """ Updates the given list of all_message with predefined fileds.
        """
        port_no = file_data['port']
        if file_data['connection_protocol'].lower() == 'http' and file_data['port'] == 0:
            port_no = 8080
        all_message.append({'message': message, 'extra_error_message': extra_error_message, \
                                                    'file_id': file_data['id'], 
                                                    'file_name': file_data['file_name'], \
                                                    'file_path': file_data['file_path'], \
                                                    'file_datetime': file_data['file_datetime'], \
                                                    'sftp_files_config_id': file_data['sftp_files_config_id'], 
                                                    'site': file_data['site'], \
                                                    'port': port_no, \
                                                    'username': file_data['username'], \
                                                    'jira_ticket': file_data['jira_ticket'], \
                                                    'source': file_data['source'], \
                                                    'contact_person_name': file_data['contact_person_name'], \
                                                    'contact_person_email': file_data['contact_person_email'], \
                                                    'status': status, \
                                                    'process_datetime': process_datetime, \
                                                    'connection_protocol': file_data['connection_protocol'], \
                                                    'email_notification_id': file_data.get('email_notification_id'), \
                                                    })
        return all_message

    def _download_files(self):
        """ Downloads files from the given sites. It tries to download files having status 
            'needs-to-download' in sftp_file table and corresponding activity is 'download' in the sftp_files_config table.
        """
        logutil.log(LOG, logutil.INFO, "START: Downloading SFTP files")

        query_str = """SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, `password`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email, `auth_key_file`,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`,
                                      `login_url`, `keep_alive`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'needs-to-download' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'download'
                             ORDER BY site
                    """

        logutil.log(LOG, logutil.INFO, "SQL to determine the SFTP files which needs to be download is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        if not sftp_files:
            logutil.log(LOG, logutil.INFO, "Nothing to download...")
            logutil.log(LOG, logutil.INFO, "END: Downloading SFTP files completed")
            return
        logutil.log(LOG, logutil.INFO, "Total SFTP files which will be downloaded: %s" % (len(sftp_files),))
        
        update_query = """    UPDATE sftp_files 
                                 SET status = %s, process_datetime = %s, additional_status_msg = %s, target_file_name = %s, target_file_path = %s
                               WHERE id=%s
                       """
        
        all_error_message = []
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()
        
        for each_file in sftp_files:
            additional_status_msg = None
            target_file_full_path = None

            try:
                if each_file['connection_protocol'].lower() == 'sftp':
                    sftp_file_full_path = each_file['file_path'] + '/' + each_file['file_name']
                    logutil.log(LOG, logutil.INFO, "Trying to download SFTP file from %s site: %s file" % (each_file['site'], sftp_file_full_path))
                    
                    self.claims_master_cursor.execute(update_query, ('downloading', self.get_now_time(), \
                                                                additional_status_msg, None, None, each_file['id']))
    
                    if self.connected_sites.get(each_file['site'] + str(each_file['port']) + each_file['username']):
                        sftp_con = self.connected_sites[each_file['site'] + str(each_file['port']) + each_file['username']]
                    else:
                        sftp_con = SFTPConnection(each_file['site'], each_file['username'], each_file['password'], each_file['port'],
                                                  each_file['auth_key_file'], keep_alive = each_file['keep_alive'])
                        self.connected_sites[each_file['site'] + str(each_file['port']) + each_file['username']] = sftp_con
        
                    #rename old file to old_filename_file_timestamp if present
                    target_file_full_path = each_file['local_dir'] + '/' + each_file['file_name']
                    if os.path.exists(target_file_full_path):
                        shutil.move(target_file_full_path, target_file_full_path + '_%s' % \
                                                    (datetime.datetime.fromtimestamp(os.stat(target_file_full_path)[8]).strftime('%Y-%m-%d_%H:%M:%S'),))
                    
                    #download file
                    sftp_client = sftp_con.get_sftp_client()
                    sftp_client.get(sftp_file_full_path, target_file_full_path)
                    
                    logutil.log(LOG, logutil.INFO, "Downloaded successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))

                    self.claims_master_cursor.execute(update_query, ('downloaded', self.get_now_time(), \
                                                                additional_status_msg, each_file['file_name'], each_file['local_dir'], each_file['id']))
                    sftp_con.cleanup()

                elif each_file['connection_protocol'].lower() == 'http':
                    http_file_full_name = each_file['site']
                    if each_file['port'] > 0:
                        http_file_full_name = http_file_full_name + ":" + str(each_file['port'])
                    if each_file['remote_dir'] != '':
                        http_file_full_name = http_file_full_name + '/' + each_file['remote_dir']
                    http_file_full_path = http_file_full_name
                    http_file_full_name = http_file_full_name + '/' + each_file['file_name']
                    
                    logutil.log(LOG, logutil.INFO, "Trying to download SFTP file from %s site: %s file" % (each_file['site'], http_file_full_name))
                    
                    self.claims_master_cursor.execute(update_query, ('downloading', self.get_now_time(), \
                                                                additional_status_msg, None, None, each_file['id']))
                    
                    #rename old file to old_filename_file_timestamp if present
                    target_file_full_path = each_file['local_dir'] + '/' + each_file['file_name'].split("/")[-1]
                    if os.path.exists(target_file_full_path):
                        shutil.move(target_file_full_path, target_file_full_path + '_%s' % \
                                                    (datetime.datetime.fromtimestamp(os.stat(target_file_full_path)[8]).strftime('%Y-%m-%d_%H:%M:%S'),))
                    
                    #download file
                    if http_file_full_name.startswith("https") or http_file_full_name.startswith("HTTPS"):
                        # Use username and password to connect
                        login_url = each_file['site']
                        if each_file.get("login_url"):
                            login_url = each_file['login_url']
                        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
                        password_mgr.add_password(None, login_url, each_file['username'], each_file['password'])
                        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
                        opener = urllib2.build_opener(handler)
                        http_client = opener.open(http_file_full_name)
                    else:
                        # Try to connect directly
                        http_client = urllib2.urlopen(http_file_full_name)
                    total_size = int(http_client.info().getheader('Content-Length').strip())
                    
                    with open(target_file_full_path, 'wb') as fp:
                        while True:
                            chunk = http_client.read(FILE_CHUNK_SIZE)
                            if not chunk: break
                            fp.write(chunk)
                    
                    downloaded_file_size = os.stat(target_file_full_path).st_size
                    if downloaded_file_size == total_size:
                        logutil.log(LOG, logutil.INFO, "Downloaded successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))

                        self.claims_master_cursor.execute(update_query, ('downloaded', self.get_now_time(), \
                                                                    additional_status_msg, each_file['file_name'].split("/")[-1], \
                                                                    each_file['local_dir'], each_file['id']))
                    else:
                        logutil.log(LOG, logutil.CRITICAL, "File Size mismatch. Adding for redownloading Updating status for sftp_file_id=%s" % (each_file['id'],))

                        self.claims_master_cursor.execute(update_query, ('needs-to-download', self.get_now_time(), \
                                                                    additional_status_msg, each_file['file_name'].split("/")[-1], \
                                                                    each_file['local_dir'], each_file['id']))
                        continue
                
                try:
                    logutil.log(LOG, logutil.INFO, "Changing file permission for=%s" % (target_file_full_path,))
                    os.chmod(target_file_full_path, 0664)
                    logutil.log(LOG, logutil.INFO, "Changed file permission successfully for=%s" % (target_file_full_path,))
                except Exception as mod_err:
                    logutil.log(LOG, logutil.INFO, "Error during changing file permission for=%s" % (target_file_full_path,))
                    logutil.log(LOG, logutil.INFO, "Error =%s" % (str(mod_err),))

            except Exception as error:
                backtrace = str(traceback.format_exc())
                error_message = "Error during file downloading: %s" % (error,)
                additional_status_msg = '%s\n%s' % (str(error), backtrace)
                status = 'downloading-error'
                process_datetime = self.get_now_time()

                self.update_message_list(all_error_message, error_message, backtrace, each_file, status, process_datetime)
                
                self._comment_jira(each_file['jira_ticket'], jira_params, additional_status_msg)
                
                self.claims_master_cursor.execute(update_query, (status, process_datetime, \
                                                            additional_status_msg, None, None, each_file['id']))
                
                if each_file['connection_protocol'].lower() == 'sftp':
                    try:
                        if sftp_con:
                            # Cleanup connection if connected
                            sftp_con.cleanup()
                    except NameError:
                        pass
                
                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, "SFTP File ID=%s, SFTP File Config ID=%s, File Name=%s, File Path=%s, File Datetime=%s" % \
                                                        (each_file['id'], each_file['sftp_files_config_id'], each_file['file_name'], \
                                                        each_file['file_path'], each_file['file_datetime']))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, backtrace)
                
        if all_error_message:
            subject = "[SFTP Files Monitor] Downloading SFTP files: Error occured"
            self.notify_users(subject, all_error_message, False)
            
        logutil.log(LOG, logutil.INFO, "END: Downloading SFTP files completed.")
        
    def _encrypt_files(self):
        """ Encrypt files that have cryptography type and recipients,change the status of all files to 'upload-ready'
        """
        
        logutil.log(LOG, logutil.INFO, "START: Encrypting files")

        logutil.log(LOG, logutil.INFO, "Updating status of SFTP files which don't have cryptography_type ...")
        query_str = """        UPDATE sftp_files as sf, sftp_files_config as sfc 
                                  SET status='upload-ready', process_datetime = NOW() 
                                WHERE sf.status = 'need-to-upload' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'upload'
                                      AND (sfc.cryptography_type IS NULL OR sfc.cryptography_type = '')
                    """

        logutil.log(LOG, logutil.INFO, "SQL to update status to 'upload-ready' for the SFTP files which " + \
                                                                "don't have Cryptography_type is \n%s" % (query_str,))

        rows = self.claims_master_cursor.execute(query_str)
        logutil.log(LOG, logutil.INFO, "Updated total %s SFTP file, ids: %s" % (rows, str(rows),))

        #update files for the next stage 'upload-encrypting-error' if they have cryptography type but not the recipients
        
        query_str = """SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `recipients`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'need-to-upload' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'upload'
                                      AND (sfc.recipients IS NULL OR sfc.recipients = '') AND sfc.cryptography_type IS NOT NULL
                    """

        logutil.log(LOG, logutil.INFO, "Determing SFTP files which has cryptography_type but not the recipient list... " + \
                                                                    "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        all_error_message = []
        upload_encrypt_error_status = 'upload-encrypting-error'
        upload_encrypt_error_additional_msg = "SFTP file config has cryptography_type but doesn't have recipient list"
        error_message = "Error during file encrypting. %s." % (upload_encrypt_error_additional_msg,)
        upload_encrypt_failure_status = 'upload-encrypting-failure'
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()

        for each_file in sftp_files:
            process_datetime = self.get_now_time()

            self.update_message_list(all_error_message, error_message, None, each_file, upload_encrypt_error_status, process_datetime)

            self._comment_jira(each_file['jira_ticket'], jira_params, error_message)

        logutil.log(LOG, logutil.INFO, "Going to update status to '%s' of total %s SFTP files" % \
                                                        (upload_encrypt_error_status, str(len(sftp_files)),) + \
                                                        "(which has cryptography_type but doesn't have recipient list)..." + \
                                                        "\nFile IDs: %s" % (str([ each_file['id'] for each_file in sftp_files]),))

        if sftp_files:
            query_str = """        UPDATE sftp_files as sf, sftp_files_config as sfc 
                                      SET status=%s, additional_status_msg=%s, process_datetime = NOW() 
                                    WHERE sf.status = 'need-to-upload' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'upload'
                                          AND (sfc.file_key IS NULL OR sfc.file_key = '') AND sfc.cryptography_type IS NOT NULL
                        """
    
            logutil.log(LOG, logutil.INFO, "SQL to update status is \n%s" % (query_str,))

            rows = self.claims_master_cursor.execute(query_str, (upload_encrypt_error_status, upload_encrypt_error_additional_msg))
            logutil.log(LOG, logutil.INFO, "Updated total %s SFTP file." % (rows,))

        query_str = """        SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `recipients`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'need-to-upload' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'upload'
                                      AND sfc.recipients IS NOT NULL AND sfc.cryptography_type IS NOT NULL
                             ORDER BY site
                    """

        logutil.log(LOG, logutil.INFO, "Determing SFTP files which needs to be encrypted... " + \
                                                                    "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        if not sftp_files:
            if all_error_message:
                subject = "[SFTP Files Monitor] Encrypting SFTP files: Error occured"
                self.notify_users(subject, all_error_message, False)
                
            logutil.log(LOG, logutil.INFO, "Nothing to encrypt...")
            logutil.log(LOG, logutil.INFO, "END: Encrypting SFTP files completed")
            return
        
        logutil.log(LOG, logutil.INFO, "Total SFTP files which will be encrypted: %s" % (len(sftp_files),))
        
        update_query = """        UPDATE sftp_files 
                                     SET status=%s, process_datetime = %s, additional_status_msg=%s, postprocess_file_name=%s 
                                   WHERE id=%s
                       """
        
        for each_file in sftp_files:
            additional_status_msg = None
            output_file_name = None

            try:
                if(each_file['cryptography_type'] == 'pgp'):
                    sftp_file_full_path = each_file['local_dir'] + '/' + each_file['file_name']
                    logutil.log(LOG, logutil.INFO, "Trying to encrypt file: %s file" % ( sftp_file_full_path))
                    self.claims_master_cursor.execute(update_query, ('upload-encrypting', self.get_now_time(), \
                                                                additional_status_msg, None, each_file['id']))
                    output_file_name = each_file['file_name']+'.gpg'
                    output_file_full_path = each_file['local_dir'] + '/' + output_file_name
                    if os.path.exists(output_file_full_path):
                        shutil.move(output_file_full_path, output_file_full_path + '_%s' % \
                                            (datetime.datetime.fromtimestamp(os.stat(output_file_full_path)[8]).strftime('%Y-%m-%d_%H:%M:%S'),))
                    with open(sftp_file_full_path, 'rb') as f:
                        status = self.gpg.encrypt_file(f, recipients = each_file['recipients'].split(','), output = output_file_full_path)
                    if status.ok:
                        logutil.log(LOG, logutil.INFO, "Encrypted file successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))
                        self.claims_master_cursor.execute(update_query, ('upload-ready', self.get_now_time(), \
                                                          additional_status_msg, output_file_name, each_file['id']))
                    else:
                        additional_status_msg = 'ok: ' + str(status.ok) + ', status: ' + str(status.status) \
                                                                + ', stderr: ' + str(status.stderr)
                        logutil.log(LOG, logutil.INFO, "Encryption failed... Updating status for sftp_file_id=%s" % (each_file['id'],))
                        process_datetime = self.get_now_time()
                        error_message = 'Error during file encryption. %s' % (additional_status_msg,)
                        self.update_message_list(all_error_message, 'Error during file encryption', additional_status_msg, \
                                                 each_file, upload_encrypt_failure_status, process_datetime)
                        self._comment_jira(each_file['jira_ticket'], jira_params, error_message)
                        self.claims_master_cursor.execute(update_query, (upload_encrypt_failure_status, process_datetime, \
                                                        additional_status_msg, None, each_file['id']))
                else:
                    logutil.log(LOG, logutil.WARNING, "Unknown Cryptography configured for sftp_file_id=%s" % (each_file['id'],))
                    additional_status_msg = "Unknown Cryptography configured"
                    error_message = 'Error during file encrypting. %s' % (additional_status_msg,)
                    process_datetime = self.get_now_time()
                    
                    self.update_message_list(all_error_message, error_message, None, \
                                                                each_file, upload_encrypt_failure_status, process_datetime)

                    self._comment_jira(each_file['jira_ticket'], jira_params, error_message)

                    self.claims_master_cursor.execute(update_query, (upload_encrypt_failure_status, process_datetime, \
                                                            additional_status_msg, output_file_name, each_file['id']))

            except Exception as error:
                #change status to 'error'
                backtrace = str(traceback.format_exc())
                error_message = "Error during file Encrypting: %s" % (error,)
                additional_status_msg = '%s\n%s' % (str(error), backtrace)
                process_datetime = self.get_now_time()

                self.update_message_list(all_error_message, error_message, backtrace, \
                                                            each_file, upload_encrypt_error_status, process_datetime)

                self._comment_jira(each_file['jira_ticket'], jira_params, additional_status_msg)

                self.claims_master_cursor.execute(update_query, (upload_encrypt_error_status, process_datetime, \
                                                            additional_status_msg, output_file_name, each_file['id']))

                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, "SFTP File ID=%s, SFTP File Config ID=%s, File Name=%s, File Path=%s, File Datetime=%s" % \
                                                        (each_file['id'], each_file['sftp_files_config_id'], each_file['file_name'], \
                                                        each_file['file_path'], each_file['file_datetime']))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, backtrace)
                
        if all_error_message:
            subject = "[SFTP Files Monitor] Encrypting SFTP files: Error occured"
            self.notify_users(subject, all_error_message, False)
            
        logutil.log(LOG, logutil.INFO, "END: Encrypting SFTP files completed.")
        
    def _sftp_path_exists(self, sftp, path):
        """To check if file already exists on sftp server
        """
        try:
            sftp.stat(path)
        except IOError, e:
            if e.errno == errno.ENOENT:
                return False
        else:
            return True
        
    def _upload_files(self):
        """ Uploads files to the given sites. It tries to upload files having status 
            'upload-ready' in sftp_file table and corresponding activity is 'upload' in the sftp_files_config table.
        """
        logutil.log(LOG, logutil.INFO, "START: Uploading SFTP files")

        query_str = """SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, `password`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`,sf.postprocess_file_name,
                                      `keep_alive`, `auth_key_file`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'upload-ready' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'upload'
                             ORDER BY site
                    """

        logutil.log(LOG, logutil.INFO, "SQL to determine the SFTP files which needs to be uploaded is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        if not sftp_files:
            logutil.log(LOG, logutil.INFO, "Nothing to upload...")
            logutil.log(LOG, logutil.INFO, "END: uploading SFTP files completed")
            return
        
        logutil.log(LOG, logutil.INFO, "Total SFTP files which will be uploaded: %s" % (len(sftp_files),))
        
        update_query = """    UPDATE sftp_files 
                                 SET status = %s, process_datetime = %s, additional_status_msg = %s, target_file_name = %s, target_file_path = %s
                               WHERE id=%s
                       """
        all_error_message = []
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()
        for each_file in sftp_files:
            additional_status_msg = None 

            try:
                sftp_file_full_path = each_file['file_path'] + '/' + each_file['file_name']
                logutil.log(LOG, logutil.INFO, "Trying to upload SFTP file from %s site: %s file" % (each_file['site'], sftp_file_full_path))
                
                self.claims_master_cursor.execute(update_query, ('uploading', self.get_now_time(), \
                                                            additional_status_msg, None, None, each_file['id']))
                if each_file['postprocess_file_name']:
                    sftp_file_full_path = each_file['file_path'] + '/' + each_file['postprocess_file_name']
                    
                if self.connected_sites.get(each_file['site'] + str(each_file['port']) + each_file['username']):
                    sftp_con = self.connected_sites[each_file['site'] + str(each_file['port']) + each_file['username']]
                else:
                    sftp_con = SFTPConnection(each_file['site'], each_file['username'], each_file['password'], each_file['port'],
                                              each_file['auth_key_file'], keep_alive = each_file['keep_alive'])
                    self.connected_sites[each_file['site'] + str(each_file['port']) + each_file['username']] = sftp_con
    
                #rename old file to old_filename_file_timestamp if present
                target_file_name = os.path.basename(sftp_file_full_path)
                target_file_full_path = each_file['remote_dir'] + '/' + target_file_name
                sftp_client = sftp_con.get_sftp_client()
                
                if self._sftp_path_exists(sftp_client,target_file_full_path):
                    file_name, file_extension = os.path.splitext(target_file_name)
                    target_file_name = '%s_%s%s' %(file_name, datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S'), file_extension)
                    target_file_full_path = each_file['remote_dir'] + '/' + target_file_name
                sftp_client.put(sftp_file_full_path, target_file_full_path)
                logutil.log(LOG, logutil.INFO, "Uploaded successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))
                self.claims_master_cursor.execute(update_query, ('uploaded', self.get_now_time(), \
                                                            additional_status_msg, target_file_name, each_file['remote_dir'], each_file['id']))
                sftp_con.cleanup()

            except Exception as error:
                backtrace = str(traceback.format_exc())
                error_message = "Error during file uploading: %s" % (error,)
                additional_status_msg = '%s\n%s' % (str(error), backtrace)
                status = 'uploading-error'
                process_datetime = self.get_now_time()

                self.update_message_list(all_error_message, error_message, backtrace, each_file, status, process_datetime)
                
                self._comment_jira(each_file['jira_ticket'], jira_params, additional_status_msg)
                
                self.claims_master_cursor.execute(update_query, (status, process_datetime, \
                                                            additional_status_msg, None, None, each_file['id']))

                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, "SFTP File ID=%s, SFTP File Config ID=%s, File Name=%s, File Path=%s, File Datetime=%s" % \
                                                        (each_file['id'], each_file['sftp_files_config_id'], each_file['file_name'], \
                                                        each_file['file_path'], each_file['file_datetime']))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, backtrace)
                
            finally:
                try:
                    if each_file['postprocess_file_name'] and os.path.exists(sftp_file_full_path):
                        os.remove(sftp_file_full_path)
                        self.claims_master_cursor.execute('update sftp_files set postprocess_file_name = NULL where id = %s',(each_file['id']))
                except OSError:
                    pass
                
                try:
                    if sftp_con:
                        # Cleanup connection if connected
                        sftp_con.cleanup()
                except NameError:
                    pass
                
        if all_error_message:
            subject = "[SFTP Files Monitor] Uploading SFTP files: Error occured"
            self.notify_users(subject, all_error_message, False)
            
        logutil.log(LOG, logutil.INFO, "END: Uploading SFTP files completed.")
    
    def _decrypt_files(self):
        """ Decrpyte downloaded files having status as 'downloaded', and if file_key/cryptography_type are present in the sftp_file table 
            and corresponding activity is 'download' in the sftp_files_config table.
        """
        logutil.log(LOG, logutil.INFO, "START: Decrypting SFTP files")

        #update files for the next stage 'download-decrpted' if they dont have cryptography type set
        logutil.log(LOG, logutil.INFO, "Updating status of SFTP files which don't have cryptography_type ...")
        query_str = """        UPDATE sftp_files as sf, sftp_files_config as sfc 
                                  SET status='download-decrypted', process_datetime = NOW() 
                                WHERE sf.status = 'downloaded' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'download'
                                      AND (sfc.cryptography_type IS NULL OR sfc.cryptography_type = '')
                    """

        logutil.log(LOG, logutil.INFO, "SQL to update status to 'download-decrypted' for the SFTP files which " + \
                                                                "don't have Cryptography_type is \n%s" % (query_str,))

        rows = self.claims_master_cursor.execute(query_str)
        logutil.log(LOG, logutil.INFO, "Updated total %s SFTP file, ids: %s" % (rows, str(rows),))

        #update files for the next stage 'download-decrpting-error' if they has cryptography type and doesn't have key
        
        query_str = """SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'downloaded' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'download'
                                      AND (sfc.file_key IS NULL OR sfc.file_key = '') AND sfc.cryptography_type IS NOT NULL
                    """

        logutil.log(LOG, logutil.INFO, "Determing SFTP files which has cryptography_type but doesn't have file_key... " + \
                                                                    "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        all_error_message = []
        download_decrypt_error_status = 'download-decrpting-error'
        download_decrypt_error_additional_msg = "SFTP file config has cryptography_type but doesn't have file_key"
        error_message = "Error during file decryption. %s." % (download_decrypt_error_additional_msg,)
        download_decrypt_failure_status = 'download-decrypting-failure'
        
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()
        for each_file in sftp_files:
            process_datetime = self.get_now_time()

            self.update_message_list(all_error_message, error_message, None, each_file, download_decrypt_error_status, process_datetime)

            self._comment_jira(each_file['jira_ticket'], jira_params, error_message)

        logutil.log(LOG, logutil.INFO, "Going to update status to '%s' of total %s SFTP files" % \
                                                        (download_decrypt_error_status, str(len(sftp_files)),) + \
                                                        "(which has cryptography_type but doesn't have file_key)..." + \
                                                        "\nFile IDs: %s" % (str([ each_file['id'] for each_file in sftp_files]),))

        if sftp_files:
            query_str = """        UPDATE sftp_files as sf, sftp_files_config as sfc 
                                      SET status=%s, additional_status_msg=%s, process_datetime = NOW() 
                                    WHERE sf.status = 'downloaded' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'download'
                                          AND (sfc.file_key IS NULL OR sfc.file_key = '') AND sfc.cryptography_type IS NOT NULL
                        """
    
            logutil.log(LOG, logutil.INFO, "SQL to update status is \n%s" % (query_str,))

            rows = self.claims_master_cursor.execute(query_str, (download_decrypt_error_status, download_decrypt_error_additional_msg))
            logutil.log(LOG, logutil.INFO, "Updated total %s SFTP file." % (rows,))

        query_str = """        SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`,
                                      `site`, `port`, `connection_protocol`, `username`, jira_ticket,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE sf.status = 'downloaded' AND sfc.id = sf.sftp_files_config_id AND sf.activity = 'download'
                                      AND sfc.file_key IS NOT NULL AND sfc.cryptography_type IS NOT NULL
                             ORDER BY site
                    """

        logutil.log(LOG, logutil.INFO, "Determing SFTP files which needs to be decrypt... " + \
                                                                    "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        if not sftp_files:
            if all_error_message:
                subject = "[SFTP Files Monitor] Decrypting SFTP files: Error occured"
                self.notify_users(subject, all_error_message, False)
                
            logutil.log(LOG, logutil.INFO, "Nothing to decrpt...")
            logutil.log(LOG, logutil.INFO, "END: Decrypting SFTP files completed")
            return
        
        logutil.log(LOG, logutil.INFO, "Total SFTP files which will be decrypted: %s" % (len(sftp_files),))
        
        update_query = """        UPDATE sftp_files 
                                     SET status=%s, process_datetime = %s, additional_status_msg=%s, postprocess_file_name=%s 
                                   WHERE id=%s
                       """
        
        for each_file in sftp_files:
            additional_status_msg = None
            output_file_name = None

            try:
                sftp_file_full_path = each_file['local_dir'] + '/' + each_file['file_name']
                logutil.log(LOG, logutil.INFO, "Trying to decrypt SFTP file from %s site: %s file" % (each_file['site'], sftp_file_full_path))
                
                self.claims_master_cursor.execute(update_query, ('download-decrpting', self.get_now_time(), \
                                                                        additional_status_msg, output_file_name, each_file['id']))

                pass_fd = open(each_file['file_key'], 'r')
                passphrase = pass_fd.read()
                pass_fd.close()
                
                if each_file['cryptography_type'].lower() == 'pgp':
                    # set decrypted file name to current file name + file timestamp if .pgp/.gpg are in not in the file name
                    output_file_name = each_file['file_name'] + '_%s' % (each_file['file_datetime'].strftime('%Y-%m-%d_%H:%M:%S'),)
                    
                    if each_file['file_name'] >= 5 and ('.pgp' in each_file['file_name'] or '.gpg' in each_file['file_name']):
                        #remove '.pgp' extension from decrypted file name
                        output_file_name = each_file['file_name'][:-4]
                        
                    #rename old file to old_filename_file_timestamp if present
                    output_file_full_path = each_file['local_dir'] + '/' + output_file_name
                    if os.path.exists(output_file_full_path):
                        shutil.move(output_file_full_path, output_file_full_path + '_%s' % \
                                            (datetime.datetime.fromtimestamp(os.stat(output_file_full_path)[8]).strftime('%Y-%m-%d_%H:%M:%S'),))

                    enc_file = open(sftp_file_full_path, 'r')
                    dec_file_object = self.gpg.decrypt_file(enc_file, output = output_file_full_path, passphrase = passphrase)
                    
                    if dec_file_object.ok:
                        logutil.log(LOG, logutil.INFO, "Decrypted file successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))
                        self.claims_master_cursor.execute(update_query, ('download-decrypted', self.get_now_time(), \
                                                            additional_status_msg, output_file_name, each_file['id']))
                    else:
                        additional_status_msg = 'ok: ' + str(dec_file_object.ok) + ', status: ' + str(dec_file_object.status) \
                                                            + ', stderr: ' + str(dec_file_object.stderr)
                        logutil.log(LOG, logutil.INFO, "Decryption failed... Updating status for sftp_file_id=%s" % (each_file['id'],))
                        process_datetime = self.get_now_time()
                        
                        error_message = 'Error during file decrypting. %s' % (additional_status_msg,)
                        
                        self.update_message_list(all_error_message, 'Error during file decrypting', additional_status_msg, \
                                                                    each_file, download_decrypt_failure_status, process_datetime)
                        
                        self._comment_jira(each_file['jira_ticket'], jira_params, error_message)

                        self.claims_master_cursor.execute(update_query, (download_decrypt_failure_status, process_datetime, \
                                                            additional_status_msg, None, each_file['id']))
                else:
                    logutil.log(LOG, logutil.WARNING, "Unknown Cryptography configured for sftp_file_id=%s" % (each_file['id'],))
                    additional_status_msg = "Unknown Cryptography configured"
                    error_message = 'Error during file decrypting. %s' % (additional_status_msg,)
                    process_datetime = self.get_now_time()
                    
                    self.update_message_list(all_error_message, error_message, None, \
                                                                each_file, download_decrypt_failure_status, process_datetime)

                    self._comment_jira(each_file['jira_ticket'], jira_params, error_message)

                    self.claims_master_cursor.execute(update_query, (download_decrypt_failure_status, process_datetime, \
                                                            additional_status_msg, output_file_name, each_file['id']))

            except Exception as error:
                #change status to 'error'
                backtrace = str(traceback.format_exc())
                error_message = "Error during file decrypting: %s" % (error,)
                additional_status_msg = '%s\n%s' % (str(error), backtrace)
                process_datetime = self.get_now_time()

                self.update_message_list(all_error_message, error_message, backtrace, \
                                                            each_file, download_decrypt_error_status, process_datetime)

                self._comment_jira(each_file['jira_ticket'], jira_params, additional_status_msg)

                self.claims_master_cursor.execute(update_query, (download_decrypt_error_status, process_datetime, \
                                                            additional_status_msg, output_file_name, each_file['id']))

                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, "SFTP File ID=%s, SFTP File Config ID=%s, File Name=%s, File Path=%s, File Datetime=%s" % \
                                                        (each_file['id'], each_file['sftp_files_config_id'], each_file['file_name'], \
                                                        each_file['file_path'], each_file['file_datetime']))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, backtrace)
                
        if all_error_message:
            subject = "[SFTP Files Monitor] Decrypting SFTP files: Error occured"
            self.notify_users(subject, all_error_message, False)
            
        logutil.log(LOG, logutil.INFO, "END: Decrypting SFTP files completed.")

    def _move_files(self):
        """ Moves decrypted files having status as 'download-decrypted' and if postprocess_dir is present in the sftp_file table 
            and corresponding activity is 'download' in the sftp_files_config table.
        """
        logutil.log(LOG, logutil.INFO, "START: Moving SFTP files")
        successful_download_files = []
        successful_upload_files = []
        query_str = """        SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, jira_ticket,
                                      `site`, `port`, `connection_protocol`, `username`, `postprocess_file_name`,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `description`, 
                                      `email_notification_id`, `day_to_process`
                                FROM `sftp_files` as sf, `sftp_files_config` as sfc
                                WHERE ((sf.status = 'download-decrypted' AND sf.activity = 'download') OR (sf.status = 'uploaded' AND sf.activity = 'upload'))
                                      AND sfc.id = sf.sftp_files_config_id 
                                      AND (sfc.postprocess_dir IS NULL OR sfc.postprocess_dir = '')
                    """

        logutil.log(LOG, logutil.INFO, "Updating status for the SFTP files which don't have postprocess_dir..." + \
                                                        "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        final_download_status = 'download-completed'
        final_download_message = 'Successfully downloaded SFTP File'
        
        final_upload_status = 'upload-completed'
        final_upload_message = 'Successfully uploaded File'
        jira_params, jira_timetracking_details = self.get_jira_session_and_params()
        
        for each_file in sftp_files:
            process_datetime = self.get_now_time()
            
            if each_file['activity'] == 'download':
                self.update_message_list(successful_download_files, final_download_message, None, \
                                                        each_file, final_download_status, process_datetime)
            
                self._resolve_jira(each_file['jira_ticket'], jira_params, \
                                                        'File download process completed. Resolved by SFTP Files Monitor service.', \
                                                        'Download completed')
            elif each_file['activity'] == 'upload':
                self.update_message_list(successful_upload_files, final_upload_message, None, \
                                                        each_file, final_upload_status, process_datetime)
            
                self._resolve_jira(each_file['jira_ticket'], jira_params, \
                                                        'File upload process completed. Resolved by SFTP Files Monitor service.', \
                                                        'Upload completed')
                if each_file['description'] and each_file['description'].lower() == 'incentive':
                    self.notify_users_incentive_upload(each_file)

        logutil.log(LOG, logutil.INFO, "Going to update total %s SFTP files(which don't have postprocess_dir)..." % \
                                                        (str(len(sftp_files)),) + \
                                                        "\nFile IDs: %s" % (str([ each_file['id'] for each_file in sftp_files]),))

        if sftp_files:
            query_str = """        UPDATE sftp_files as sf, sftp_files_config as sfc 
                                      SET status=(CASE WHEN sf.status = 'download-decrypted' THEN %s ELSE %s END), process_datetime = NOW() 
                                    WHERE ((sf.status = 'download-decrypted' AND sf.activity = 'download') OR (sf.status = 'uploaded' AND sf.activity = 'upload')) 
                                          AND sfc.id = sf.sftp_files_config_id
                                          AND (sfc.postprocess_dir IS NULL OR sfc.postprocess_dir = '')
                        """
            
            logutil.log(LOG, logutil.INFO, "SQL to update status is \n%s" % (query_str,))
            
            rows = self.claims_master_cursor.execute(query_str, (final_download_status,final_upload_status))
            logutil.log(LOG, logutil.INFO, "Updated total %s SFTP files" % (rows,))

        query_str = """        SELECT sf.id, `sftp_files_config_id`, `file_name`, `file_path`, `file_datetime`, jira_ticket,
                                      `site`, `port`, `connection_protocol`, `username`, `postprocess_file_name`,
                                      `file_key`, `cryptography_type`, sf.`activity`, `expected_date`, `frequency`, `source`,
                                      contact_person_name, contact_person_email,
                                      `file_detection_rule`, `remote_dir`, `local_dir`, `postprocess_dir`, `last_process_date`, `description`,
                                      `email_notification_id`, `day_to_process`
                                 FROM sftp_files as sf, sftp_files_config as sfc 
                                WHERE ((sf.status = 'download-decrypted' AND sf.activity = 'download') OR (sf.status = 'uploaded' AND sf.activity = 'upload')) 
                                      AND sfc.id = sf.sftp_files_config_id
                                      AND sfc.postprocess_dir IS NOT NULL
                             ORDER BY site
                    """

        logutil.log(LOG, logutil.INFO, "Determing SFTP files which needs to be moved to postprocess directory... " + \
                                                                    "Corresponding SQL is \n%s" % (query_str,))

        self.claims_master_cursor.execute(query_str)
        sftp_files = self.claims_master_cursor.fetchall()

        if not sftp_files:
            logutil.log(LOG, logutil.INFO, "Nothing to move...")
           # logutil.log(LOG, logutil.INFO, "END: Moving SFTP files completed")
        
        logutil.log(LOG, logutil.INFO, "Total SFTP files which will be moved: %s" % (len(sftp_files),))
        
        update_query = """    UPDATE sftp_files 
                                 SET status=%s, process_datetime = %s, additional_status_msg=%s, postprocess_file_path=%s 
                               WHERE id=%s
                       """
        
        all_error_message = []
        
        for each_file in sftp_files:
            additional_status_msg = None

            try:
                if each_file['connection_protocol'].lower() == 'http':
                    sftp_file_full_path = each_file['local_dir'] + '/' + each_file['file_name'].split("/")[-1]
                    target_file_full_path = each_file['postprocess_dir'] + '/' + each_file['file_name'].split("/")[-1]
                else:
                    sftp_file_full_path = each_file['local_dir'] + '/' + each_file['file_name']
                    target_file_full_path = each_file['postprocess_dir'] + '/' + each_file['file_name']
                
                if each_file['postprocess_file_name'] and each_file['activity'] == 'download':
                    sftp_file_full_path = each_file['local_dir'] + '/' + each_file['postprocess_file_name']
                    target_file_full_path = each_file['postprocess_dir'] + '/' + each_file['postprocess_file_name']
                    
                logutil.log(LOG, logutil.INFO, "Trying to move SFTP File \nfrom %s to %s" % (sftp_file_full_path, target_file_full_path))
                
                if each_file['activity'] == 'download':
                    self.claims_master_cursor.execute(update_query, ('download-moving', self.get_now_time(), \
                                                                additional_status_msg, each_file['postprocess_dir'], each_file['id']))
                else:
                    self.claims_master_cursor.execute(update_query, ('upload-moving', self.get_now_time(), \
                                                                additional_status_msg, each_file['postprocess_dir'], each_file['id']))

                #rename old file to old_filename_file_timestamp if present
                if os.path.exists(target_file_full_path):
                    shutil.move(target_file_full_path, target_file_full_path + '_%s' % \
                                                (datetime.datetime.fromtimestamp(os.stat(target_file_full_path)[8]).strftime('%Y-%m-%d_%H:%M:%S'),))
                
                shutil.move(sftp_file_full_path, target_file_full_path)
                
                logutil.log(LOG, logutil.INFO, "Moved file successfully... Updating status for sftp_file_id=%s" % (each_file['id'],))
                
                process_datetime = self.get_now_time()
                if each_file['activity'] == 'download':
                    self.update_message_list(successful_download_files, final_download_message, None, \
                                                        each_file, final_download_status, process_datetime)

                    self._resolve_jira(each_file['jira_ticket'], jira_params, \
                                                        'File download process completed. Resolved by SFTP Files Monitor service.', \
                                                        'Download completed')

                    self.claims_master_cursor.execute(update_query, (final_download_status, self.get_now_time(), \
                                                                additional_status_msg, each_file['postprocess_dir'], each_file['id']))
                elif each_file['activity'] == 'upload':
                    self.update_message_list(successful_upload_files, final_upload_message, None, \
                                                        each_file, final_upload_status, process_datetime)

                    self._resolve_jira(each_file['jira_ticket'], jira_params, \
                                                        'File upload process completed. Resolved by SFTP Files Monitor service.', \
                                                        'Upload completed')

                    self.claims_master_cursor.execute(update_query, (final_upload_status, self.get_now_time(), \
                                                                additional_status_msg, each_file['postprocess_dir'], each_file['id']))
                    if each_file['description'] and each_file['description'].lower() == 'incentive':
                        each_file['file_path'] = each_file['postprocess_dir']
                        self.notify_users_incentive_upload(each_file)
            except Exception as error:
                #change status to 'error'

                backtrace = str(traceback.format_exc())
                error_message = "Error during file moving: %s" % (error,)
                additional_status_msg = '%s\n%s' % (str(error), backtrace)
                if each_file['activity'] == 'download':
                    status = 'download-moving-error'
                else:
                    status = 'upload-moving-error'
                process_datetime = self.get_now_time()

                self._comment_jira(each_file['jira_ticket'], jira_params, additional_status_msg)
                
                self.update_message_list(all_error_message, error_message, backtrace, each_file, status, process_datetime)
                
                self.claims_master_cursor.execute(update_query, (status, process_datetime, \
                                                            additional_status_msg, each_file['postprocess_dir'], each_file['id']))

                logutil.log(LOG, logutil.CRITICAL, error_message)
                logutil.log(LOG, logutil.CRITICAL, '####### Files Data ######')
                logutil.log(LOG, logutil.CRITICAL, "SFTP File ID=%s, SFTP File Config ID=%s, File Name=%s, File Path=%s, File Datetime=%s" % \
                                                        (each_file['id'], each_file['sftp_files_config_id'], each_file['file_name'], \
                                                        each_file['file_path'], each_file['file_datetime']))
                logutil.log(LOG, logutil.CRITICAL, '####### Error Traceback ######')
                logutil.log(LOG, logutil.CRITICAL, backtrace)
                
        #Send Error messages
        if all_error_message:
            subject = "[SFTP Files Monitor] Moving SFTP files to postprocess directory: Error occured"
            self.notify_users(subject, all_error_message, False)
            
        #Send Successful message for download completed files
        if successful_download_files:
            subject = "[SFTP Files Monitor] Successfully download process completed: Total %s files downloaded." \
                                                        % (str(len(successful_download_files)))
            self.notify_users(subject, successful_download_files, False)
            # Notify download completion for each file to particular stakeholders
            subject = "[SFTP Files Monitor] Successfully download process completed"
            self.notify_stakeholders(subject, successful_download_files)
        
        if successful_upload_files:
            subject = "[SFTP Files Monitor] Successfully upload process completed: Total %s files uploaded." \
                                                        % (str(len(successful_upload_files)))
            self.notify_users(subject, successful_upload_files, False)
            # Notify upload completion for each file to particular stakeholders for other than incentive files
            selective_upload_files = [each for each in successful_upload_files if each.get("description") == None or \
                                                                            each.get("description").lower() != 'incentive']
            subject = "[SFTP Files Monitor] Successfully upload process completed"
            self.notify_stakeholders(subject, selective_upload_files)
            
        logutil.log(LOG, logutil.INFO, "END: Moving SFTP files completed.")
        
    def notify_users_incentive_upload(self, file_data):
        """ Sends user alerts via email messages 
        """
        logutil.log(LOG, logutil.INFO, "Sending email notification for incentive file id=%s, recipients=%s" % (file_data['id']
                                                , file_data['email_notification_id']))
        if self.is_test:
            recipients = [each_user.strip() + '@castlighthealth.com' for each_user in LOGGED_USERNAME.split(',')]
        else:
            recipients = [each_user.strip() for each_user in file_data['email_notification_id'].split(',')]
        timestamp = datetime.datetime.now()
        file_data['period_end'] = file_data['file_datetime']-datetime.timedelta(days=1)
        file_data['period_end'] = file_data['period_end'].date()
        file_data['process_date'] = file_data['file_datetime'].date()
        file_data['period_start'] = None
        if file_data['day_to_process']:
            file_data['period_start'] = datetime.date(file_data['file_datetime'].year -1 if file_data['file_datetime'].month == 1 else\
                                        file_data['file_datetime'].year ,12 if file_data['file_datetime'].month == 1 else file_data['file_datetime'].month-1,file_data['file_datetime'].day)
        elif file_data['frequency']:
            file_data['period_start'] = file_data['file_datetime']-datetime.timedelta(days=file_data['frequency'])
            file_data['period_start'] = file_data['period_start'].date()
        subject = 'Incentive file uploaded for %s - %s' % (file_data['source'], str(datetime.date.today()))
        subject = " ".join(subject.split())
        from_email = 'SFTP_files_manager <%s@castlighthealth.com>' % (LOGGED_USERNAME, )
        template_name = 'incentive_upload'
        logutil.log(LOG, logutil.INFO, "Email data prepared for sftp_file_id: %s" % (str(file_data['id']), ))
        DJANGO_EMAIL.send_email_template(template_name, file_data, subject, recipients, from_email) 

    def _process_files(self):
        """ Runs the file processing methods.
        """
        self._get_activities()
        self._scans_sites()
        self._download_files()
        self._decrypt_files()
        self._encrypt_files()
        self._upload_files()
        self._move_files()
        
    def start(self):
        self._process_files()

def main():
    program_descriptions = """Monitors configured SFTP sites and directories for sftp_file_config
                        and updates 'sftp_files' table. Creates JIRA tickets for errors.
                        """
    parser = argparse.ArgumentParser(description=program_descriptions)
    
    parser.add_argument("-p", "--properties_file", type=str,
                        dest="properties_file",
                        help = """default property file is %s 
                            """ % (DEFAULT_PROPERTY_FILE,),
                        default = DEFAULT_PROPERTY_FILE)
    
    parser.add_argument("-t", "--test_run",
                      action="store_true",
                      dest="test_run",
                      default=False,
                      help="Run in test mode.")
    
    parser.add_argument("-j", "--test_jira",
                      action="store_false",
                      dest="test_jira",
                      default=True,
                      help="This will create real Jira Tickets.")

    cmd_args = parser.parse_args()
    
    logutil.log(LOG, logutil.INFO, "SFTP Files Monitor")
    logutil.log(LOG, logutil.INFO, "Command line arguments passed are: %s" % str(cmd_args))

    get_logged_username()
    
    subject = "[SFTP Files Monitor] Unexpected Error Occurred"
    try:
        #start SFTP files manager
        files_monitor = SFTP_Files_Manager(cmd_args.properties_file, cmd_args.test_run, cmd_args.test_jira)
        files_monitor.start()
    except Exception as error:
        error_message = str(traceback.format_exc())
        logutil.log(LOG, logutil.CRITICAL, "Unknown Error in 'SFTP files monitor', %s:" % (error,))
        logutil.log(LOG, logutil.CRITICAL, '####### ERROR Traceback ######')
        logutil.log(LOG, logutil.CRITICAL, error_message)

        body = 'Unexpected Error Occurred: \n%s' % (error_message)
        _send_generic_email(subject, body, SCRIPT_FAILURE_NOTIFICATION_EMAILS, cmd_args.test_run)
    except:
        error_message = str(traceback.format_exc())
        logutil.log(LOG, logutil.CRITICAL, "Unknown Error in 'SFTP files monitor'")
        logutil.log(LOG, logutil.CRITICAL, '####### ERROR Traceback ######')
        logutil.log(LOG, logutil.CRITICAL, error_message)

        body = 'Unexpected Error Occurred: \n%s' % (error_message)
        _send_generic_email(subject, body, SCRIPT_FAILURE_NOTIFICATION_EMAILS, cmd_args.test_run)

if __name__ == "__main__":
    main()

