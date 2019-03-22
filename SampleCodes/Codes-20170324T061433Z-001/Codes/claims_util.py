from claims_bulk_load_helper import *
from cStringIO import StringIO
from claims_load_helper import *
from dbutils import *
from location_utils import *
from model import *
from optparse import OptionParser 
from statsutil import *
import MySQLdb
import datetime
import logutil
import hashlib
import os
import pprint
import traceback
import whcfg
import time
import platform
import pexpect
import sys
import utils
import claims_load_helper
import rx_claims_load_helper
import types
import provider_matcher
import facility_checker
import statsutil
import dbutils
import re
import warnings
import dental_bulk_loader

LOG = logutil.initlog('importer')
st = Stats("claims_util")

class ClaimsLoaderException(MasterLoaderException):
    
    CLAIMS_EXCEPTION_MESSAGES = {}
    
    def _get_standard_exception_message(self, exception_code):
        return ClaimsLoaderException.CLAIMS_EXCEPTION_MESSAGES.get(exception_code, super(ClaimsLoaderException,self)._get_standard_exception_message(exception_code))
        
class _Callable:
    def __init__(self, anycallable):
        self.__call__ = anycallable

class ClaimsProviderMatcher:

    def __init_options(self, input): 
        p = OptionParser(usage="""Usage: claims_util.py -m refresh_claim_participations
  -H, --Help                                              show this help message and exit
  -d DB_NAME, --db_name=DB_NAME                           name of claims master database
  -f IMPORTED_CLAIM_FILE_IDS --imported_claim_file_ids    List of Imported Claim File IDs for augmentation.""")

        p.add_option("-d", "--db_name", type="string",
                      dest="db_name",
                      help="Name of claims master database.")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids", 
                      help="Imported Claim File IDs. For e.g. 1,3,7")
        p.add_option("-c", "--claim_id", type="string",
                      dest="claim_id", 
                      help="Claim ID to run")
        if not input:
            print p.usage
            sys.exit(2)
        (self.method_options, args) = p.parse_args(input.split(' '))
    
    def __init__(self, input, logger):
        
        self.__init_options(input)
        
#        self._conn = getDBConnection(dbname = self.method_options.db_name,
#                              host = whcfg.claims_master_host,
#                              user = whcfg.claims_master_user,
#                              passwd = whcfg.claims_master_password,
#                              useDictCursor = True)
#        self._conn = getDBConnection(dbname = whcfg.claims_master_schema,
#                              host = whcfg.claims_master_host,
#                              user = whcfg.claims_master_user,
#                              passwd = whcfg.claims_master_password,
#                              useDictCursor = True)
        
        self.imported_claim_file_id = self.method_options.imported_claim_file_ids
        
        self.id = self.method_options.claim_id
        self.fc = facility_checker.FacilityChecker()
        self.matches = {} 
    
    def process(self):
        _conn = None
        try:
            _conn = getDBConnection(dbname = self.method_options.db_name,
                              host = whcfg.claims_master_host,
                              user = whcfg.claims_master_user,
                              passwd = whcfg.claims_master_password,
                              useDictCursor = True)
            
            self.match(_conn)
        finally:
            dbutils.close_connections([_conn])
        
    def match(self, _conn):

        self.fac_claims_run_logs = ModelFactory.get_instance(_conn, 'claims_run_logs')
        
        t_claim_provider_exceptions = Table(_conn, 'claim_provider_exceptions')
        if self.id:
            t_claim_provider_exceptions.search('imported_claim_file_id IN (%s) AND id=%s AND provider_name is not NULL and provider_id is NULL' % (self.imported_claim_file_id, self.id))
        else:
            t_claim_provider_exceptions.search('imported_claim_file_id IN (%s) AND provider_name is not NULL and provider_id is NULL' % self.imported_claim_file_id)
        
        pp = pprint.PrettyPrinter()
        total = 0
        num_queries = 0
        num_good_matches = 0
        pm = provider_matcher.ProviderMatcher()
        
        update_query = """UPDATE %s.claim_provider_exceptions
                             SET provider_id=%s,
                                 location_id=%s,
                                 match_code=%s,
                                 match_date=DATE(NOW())
                           WHERE claim_id=%s""" % (whcfg.claims_master_schema,
                                             '%s',
                                             '%s',
                                             '%s',
                                             '%s')  
        update_query_v= []
        if len(t_claim_provider_exceptions) <= 20000:
            for provider_exception in t_claim_provider_exceptions:
#                logutil.log(LOG, logutil.INFO, 'Done')
                match_entry = {'provider_display_name': provider_exception['provider_name'],
                           'provider_npi': None,
                           'insurance_company_id': provider_exception['insurance_company_id'],
                           'provider_addresses': [{'street': provider_exception['street_address'],
                                                   'city': provider_exception['city'],
                                                   'state': provider_exception['state'] if provider_exception['state'] else provider_exception['subscriber_state'],
                                                   'zip': provider_exception['zip'] if provider_exception['zip'] else provider_exception['subscriber_zip']}]
                           }
             
                me_hash = ':'.join([match_entry['provider_display_name'] if match_entry['provider_display_name'] else '',
                            match_entry['provider_addresses'][0]['street'] if match_entry['provider_addresses'][0]['street'] else '',
                            match_entry['provider_addresses'][0]['city'] if match_entry['provider_addresses'][0]['city'] else '',
                            match_entry['provider_addresses'][0]['state'] if match_entry['provider_addresses'][0]['state'] else '',
                            match_entry['provider_addresses'][0]['zip'] if match_entry['provider_addresses'][0]['zip'] else ''
                            ])
            
#                me_hash = hashlib.sha1(repr(sorted(match_entry.items())))
                total = total + 1
                existing_match = self.matches.get(me_hash)
            
                if not existing_match:
                 
                    is_facility = False
                    try:
                        is_facility = self.fc.is_facility(provider_exception['provider_name'])
                    except:
                        continue
                
                    match_entry['provider_type'] = 'facility' if is_facility else 'practitioner'
                           
                    num_queries = num_queries + 1
                    cid = st.start("getBestProviderMatches", "getBestProviderMatches")

                    try:
                        res = pm.getBestProviderMatches(match_entry)
                    except Exception as e:
                        pm = provider_matcher.ProviderMatcher()
                        print e
                        continue
                    
                    st.end(cid)
                
                    stats_report = open(whcfg.providerhome + '/claims/claims_match_stats_report.txt', 'w')
                    stats_report.write(st.report())
                    stats_report.close()
        
                    self.matches[me_hash] = res
                    print 'INPUT(%s)(total:%s,num_queries:%s,num_good:%s):' % (me_hash, total, num_queries, num_good_matches)
                    if res[0].get('match_result')[0].get('code') <= 1090:
                        update_query_v.append((res[0].get('provider_id'),res[0].get('location_id'),res[0].get('match_result')[0].get('code'), provider_exception['claim_id']))
                        num_good_matches = num_good_matches + 1
                else:
                    if existing_match[0].get('match_result')[0].get('code') <= 1090:
                        update_query_v.append((existing_match[0].get('provider_id'),existing_match[0].get('location_id'),existing_match[0].get('match_result')[0].get('code'), provider_exception['claim_id']))
                        num_good_matches = num_good_matches + 1
        
            print 'Updating claim_provider_exceptions: %s' % len(update_query_v) 
            _conn.cursor().executemany(update_query, update_query_v)        
            print 'Total Entries: %s. Num Queries: %s. Num Good Matches: %s' % (total, num_queries, num_good_matches)
        
            update_claims = """UPDATE claims c
                           JOIN claim_provider_exceptions cpe ON c.id=cpe.claim_id
                           SET c.provider_id=cpe.provider_id,
                               c.provider_location_id=IFNULL(cpe.location_id,-1)
                           WHERE cpe.imported_claim_file_id IN (%s)
                           AND cpe.provider_id is not null""" % self.imported_claim_file_id
                             
            _conn.cursor().execute(update_claims)   
        else:
            print 'skipping provider matcher as count of claim provider exception is %s' % (len(t_claim_provider_exceptions)) 
            
class ClaimsRunLogger:
    
    def __init__(self, run_type, run_options):
        timevalue = datetime.datetime.now()
        now = timevalue.isoformat(' ').split('.')[0]
        self._start = now
        self._finish = None
        self._run_type = run_type
        self._run_options = run_options

        
    def finish(self, status = 'success', status_message = None):
        if not self._finish:
            timevalue = datetime.datetime.now()
            now = timevalue.isoformat(' ').split('.')[0]
            self._finish = now
            cro_entry = {'run_type':self._run_type,
                         'run_start':self._start,
                         'run_finish':now,
                         'run_options':self._run_options,
                         'run_status':status,
                         'run_status_message': status_message
                         }
            _conn = None
            try:
                _conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = whcfg.claims_master_host,
                                  user = whcfg.claims_master_user,
                                  passwd = whcfg.claims_master_password,
                                  useDictCursor = True)
                fac_claims_run_logs = ModelFactory.get_instance(_conn, 'claims_run_logs')
                print cro_entry
                fac_claims_run_logs.create(cro_entry)
            finally:
                dbutils.close_connections([_conn])
        
        
class ClaimsLockFileHelper:
    KNOWN_LOCK_FILES = {'claims_master_prod':'/share/whdata/backup/phi/sql_dumps_revenge/presales/presales1.lock',
                        'claims_master_presales':'/share/whdata/backup/phi/sql_dumps_revenge/presales/presales.lock'
                        }
    
    def get_instance(lock_file_location = None):
        if not lock_file_location:
            if whcfg.claims_master_schema:
                lock_file_location = ClaimsLockFileHelper.KNOWN_LOCK_FILES.get(whcfg.claims_master_schema.lower())
            if not lock_file_location:
                return LockFile()
            else:
                return LockFile(lock_file_location)
        else:
            return LockFile(lock_file_location)
            
    get_instance = _Callable(get_instance)
    
class LockFile:
    def __init__(self, lock_file_location = None):
        self.lock_file_location = lock_file_location
    
    def release_lock(self):
        if self.lock_file_location:
            release_lock_file = os.system("rm -f %s" % self.lock_file_location)
            return release_lock_file <= 0
        return True
    
    def acquire_lock(self):
        if self.lock_file_location:
            lock_file = os.system("lockfile -r1 %s" % self.lock_file_location)
            return lock_file <= 0
        return True
    
helpers = {"augment_claims_grouper": lambda input, logger: AugmentClaimsGrouper(input, logger), 
           "refresh_claim_participations": lambda input, logger: RefreshClaimParticipations(input, logger),
           "claims_validation_report": lambda input, logger: ClaimsValidator(input, logger),
           "sync_grouper_tables": lambda input, logger: SyncGrouperTables(input, logger),
           "create_test_claims_master_instance": lambda input, logger: ClaimsTestInstanceCreator(input, logger),
           "create_test_provider_master_instance": lambda input, logger: ProviderTestInstanceCreator(input, logger),
           "match_claim_providers": lambda input, logger: ClaimsProviderMatcher(input, logger),
           "refresh_claim_participations_provider_location": lambda input, logger: RefreshClaimParticipationsProviderLocation(input, logger),
           "profile_raw_claims": lambda input, logger: RawClaimsProfiler(input, logger),
           "refresh_patients": lambda input, logger: RefreshPatients(input, logger),
           }

#class _Callable:
#    def __init__(self, anycallable):
#        self.__call__ = anycallable
class RefreshPatients:
    
    def __init_options(self, input): 
        p = OptionParser(usage="""Usage: claims_util.py -m refresh_patients
                                  -H, --Help                                              show this help message and exit
                                  -i INSURANCE_COMPANY_ID, --insurance_company_id         ID of the insurance company
                                  -e EMPLOYER_ID, --employer_id                           ID of the employer
                                  -t TIME_INTERVAL, --time_interval                       Time Interval of claims to consider (in days, defaults to 180 days)
                                  -f IMPORTED_CLAIM_FILE_IDS --imported_claim_file_ids    List of Imported Claim File IDs for augmentation
                                  -s subscriber_account_id --subscriber_account_id
                                  -a patient_account_id --patient_account_id
                                  -u unidentified claims --unidentified_claims""")

        p.add_option("-i", "--insurance_company_id", type="string",
                      dest="insurance_company_id",
                      help="Insurance Company ID.")              
        p.add_option("-e", "--employer_id", type="string",
                      dest="employer_id", 
                      help="Employer ID.")
        p.add_option("-t", "--time_interval", type="string",
                      dest="time_interval", default="180",
                      help="Time Interval of claims to consider (in days, defaults to 180 days).")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids", 
                      help="Imported Claim File IDs. For e.g. 1,3,7")
        p.add_option("-s", "--subscriber_account_id", type = "string",
                      dest="subscriber_account_id",
                      help="Used for selective rehash of specific user" )
        p.add_option("-a","--patient_account_id", type="string",
                      dest="patient_account_id",
                      help="Used for selective rehash of specific user")
        p.add_option("-u","--unidentified_claims", 
                      action="store_true",
                      default=False,
                      dest="unidentified_claims",
                      help="Used for selective rehash of specific user")

        if not input:
            print p.usage
            sys.exit(2)
        (self.method_options, args) = p.parse_args(input.split(' '))
        
        if not self.method_options.employer_id and not self.method_options.insurance_company_id:
            print p.usage
            sys.exit(2)
    
    def __init__(self, input, logger):
        
        self.logger = logger if logger else logutil.initlog('importer')
        
        self.method_options = None
        self.scratch_tables_created = set([])
        
        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)
        
        self.conn = getDBConnection(dbname = whcfg.claims_master_schema,
                              host = whcfg.claims_master_host,
                              user = whcfg.claims_master_user,
                              passwd = whcfg.claims_master_password,
                              useDictCursor = True)

        self.master_conn = getDBConnection(dbname = whcfg.master_schema,
                              host = whcfg.master_host,
                              user = whcfg.master_user,
                              passwd = whcfg.master_password,
                              useDictCursor = True)
        
        self.imported_claim_file_ids = None
        self.insurance_company_id = None
        self.employer_id = None
        
        if self.method_options.imported_claim_file_ids:
            self.imported_claim_file_ids = [int(i) for i in self.method_options.imported_claim_file_ids.split(',')]
        if self.method_options.insurance_company_id:
            self.insurance_company_id = int(self.method_options.insurance_company_id)
        if self.method_options.employer_id:
            self.employer_id = int(self.method_options.employer_id)
            
    def __lookup_imported_claim_files(self, icf_ids, ic_id, employer_id):
        icf_query = """SELECT DISTINCT icf.table_name, icf.id, icf.load_properties
                   FROM imported_claim_files icf, imported_claim_files_insurance_companies icfic 
                   WHERE icf.id=icfic.imported_claim_file_id
                   AND icf.claim_file_type = 'M'"""    
        if employer_id:
            icf_query = icf_query + """ AND icf.employer_id=%d""" % (employer_id)

        if ic_id:
            icf_query = icf_query + """ AND icfic.insurance_company_id=%d""" % (payer_id)

        if icf_ids:
            icf_query = icf_query + """ AND icf.id IN (%s)""" % ','.join([str(x) for x in icf_ids])
            
        icf_results = Query(self.conn, icf_query)
        
        return icf_results

    def process(self):
        
        icf_results = self.__lookup_imported_claim_files(self.imported_claim_file_ids, self.insurance_company_id, self.employer_id)
        patient_account_id = self.method_options.patient_account_id
        subscriber_patient_account_id = self.method_options.patient_account_id
        for icf_result in icf_results:
            fac_bulk_claims_loader = ClaimsBulkLoaderFactory.get_instance(self.conn, self.master_conn, icf_result['id'])
            fac_bulk_claims_loader.rehash_claim_patients(LOG, subscriber_patient_account_id, patient_account_id, self.method_options.unidentified_claims)
    

class ClaimsUtilFactory:
    
    def get_instance(method_name, input, logger = None):
        return helpers[method_name](input, logger)
    
    get_instance = _Callable(get_instance)


class SyncGrouperTables:
    
    ALLOWED_SYNC_SERVERS = set(['wh2.castlighthealth.com'])
    
    SYNC_TABLES = {'claims':['betos', 
                              'claim_attributes', 
                              'claim_specialties', 
                              'claims', 
                              'claims_grouper', 
                              'claims_run_logs', 
                              'external_procedure_code_types', 
                              'external_service_places', 
                              'external_service_types', 
                              'imported_claim_files', 
                              'imported_claim_files_insurance_companies', 
                              'internal_member_ids', 
                              'labels', 
                              'new_procedure_code_to_procedure_mappings', 
                              'pantry_procedure_mappings', 
                              'pantry_procedures', 
                              'procedure_code_types', 
                              'procedure_codes', 
                              'procedure_labels', 
                              'procedure_modifiers', 
                              'service_descriptions', 
                              'service_places', 
                              'service_types'],
                    'providers':['employers', 
                              'insurance_companies', 
                              'locations', 
                              'providers', 
                              'providers_specialties', 
                              'specialties']}

#    SYNC_TABLES = {'claims':['claims']} 
    
    SYNC_ENVIRONMENTS = {'production':{'master':{'host':'wh2.castlighthealth.com',
                                                 'port':'3306',
                                                 'schema': {'claims':'claims_master_prod',
                                                            'providers':'provider_master_prod'}},
                                       'grouper':{'host':'whperf.castlighthealth.com',
                                                  'port':'3306',
                                                  'schema':'claims_master_prod_xphi'}
                                       }
                         }
    
    
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m sync_grouper_tables
  -H, --Help                                              show this help message and exit
  -e ENVIRONMENT, --environment=ENVIRONMENT               Grouper environment to sync. Supported environments: production
  -f DB_CONFIG_FILE, --db_config_file=DB_CONFIG_FILE      location of mapping properties file.
  -d DRY_RUN, --dry_run                                   dry run.""") 

        p.add_option("-e", "--environment", type="string",
                      dest="environment", 
                      help="Grouper environment to sync. Supported environments: production")
                
        p.add_option("-f", "--db_config_file", type="string",
                      dest="db_config_file", default='db.yml',
                      help="Location of db config file.")
        
        p.add_option("-d", "--dry_run",
                      type="string",
                      dest="dry_run",
                      default='False',
                      help="Dry Run only. Will not write to database.") 
                      
        if not input:
            print p.usage
            sys.exit(2)
        (self.sync_grouper_tables_options, args) = p.parse_args(input.split(' '))
        
        if (not self.sync_grouper_tables_options or 
            not self.sync_grouper_tables_options.environment or
            not self.sync_grouper_tables_options.db_config_file):
            print p.usage
            sys.exit(2)

    def __init__(self, input, logger):
        self.sync_grouper_tables_options = None
        self.__init_options(input)
        self.logger = logger if logger else logutil.initlog('importer')
        self.local_host_name = platform.node().lower()
        self.db_configuration = None
        self.dry_run = True if self.sync_grouper_tables_options.dry_run == 'True' else False
        
        self.master_host = None
        self.master_port = None
        self.master_claims_schema = None
        self.master_providers_schema = None
        self.master_creds = None
        
        self.grouper_host = None
        self.grouper_port = None
        self.grouper_creds = None 
        
        self.__validate_input()
        
    def __validate_input(self):
        
        if self.local_host_name not in SyncGrouperTables.ALLOWED_SYNC_SERVERS:
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'Grouper Sync cannot be run from %s' % (self.local_host_name))
        
        if self.sync_grouper_tables_options.environment.lower() not in SyncGrouperTables.SYNC_ENVIRONMENTS.keys():
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'Unknown Sync environment: %s' % (self.sync_grouper_tables_options.environment))
        
        f_db_config = open(self.sync_grouper_tables_options.db_config_file,'r')
        if not f_db_config:
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'Unable to read DB Configuration file: %s' % (self.sync_grouper_tables_options.environment))
        
        self.db_configuration = yaml.load(f_db_config)
        env_config = SyncGrouperTables.SYNC_ENVIRONMENTS.get(self.sync_grouper_tables_options.environment.lower())
        
        self.master_host = env_config.get('master').get('host').lower()
        self.master_port = env_config.get('master').get('port')
        self.master_claims_schema = env_config.get('master').get('schema').get('claims')
        self.master_providers_schema = env_config.get('master').get('schema').get('providers')
        
        self.grouper_host = env_config.get('grouper').get('host').lower()
        self.grouper_port = env_config.get('grouper').get('port')
        self.grouper_schema = env_config.get('grouper').get('schema')
        
        self.master_creds = self.__get_creds(self.master_host, self.master_port)
        self.grouper_creds = self.__get_creds(self.grouper_host, self.grouper_port)
        
        if not self.master_creds:
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'DB credentials unavailable for Master host: %s, port: %s' % (self.master_host, self.master_port))
        
        if not self.grouper_creds:
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'DB credentials unavailable for Grouper host: %s, port: %s' % (self.grouper_host, self.grouper_port))
    
    def __get_creds(self, host, port): 
        creds = None
        for cf_entry in self.db_configuration:
            if ((host.lower() == cf_entry.get('host').lower()) 
                and 
                (str(port) == str(cf_entry.get('port')))):
                creds = {'u': cf_entry.get('user'),
                        'p': cf_entry.get('password')}
                break
        return creds
    
    def process(self):
        logutil.log(self.logger, logutil.INFO, "Entering process() method.")
        # TODO: acquire lock file as needed
        master_host_name = '127.0.0.1' if self.local_host_name == self.master_host else self.master_host
        grouper_host_name = '127.0.0.1' if self.local_host_name == self.grouper_host else self.grouper_host
        
        control_method = 'print' if self.dry_run else 'execute'
        
        try:
            for schema, tables in SyncGrouperTables.SYNC_TABLES.iteritems():
                for table in tables:
                    sync_cmd = "%s/util/third-party/mk-table-sync --%s --ask-pass h=%s,u=%s,D=%s,t=%s h=%s,D=%s,u=%s" % (whcfg.providerhome,
                                                                                                                             control_method,
                                                                                                                             master_host_name,
                                                                                                                             self.master_creds.get('u'), 
                                                                                                                             self.master_claims_schema if schema =='claims' else self.master_providers_schema,
                                                                                                                             table,
                                                                                                                             grouper_host_name,
                                                                                                                             self.grouper_schema,
                                                                                                                             self.grouper_creds.get('u'))
                    print sync_cmd
                    child = pexpect.spawn(sync_cmd)
                    child.expect('(?i)%s' % master_host_name)
                    child.sendline(self.master_creds.get('p')) 
                    child.expect('(?i)%s' % grouper_host_name)
                    child.sendline(self.grouper_creds.get('p'))
                    try:
                        child.interact()
                    except:
                        if child.isalive():
                            child.close() 
                    
        finally:
            logutil.log(self.logger, logutil.INFO, "Exiting process() method.")
                
def sync_grouper_tables(environment, db_config_file_location, dry_run=True):
    method_input = """-e %s -f '%s' -d True""" % (environment, db_config_file_location) if dry_run else """-e %s -f '%s'""" % (environment, db_config_file_location)
    method_handler = ClaimsUtilFactory.get_instance('sync_grouper_tables', method_input)
    method_handler.process()
            
class AugmentClaimsGrouper:
    
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m augment_claims_grouper
  -H, --Help                                              show this help message and exit
  -p PROPERTIES_FILE, --properties_file=PROPERTIES_FILE   location of mapping properties file.
  -t TABLE_NAME, --table_name=TABLE_NAME                  name of _imported_claims table name. e.g., aetna_imported_claims
  -d DB_NAME, --db_name=DB_NAME                           name of claims master database""") 
        
        p.add_option("-p", "--properties_file", type="string",
                      dest="properties_file", default='mappings.yml',
                      help="Location of mapping properties file.")
        p.add_option("-t", "--table_name", type="string",
                      dest="table_name",
                      help="Name of _imported_claims table name. e.g., aetna_imported_claims")
        p.add_option("-d", "--db_name", type="string",
                      dest="db_name",
                      help="Name of claims master database.")
#        p.add_option("-i", "--imported_claim_file_ids", type="string",
#                      dest="imported_claim_file_ids",
#                      help="Comma separated list of Imported Claim File IDs for which to rehash claims.")
                  
        if not input:
            print p.usage
            sys.exit(2)
        (self.augment_claims_grouper_options, args) = p.parse_args(input.split(' '))

#        icf_ids = self.augment_claims_grouper_options.imported_claim_file_ids.split(',') if self.augment_claims_grouper_options.imported_claim_file_ids else None
#        if icf_ids:
#            self.augment_claims_grouper_options.imported_claim_file_ids = set([int(x) for x in self.augment_claims_grouper_options.imported_claim_file_ids.split(',')])

            
    def __init__(self, input, logger):
        self.augment_claims_grouper_options = None
        self.mapping_properties = None
        
        self.__init_options(input)
        self.logger = logger if logger else logutil.initlog('importer')
    
    def __create_missing_tables(self, conn, logger):
        
        logutil.log(self.logger, logutil.INFO, "Create tables claims_grouper, internal_member_ids if they do not already exist.")
        create_claims_grouper = """CREATE TABLE IF NOT EXISTS claims_grouper 
                                    (
                                      claim_id int(11) NOT NULL,
                                      employer_id INT(11) NOT NULL,
                                      insurance_company_id INT(11) NOT NULL,
                                      employee_id_hash varchar(40) DEFAULT NULL,
                                      internal_member_id INT(11), 
                                      claim_specialty_id int(11) DEFAULT NULL,
                                      patient_status varchar(20) DEFAULT NULL,
                                      mangled_patient_dob date DEFAULT NULL,
                                      patient_gender char(1) DEFAULT NULL,
                                      raw_ndc_code varchar(20) DEFAULT NULL,
                                      raw_revenue_code varchar(10) DEFAULT NULL,
                                      source_record_id1 varchar(20) DEFAULT NULL,
                                      source_record_id2 varchar(20) DEFAULT NULL,
                                      imported_claim_file_id INT(11),
                                      member_id_hash VARCHAR(40),
                                      PRIMARY KEY (claim_id),
                                      KEY employee_id_ix (employee_id_hash),
                                      KEY member_id_hash_ix (imported_claim_file_id, member_id_hash),
                                      KEY claim_id_ix (imported_claim_file_id, claim_id)
                                    ) ENGINE=MyISAM"""
                                    
        conn.cursor().execute(create_claims_grouper)
        
        create_internal_member_ids = """CREATE TABLE IF NOT EXISTS internal_member_ids
                                        (
                                        id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                                        member_id_hash VARCHAR(40),
                                        UNIQUE INDEX midh_uq(member_id_hash)
                                        ) ENGINE=MyISAM"""
        
        conn.cursor().execute(create_internal_member_ids)
    
    def __mangle_date(self, date_column_name):
#        return """DATE(CONCAT(5*ROUND(YEAR(%s)/5),'-01-01'))""" % date_column_name
        return date_column_name
        
    def process(self):
        
        logutil.log(self.logger, logutil.INFO, "Inside process() method.")
        mapping_properties_stream = None
        
        if self.augment_claims_grouper_options.properties_file:
            mapping_properties_stream = open(self.augment_claims_grouper_options.properties_file, 'r')
                    
        self.mapping_properties = yaml.load(mapping_properties_stream) if mapping_properties_stream else None
#        pprint.pprint(self.mapping_properties)
        
        table_names = [self.augment_claims_grouper_options.table_name] if self.augment_claims_grouper_options.table_name else self.mapping_properties.keys()
        
        conn = getDBConnection(dbname = self.augment_claims_grouper_options.db_name,
                              host = whcfg.claims_master_host,
                              user = whcfg.claims_master_user,
                              passwd = whcfg.claims_master_password,
                              useDictCursor = True)
        
        self.__create_missing_tables(conn, self.logger)
        
        q_icf = Query(conn, """SELECT id, table_name from imported_claim_files
                                WHERE id IN 
                                 (SELECT distinct c.imported_claim_file_id 
                                    FROM claims c
                                    LEFT OUTER JOIN claims_grouper cp
                                      ON c.id=cp.claim_id
                                   WHERE cp.claim_id is null)
                                """)
        
        new_icf_entries = {}
        for res in q_icf:
            icf_ids = new_icf_entries.get(res['table_name'], [])
            icf_ids.append(res['id'])
            new_icf_entries[res['table_name']] = icf_ids
        
        c = conn.cursor()    
        for table_name in table_names:
            logutil.log(self.logger, logutil.INFO, "Augmenting claims_grouper from table %s." % table_name)
            icf_ids = new_icf_entries.get(table_name, None)
            if icf_ids:
                icf_ids_str = [str(id) for id in icf_ids]
            
                logutil.log(self.logger, logutil.INFO, "Updating internal_member_ids table.")
                insert_internal_member_ids = """INSERT IGNORE INTO internal_member_ids (member_id_hash)
                                                SELECT DISTINCT member_id FROM claims
                                                WHERE imported_claim_file_id IN (%s)
                                                """ % (','.join(icf_ids_str))
                c.execute(insert_internal_member_ids)
                                                
                logutil.log(self.logger, logutil.INFO, "Augmenting claims_grouper table with claims from imported_claim_file_ids: %s" % (','.join(icf_ids_str)))
                insert_grouper = """INSERT IGNORE INTO claims_grouper 
                                (claim_id, employer_id, insurance_company_id, imported_claim_file_id, member_id_hash, employee_id_hash, raw_ndc_code, source_record_id1, raw_revenue_code, source_record_id2, mangled_patient_dob, patient_gender)
                                SELECT c.id as claim_id,
                                       c.employer_id,
                                       c.insurance_company_id,
                                       c.imported_claim_file_id,
                                       c.member_id as member_id_hash,
                                       SHA1(%s) as employee_id_hash,
                                       %s as raw_ndc_code,
                                       %s as source_record_id1,
                                       %s as raw_revenue_code,
                                       %s as source_record_id2,
                                       %s as mangled_patient_dob,
                                       %s as patient_gender
                                  FROM claims c, %s iic
                                 WHERE c.imported_claim_id=iic.id
                                   AND c.imported_claim_file_id=iic.imported_claim_file_id
                                   AND iic.imported_claim_file_id IN (%s)
                                """ % (self.mapping_properties[table_name]['employee_id'],
                                       self.mapping_properties[table_name]['raw_ndc_code'] if self.mapping_properties[table_name].get('raw_ndc_code') else "''",
                                       self.mapping_properties[table_name]['source_record_id1'],
                                       self.mapping_properties[table_name]['raw_revenue_code']if self.mapping_properties[table_name].get('raw_revenue_code') else "''",
                                       self.mapping_properties[table_name]['source_record_id2'],
                                       self.__mangle_date(self.mapping_properties[table_name]['patient_dob']),
                                       self.mapping_properties[table_name]['patient_gender'],
                                       table_name,
                                       ','.join(icf_ids_str))
                c.execute(insert_grouper)
                
                logutil.log(self.logger, logutil.INFO, "Updating internal_member_id in claims_grouper table.")
                update_internal_member_id = """UPDATE claims_grouper cp, internal_member_ids im
                                         SET cp.internal_member_id=im.id
                                       WHERE cp.imported_claim_file_id in (%s)
                                         AND cp.member_id_hash = im.member_id_hash 
                                         """ % (','.join(icf_ids_str))
                c.execute(update_internal_member_id)
                                
                logutil.log(self.logger, logutil.INFO, "Updating specialty_id in claims_grouper table.")
                update_specialty = """UPDATE claims_grouper cp, claim_specialties cs
                                         SET cp.claim_specialty_id=cs.specialty_id
                                       WHERE cp.claim_id=cs.claim_id
                                         AND cp.imported_claim_file_id IN (%s)
                                        """ % (','.join(icf_ids_str))
                c.execute(update_specialty)
                
                logutil.log(self.logger, logutil.INFO, "Done augmenting claims_grouper table.")
        
def augment_claims_grouper(properties_file, db_name, table_name = None):
    method_input = """-p '%s' -d %s -t %s""" % (properties_file, db_name, table_name) if table_name else """-p '%s' -d %s""" % (properties_file, db_name)
    method_handler = ClaimsUtilFactory.get_instance('augment_claims_grouper', method_input)
    method_handler.process()

def yaml_formula_insert(stage_claims_table, value, table_alias=None):
    stage_claims_table_columns = stage_claims_table.columns()
    if not table_alias:
        table_alias = stage_claims_table.name
    if isinstance(value, types.DictType):
        formula = value.get('formula')
        if formula:
            for v in stage_claims_table_columns:
                formula = formula.replace(',%s' % v, ',%s.%s' % (table_alias, v)).replace(' %s' % v, ' %s.%s' % (table_alias, v)).replace('(%s' % v, '(%s.%s' % (table_alias, v))
            if formula == value.get('formula'):
                # Nothing has changed
                formula = "%s.%s" % (table_alias, formula)
                
            return formula
    else:
        value = "%s.%s" % (table_alias, value)
#            value = "%s" % (value)
        return value

def yaml_formula_insert_simple(stage_claims_table_name, stage_claims_table_columns, value, table_alias=None):
    
    if not value:
        return None
    
    if not table_alias:
        table_alias = stage_claims_table_name
    if isinstance(value, types.DictType):
        formula = value.get('formula')
        if formula:
            for v in stage_claims_table_columns:
                formula = formula.replace(',%s' % v, ',%s.%s' % (table_alias, v)).replace(' %s' % v, ' %s.%s' % (table_alias, v)).replace('(%s' % v, '(%s.%s' % (table_alias, v))
            if formula == value.get('formula'):
                # Nothing has changed
                formula = "%s.%s" % (table_alias, formula)
                
            return formula
    else:
        value = "%s.%s" % (table_alias, value)
#            value = "%s" % (value)
        return value
    
class RefreshClaimParticipations:
    """
    Buckets:
    There should already be a bucket_mappings entry for the (employer_id, insurance_company_id) tuple
    Insurance Network:
    Create entry in insurance_networks (insurance_company_id, name, type, external_network_id, external_type)
        external_network_id => bucket_id
        external_type => bucket
        name => bucket_<bucket_id>_network
        type => bucket_network
        insurance_company_id => insurance_company_id
    Create entry in insurance_plans_networks (insurance_p
    """
    
    LABS = {'quest':{'search':['%Quest%Diag%'],
                     'name':'Quest Diagnostics',
                     'state_order':['subscriber','npi']},
            'labcorp':{'search':['Labcorp%', 'Lab% Corp%'],
                       'name':'LabCorp aka Laboratory Corporation of America',
                       'state_order':['subscriber','npi']}}

    CLINICS = {'takecare':{'search_regex':['^Take Care (Health|Clinic)'],
                           'provider_id': 5300123,
                           'identifier_column':'provider_pin',
                           'use_npi_database':True,
                           'state_order':['npi']},
                 'minute':{'search_regex':['^Minute *Clinic'],
                           'provider_id': 10481655,
                           'identifier_column':'provider_pin',
                           'use_npi_database':True,
                           'state_order':['npi']},
                 'target':{'search_regex':['^Target Clinic'],
                           'provider_id': 10481656,
                           'identifier_column':'provider_pin',
                           'use_npi_database':True,
                           'state_order':['npi']}}
        
    def __init_options(self, input): 
        p = OptionParser(usage="""Usage: claims_util.py -m refresh_claim_participations
  -H, --Help                                              show this help message and exit
  -d DB_NAME, --db_name=DB_NAME                           name of claims master database
  -i INSURANCE_COMPANY_ID, --insurance_company_id         ID of the insurance company
  -e EMPLOYER_ID, --employer_id                           ID of the employer
  -t TIME_INTERVAL, --time_interval                       Time Interval of claims to consider (in days, defaults to 180 days)
  -f IMPORTED_CLAIM_FILE_IDS --imported_claim_file_ids    List of Imported Claim File IDs for augmentation
  [-m MODE] --mode=MODE                                   Mode. npi is the default mode.
  [-r True/False] --relink_only=True/False                Only Relink lab and clinic claims. Do nothing else.""")

        p.add_option("-d", "--db_name", type="string",
                      dest="db_name",
                      help="Name of claims master database.")
        p.add_option("-i", "--insurance_company_id", type="string",
                      dest="insurance_company_id",
                      help="Insurance Company ID.")              
        p.add_option("-e", "--employer_id", type="string",
                      dest="employer_id", 
                      help="Employer ID.")
        p.add_option("-t", "--time_interval", type="string",
                      dest="time_interval", default="180",
                      help="Time Interval of claims to consider (in days, defaults to 180 days).")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids", 
                      help="Imported Claim File IDs. For e.g. 1,3,7")
        p.add_option("-m", "--mode", type="string",
                      dest="mode", default='npi', 
                      help="Mode. e.g., npi")
        p.add_option("-r", "--relink_only",
                      type="string",
                      dest="relink_only",
                      default='False',
                      help="Only Relink lab and clinic claims. Do nothing else.")
        

        if not input:
            print p.usage
            sys.exit(2)
        (self.method_options, args) = p.parse_args(input.split(' '))
    
    def __init__(self, input, logger):
        
        self.logger = logger if logger else logutil.initlog('importer')
        
        self.method_options = None
        self.scratch_tables_created = set([])
        
        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)
        
        self.conn = getDBConnection(dbname = self.method_options.db_name,
                              host = whcfg.claims_master_host,
                              user = whcfg.claims_master_user,
                              passwd = whcfg.claims_master_password,
                              useDictCursor = True)

        self.master_conn = getDBConnection(dbname = whcfg.master_schema,
                              host = whcfg.master_host,
                              user = whcfg.master_user,
                              passwd = whcfg.master_password,
                              useDictCursor = True)
        
        self.imported_claim_file_ids = None
        self.insurance_company_id = None
        self.mode = self.method_options.mode
        self.state_locations_lab_keys = []
        self.state_locations_clinic_keys = []
        
        if self.method_options.imported_claim_file_ids and self.method_options.insurance_company_id:
            logutil.log(self.logger, logutil.ERROR, 'Only one of the options [insurance_company_id, imported_claim_file_ids] can be provided at a given time.')
        
        if self.method_options.imported_claim_file_ids:
            self.imported_claim_file_ids = [int(i) for i in self.method_options.imported_claim_file_ids.split(',')]
        elif self.method_options.insurance_company_id:
            self.insurance_company_id = int(self.method_options.insurance_company_id)

        self.master_loader_properties = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml','r'))
        self.insurance_company_properties = self.master_loader_properties['insurance_companies']['entries']


#    def __yaml_formula_insert(self, stage_claims_table, value, table_alias=None):
#        stage_claims_table_columns = stage_claims_table.columns()
#        if not table_alias:
#            table_alias = stage_claims_table.name
#        if isinstance(value, types.DictType):
#            formula = value.get('formula')
#            if formula:
#                for v in stage_claims_table_columns:
#                    formula = formula.replace(',%s' % v, ',%s.%s' % (table_alias, v)).replace(' %s' % v, ' %s.%s' % (table_alias, v)).replace('(%s' % v, '(%s.%s' % (table_alias, v))
#                if formula == value.get('formula'):
#                    # Nothing has changed
#                    formula = "%s.%s" % (table_alias, formula)
#                    
#                return formula
#        else:
#            value = "%s.%s" % (table_alias, value)
##            value = "%s" % (value)
#            return value

    def __refresh_claim_provider_identifiers(self, icf_ids, table_name, load_properties):

        t_table_name = dbutils.Table(self.conn, table_name)
        
        field_column_mappings = load_properties.get('field_column_mappings')
        
        d_claim_provider_identifiers = """DELETE FROM %s.claim_provider_identifiers WHERE imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, ','.join(icf_ids))
             
        c_claim_provider_identifiers = """INSERT INTO %s.claim_provider_identifiers (claim_id, imported_claim_file_id, imported_claim_id, insurance_company_id, employer_id, external_id, external_id_type,
                       out_of_network, 
                       service_begin_date)
                SELECT c.id as claim_id,
                       c.imported_claim_file_id,
                       c.imported_claim_id,
                       c.insurance_company_id,
                       c.employer_id,
                       %s as external_id,
                       %s as external_id_type,
                       c.out_of_network, 
                       c.service_begin_date
                FROM %s.claims c,
                     %s.%s ic
                WHERE c.imported_claim_file_id=ic.imported_claim_file_id
                  AND c.imported_claim_id=ic.id
                  AND ic.imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, 
                                                      yaml_formula_insert(t_table_name, field_column_mappings.get('provider_pin'), 'ic'), 
                                                      yaml_formula_insert(t_table_name, field_column_mappings.get('external_id_type'), 'ic') if field_column_mappings.get('external_id_type') != None else"'%s'" % load_properties.get('external_id_type'),
                                                      whcfg.claims_master_schema, 
                                                      whcfg.claims_master_schema,
                                                      table_name,
                                                      ','.join(icf_ids))            
         
        utils.execute_queries(self.conn, self.logger, [{'query':d_claim_provider_identifiers,
                                                        'description':'Deleting relevant entries from table claim_provider_identifiers.',
                                                        'warning_filter':'ignore'},
                                                       {'query':c_claim_provider_identifiers,
                                                        'description':'Inserting provider identifiers for icf_ids (%s).' % ','.join(icf_ids),
                                                        'warning_filter':'ignore'}])
        
    def __lookup_imported_claim_files(self, icf_ids, ic_id):
        t_icf = dbutils.Table(self.conn, 'imported_claim_files')
        if icf_ids:
            t_icf.search("""id in (%s) and load_properties is not null and claim_file_type = 'M'""" % ','.join([str(a) for a in icf_ids]))
        elif ic_id:
            t_icf.search("""id in (SELECT imported_claim_file_id FROM imported_claim_files_insurance_companies WHERE insurance_company_id=%s) and load_properties is not null and claim_file_type = 'M'""" % ic_id)
        table_icfid_map = t_icf.rows_to_dict_partitioned_by(t_icf[0:len(t_icf)], 'table_name', column_list = ['id', 'table_name','load_properties'])
        return table_icfid_map 
     
    def __refresh_claim_participations_generic(self, icf_ids, table_name, load_properties, external_id_type = 'NPI'):
        
        cur = self.conn.cursor()
        field_column_mappings = load_properties.get('field_column_mappings')
        
        # Creation of claim participations will require both the insurance company as well as the employer 
        # because the participations would have to be created using the bucket network
        cur.execute("""SELECT DISTINCT insurance_company_id, employer_id 
                       FROM %s.imported_claim_files icf
                       JOIN %s.imported_claim_files_insurance_companies icfic ON icf.id=icfic.imported_claim_file_id
                       WHERE icf.id IN (%s)
                    """ % (whcfg.claims_master_schema, whcfg.claims_master_schema, ','.join(icf_ids)))
        employers_insurance_companies = cur.fetchall()

        #for e in employers_insurance_companies:
        #    self.__resolve_network_id(e['insurance_company_id'], e['employer_id'])

        #We build a list of the insurance_company ids for which we will be augmenting the provider directory with CCP
        #we can't use self.insurance_company_id here becuase we might be doing this for more than one icid at the same time
        augment_icids = []
        for e in employers_insurance_companies:
            if self.insurance_company_properties[e['insurance_company_id']].get('augment_with_claim_participations')==1:
                augment_icids.append(e['insurance_company_id'])

        augment_icids_clause = '(%s)' % ','.join(map(str,augment_icids)) if augment_icids else ''

        # If self.insurance_company_id is populated, we only refresh claim participations for that insurance company
        # If self.insurance_company_id is null, we refresh claim participations (across all possible insurance companies) for the icf_ids passed
        table_name_hash = hashlib.sha1('_'.join(icf_ids)).hexdigest()
        claim_npis_table_name = 't_%s_claim_npis' % (table_name_hash)
        claim_providers_table_name = 't_%s_claim_providers' % (table_name_hash)
        claim_providers_table_name_3m = 't_%s_claim_providers_3m' % (table_name_hash)
        
        
        d_claim_npis = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema,
                                                            claim_npis_table_name)
        c_claim_npis = """CREATE TABLE %s.%s (INDEX ix_npi(npi))
                          SELECT DISTINCT external_id as npi
                            FROM %s.claim_provider_identifiers
                           WHERE imported_claim_file_id IN (%s)""" % (whcfg.scratch_schema,
                                                                    claim_npis_table_name,
                                                                    whcfg.claims_master_schema,
                                                                    ','.join(icf_ids))
                           
        d_claim_providers = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, claim_providers_table_name)
        c_claim_providers = """CREATE TABLE %s.%s 
                                        (provider_id INT(11) DEFAULT -1, 
                                         oon_ratio_cumulative DECIMAL(12,2), 
                                         oon_ratio_3m DECIMAL(12,2), 
                                         oon_count_total INT(11), 
                                         claim_count_total INT(11), 
                                         oon_count_3m INT(11), 
                                         claim_count_3m INT(11), 
                                         augment_provider_directory TINYINT(1),
                                         INDEX ix_npi_ic_id(npi, insurance_company_id))
                                 SELECT external_id as npi, 
                                        insurance_company_id,
                                        employer_id,
                                        max(service_begin_date) as most_recent_service_date, 
                                        sum(out_of_network) as oon_count_total,
                                        sum(out_of_network)/count(1) as oon_ratio_cumulative, 
                                        count(1) as claim_count_total
                                 FROM %s.claim_provider_identifiers
                                 WHERE imported_claim_file_id IN (%s) AND external_id_type %s 
                                 GROUP BY npi, insurance_company_id, employer_id""" % (whcfg.scratch_schema,
                                                    claim_providers_table_name,
                                                    whcfg.claims_master_schema,
                                                    ','.join(icf_ids),
                                                    "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type)
        
#        u_claim_providers = """UPDATE %s.%s tp,
#      %s.claim_provider_identifiers cpi
#SET tp.most_recent_oon_status=0
#WHERE tp.npi=cpi.external_id 
#AND cpi.external_id_type='NPI'
#AND tp.most_recent_service_date=cpi.service_begin_date
#AND cpi.insurance_company_id=
#AND cpi.out_of_network=0"""
         
        d_claim_providers_3m = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, claim_providers_table_name_3m)
        c_claim_providers_3m = """CREATE TABLE  %s.%s
                                        SELECT  n.npi, 
                                                n.insurance_company_id,
                                                n.employer_id,
                                                sum(IF(service_begin_date >= most_recent_service_date - INTERVAL 3 MONTH, out_of_network, 0))/sum(IF(service_begin_date >= most_recent_service_date - INTERVAL 3 MONTH, 1, 0)) as oon_ratio_3m,
                                                sum(IF(service_begin_date >= most_recent_service_date - INTERVAL 3 MONTH, out_of_network, 0)) oon_count_3m,
                                                sum(IF(service_begin_date >= most_recent_service_date - INTERVAL 3 MONTH, 1, 0)) claim_count_3m
                                                FROM %s.%s n
                                                JOIN %s.claim_provider_identifiers pi ON n.npi=pi.external_id AND n.insurance_company_id=pi.insurance_company_id AND n.employer_id=pi.employer_id
                                                WHERE pi.imported_claim_file_id IN (%s) AND pi.external_id_type %s 
                                                GROUP BY n.npi, n.insurance_company_id, n.employer_id""" % (whcfg.scratch_schema, claim_providers_table_name_3m,
                                                                                                            whcfg.scratch_schema, claim_providers_table_name,
                                                                                                            whcfg.claims_master_schema,
                                                                                                            ','.join(icf_ids),
                                                                                                            "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type)
        u_claim_providers_1 = """UPDATE %s.%s p,
                                      %s.%s p3
                                  SET p.oon_ratio_3m=p3.oon_ratio_3m, 
                                      p.claim_count_3m=p3.claim_count_3m, 
                                      p.oon_count_3m=p3.oon_count_3m
                                WHERE p.npi=p3.npi 
                                  AND p.insurance_company_id=p3.insurance_company_id
                                  AND p.employer_id=p3.employer_id""" % (whcfg.scratch_schema, claim_providers_table_name,
                                                                                           whcfg.scratch_schema, claim_providers_table_name_3m)
        
        u_claim_providers_2 = """UPDATE %s.%s 
                                    SET augment_provider_directory=1 
                                  WHERE oon_ratio_cumulative < 1 and oon_ratio_3m < 0.5""" % (whcfg.scratch_schema, claim_providers_table_name)

        d_plan_network_insurance_company_id_map = """DROP TABLE IF EXISTS %s.t_plan_network_insurance_company_id_map
                                                  """ % whcfg.scratch_schema
        
        c_plan_network_insurance_company_id_map = """CREATE TABLE %s.t_plan_network_insurance_company_id_map(INDEX ix_in(in_insurance_company_id), UNIQUE INDEX uq_np(in_insurance_company_id, ip_insurance_company_id)) 
                                                         AS SELECT * FROM %s.plan_network_insurance_company_id_map""" % (whcfg.scratch_schema,
                                                                                                                         whcfg.master_schema)
        i_plan_network_insurance_company_id_map = """INSERT IGNORE INTO %s.t_plan_network_insurance_company_id_map 
                                                     SELECT 21, ic.id
                                                       FROM %s.insurance_companies ic
                                                      WHERE ic.is_bcbs = 1
                                                    AND NOT EXISTS (SELECT 1 FROM %s.insurance_networks inet WHERE ic.id=inet.insurance_company_id AND inet.external_network_id='3046')
                                                        AND ic.id <> 21""" % (whcfg.scratch_schema, whcfg.master_schema, whcfg.master_schema)
        # Mark the providers that are already present and have active, non-bucket particiations 
        
        #To support the BCBS pseudo-insurance_company_id in the insurance_networks table, we need to change this.
        #tp.insurance_company_id is the claims insurance_company_id. Under the new model, a claim might legitimately
        #resolve to an in-network provider even though no insurance_networks.insurance_company_id for that provider's
        #participations matches claims.insurance_network_id. This happens for the Blues, where multiple Blues share insurance_networks.
        #
        #We can identify the an in-network provider for a claim under such circumstances in multiple ways. One would
        #be to join the pln table to insurance_plans_networks and insurance_plans. If there is at least one insurance_plan
        #that matches tp.insurance_company_id, we are good.
        #
        #doesn't this non-deterministically select a provider?
        u_claim_providers_3 = """UPDATE %s.%s tp
                                   JOIN %s.provider_external_ids pei ON tp.npi=pei.external_id 
                                   JOIN %s.providers_locations_networks pln ON pei.provider_id=pln.provider_id 
                                   JOIN %s.insurance_networks inet ON pln.network_id=inet.id
                                   JOIN %s.t_plan_network_insurance_company_id_map pnic 
                                     ON inet.insurance_company_id=pnic.in_insurance_company_id
                                    SET tp.provider_id=pln.provider_id, 
                                        tp.augment_provider_directory=0
                                  WHERE pln.active_flag='ACTIVE' 
                                    AND pei.external_id_type %s 
                                    AND pnic.ip_insurance_company_id=tp.insurance_company_id 
                                    AND inet.type is null""" % (whcfg.scratch_schema, claim_providers_table_name,
                                                                whcfg.master_schema,
                                                                whcfg.master_schema,
                                                                whcfg.master_schema,
                                                                whcfg.scratch_schema,
                                                                "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type)
        
        #When provider_id=-1 (we start out with all provider_id=-1, so these are the providers left over after query 3 above),
        #find an NPI PLN of type PRACTICE where the NPI matches. SET provider_id accordingly.
        u_claim_providers_4 = """UPDATE %s.%s tp
                                   JOIN %s.provider_external_ids pei ON tp.npi=pei.external_id 
                                   JOIN %s.providers_locations_networks pln ON pei.provider_id=pln.provider_id
                                    SET tp.provider_id=pln.provider_id
                                  WHERE tp.provider_id=-1
                                    -- AND tp.augment_provider_directory=1 
                                    AND pln.network_id=-8888 
                                    AND pln.active_flag='ACTIVE' 
                                    AND pei.external_id_type %s""" % (whcfg.scratch_schema, claim_providers_table_name,
                                                                         whcfg.master_schema,
                                                                         whcfg.master_schema,
                                                                         "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type)

        #inactivate all PLNs with the NPI in question for the relevant bucket network
        #
        # To inactivate we need to consider all bucket-network participations for providers that may be associated with
        # the NPIs in the claim_providers_table_name table, only for the relevant buckets.
        inactivate_pln = """UPDATE %s.providers_locations_networks pln 
                              JOIN %s.provider_external_ids pei ON pln.provider_id=pei.provider_id
                              JOIN %s.%s tp ON tp.npi=pei.external_id
                              JOIN %s.bucket_mappings bm ON bm.employer_id=tp.employer_id AND bm.insurance_company_id=tp.insurance_company_id
                              JOIN %s.insurance_networks inet ON CAST(bm.bucket_id as UNSIGNED)=inet.external_network_id AND inet.type='bucket'
                               SET pln.active_flag='INACTIVE'
                             WHERE pln.network_id=inet.id
                               AND pei.external_id_type %s""" % (whcfg.master_schema, 
                                                                whcfg.master_schema,
                                                                whcfg.scratch_schema, claim_providers_table_name,
                                                                whcfg.master_schema, 
                                                                whcfg.master_schema,
                                                                "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type)
                               
#        inactivate_pln = """UPDATE %s.providers_locations_networks pln 
#                              JOIN %s.%s tp ON tp.provider_id=pln.provider_id
#                              JOIN %s.insurance_networks inet ON pln.network_id=inet.id
#                               SET pln.active_flag='INACTIVE'
#                             WHERE inet.insurance_company_id=tp.insurance_company_id 
#                               AND inet.type='bucket'""" % (whcfg.scratch_schema, 
#                                                            whcfg.scratch_schema, claim_providers_table_name,
#                                                            whcfg.master_schema)
                               
#        inactivate_pln = """UPDATE %s.providers_locations_networks pln 
#                              JOIN %s.bucket_mappings bm ON bm.employer_id=tp.employer_id AND bm.insurance_company_id=tp.insurance_company_id
#                              JOIN %s.insurance_networks inet ON pln.network_id=inet.id
#                               SET pln.active_flag='INACTIVE'
#                             WHERE inet.insurance_company_id=tp.insurance_company_id 
#                               AND inet.type='bucket'""" % (whcfg.scratch_schema, 
#                                                            whcfg.scratch_schema, claim_providers_table_name,
#                                                            whcfg.master_schema)

        #add PLNs for the bucket network when an in-network participation was not found.
        # TODO: Need to add a step here to avoid creating participations for the labs that need participation refresh. e.g., quest, labcorp, takecare
        insert_pln = """INSERT INTO %s.providers_locations_networks
                               (source, 
                                active_flag, 
                                provider_id, 
                                location_id, 
                                network_id, 
                                contact_id, 
                                created_at, 
                                updated_at)
                        SELECT DISTINCT pei.external_id_type, 'ACTIVE', pln.provider_id, pln.location_id, inet.id, pln.contact_id, NOW(), NOW()
                          FROM %s.%s tp
                          JOIN %s.provider_external_ids pei ON tp.npi=pei.external_id 
                          JOIN %s.providers_locations_networks pln ON pei.provider_id=pln.provider_id
                          JOIN %s.bucket_mappings bm ON bm.employer_id=tp.employer_id AND bm.insurance_company_id=tp.insurance_company_id
                          JOIN %s.insurance_networks inet ON CAST(bm.bucket_id as UNSIGNED)=inet.external_network_id AND inet.type='bucket'  
                        WHERE tp.augment_provider_directory=1 AND pln.network_id=-8888 AND pln.active_flag='ACTIVE' AND pei.external_id_type %s
                        AND tp.insurance_company_id IN %s
                        ON DUPLICATE KEY UPDATE
                        active_flag='ACTIVE',
                        contact_id=pln.contact_id,
                        updated_at=NOW()""" % (whcfg.master_schema,
                                               whcfg.scratch_schema, claim_providers_table_name,
                                               whcfg.master_schema,
                                               whcfg.master_schema,
                                               whcfg.master_schema,
                                               whcfg.master_schema,
                                               "IN ('%s')" % "','".join(external_id_type) if isinstance(external_id_type,list) else "= '%s'" % external_id_type,
                                               augment_icids_clause)

        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':d_claim_npis,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_claim_npis,
                                                        'warning_filter':'ignore'},
                                                       {'query':d_claim_providers,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_claim_providers,
                                                        'warning_filter':'ignore'},
                                                       {'query':d_claim_providers_3m,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_claim_providers_3m,
                                                        'warning_filter':'ignore'}
                                                       ])

        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':u_claim_providers_1,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_claim_providers_2,
                                                        'warning_filter':'ignore'},
                                                       {'query':d_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'},
                                                       {'query':i_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_claim_providers_3, # Slow
                                                        'warning_filter':'ignore'},
                                                       {'query':u_claim_providers_4,
                                                        'warning_filter':'ignore'}
                                                       ])

        #Only do these last guys if we are actually adding claims-created participations for at least
        #one of the insurance_companies we are working with this run
        if augment_icids:
            utils.execute_queries(self.conn, self.logger, [
#                                                       {'query':inactivate_pln,
#                                                        'warning_filter':'ignore'},
                                                           {'query':insert_pln, # Slow
                                                            'warning_filter':'ignore'}
                                                           ])

        i_ocl = """INSERT IGNORE INTO  %s.original_claim_locations
                   SELECT c.id as claim_id,
                           c.provider_id,
                           p.provider_type,
                           c.provider_location_id,
                           l.*,
                           substr(l.match_key,1,5) as mk5
                    FROM %s.claims c,
                         %s.providers p,
                         %s.locations l
                    WHERE c.provider_id=p.id
                      AND c.provider_location_id=l.id
                      AND c.imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, whcfg.claims_master_schema, whcfg.master_schema, whcfg.master_schema, ','.join(icf_ids))
        
        u_clb = """UPDATE %s.claims SET provider_location_id=-1
                    WHERE imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, ','.join(icf_ids))

        u_c_p ="""UPDATE %s.claims c 
                    JOIN %s.claim_provider_identifiers cpi ON c.id=cpi.claim_id 
                    JOIN %s.%s tp ON cpi.external_id=tp.npi 
                     SET c.provider_id=tp.provider_id,
                     c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, 
                                                                 whcfg.claims_master_schema, 
                                                                 whcfg.scratch_schema, 
                                                                 claim_providers_table_name, 
                                                                 ','.join(icf_ids))
        
        d_c_d_p = """DROP TABLE IF EXISTS %s.d_c_p_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)
        
        c_c_d_p = """CREATE TABLE %s.d_c_p_%s (index ix_p(provider_id))
                     SELECT DISTINCT provider_id FROM %s.claims WHERE imported_claim_file_id IN (%s)""" % (whcfg.scratch_schema,
                                                                                                           table_name_hash,
                                                                                                           whcfg.claims_master_schema,
                                                                                                           ','.join(icf_ids))
        
        d_c_p_l = """DROP TABLE IF EXISTS %s.c_p_l_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)
        
        #Find all the PLs with PLNs for each P
        #TODO: Should this respect pln.active_flag?
        c_c_p_l = """CREATE TABLE %s.c_p_l_%s (mk5 VARCHAR(5), INDEX ix_p_z_mk5(provider_id, zip, mk5), INDEX ix_p_c(provider_id, city), INDEX ix_p_s(provider_id, state))
                        AS SELECT pln.provider_id, pln.location_id, l.zip, l.city, l.state, substr(l.match_key, 1, 5) as mk5
                        FROM 
                        %s.d_c_p_%s p 
                        JOIN %s.providers_locations_networks pln ON pln.provider_id=p.provider_id
                        JOIN %s.locations l ON pln.location_id=l.id
                        GROUP BY provider_id, location_id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.master_schema,
                               whcfg.master_schema)
        d_t_c_p_l = """DROP TABLE IF EXISTS %s.t_c_p_l_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)

        #Match the original_claim_location for each claim to the Ls for the P.
        #Match using mk5 and zip. Note that if there is more than one match using these
        #criteria for a given claim, an arbitrary L will be chosen, since we are grouping
        #by c.id and not putting an aggregate on pl.location_id.
        u_c_1_1 = """CREATE TABLE %s.t_c_p_l_%s (claim_id BIGINT(20) UNSIGNED NOT NULL PRIMARY KEY, provider_location_id INT(11))
                     AS SELECT c.id AS claim_id, pl.location_id as provider_location_id 
                        FROM %s.claims c
                        JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                        JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                        AND ocl.zip=pl.zip
                        AND ocl.mk5=pl.mk5
                        WHERE c.imported_claim_file_id IN (%s)
                        GROUP BY c.id
                        """ % (
                               whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_1_2 = """UPDATE %s.claims c
                        JOIN %s.t_c_p_l_%s tcl ON c.id=tcl.claim_id
                         SET c.provider_location_id=tcl.provider_location_id,
                         c.updated_at = NOW()
                       WHERE c.imported_claim_file_id IN (%s)""" %(whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_1 = """UPDATE %s.claims c
                        JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                        JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                        AND ocl.zip=pl.zip
                        AND ocl.mk5=pl.mk5
                        SET c.provider_location_id=pl.location_id,
                        c.updated_at = NOW()
                        WHERE c.imported_claim_file_id IN (%s)
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
        
        
        
        
        
#        u_c_1 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.id=clb.claim_id
#                      AND c.imported_claim_file_id IN (%s)
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.zip=l.zip
#                      AND substr(clb.match_key,1,5)=substr(l.match_key,1,5)""" % (whcfg.claims_master_schema,
#                                                                                  whcfg.claims_master_schema,
#                                                                                  whcfg.master_schema,
#                                                                                  whcfg.master_schema,
#                                                                                  ','.join(icf_ids))
        
        # If facility update claim location if zip matches valid participating location zip
        u_c_2_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.zip=pl.zip
                    WHERE c.imported_claim_file_id IN (%s)
                      AND ocl.provider_type='facility'
                     GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_2 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.zip=pl.zip
                      SET c.provider_location_id=pl.location_id,
                      c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND ocl.provider_type='facility'
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
        
        
#        u_c_2 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers p,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=p.id
#                      AND p.provider_type='facility'
#                      AND p.id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.zip=l.zip"""  % (whcfg.claims_master_schema,
#                                               whcfg.claims_master_schema,
#                                               whcfg.master_schema,
#                                               whcfg.master_schema,
#                                               whcfg.master_schema,
#                                               ','.join(icf_ids))


        # Update claim location if city matches valid participating location city
        u_c_3_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id 
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.city=pl.city
                    WHERE c.imported_claim_file_id IN (%s)
                    GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_3 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.city=pl.city
                      SET c.provider_location_id=pl.location_id,
                      c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
#        u_c_3 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.city=l.city""" % (whcfg.claims_master_schema,
#                                                whcfg.claims_master_schema,
#                                                whcfg.master_schema,
#                                                whcfg.master_schema,
#                                                ','.join(icf_ids))

        # Update claim location if state matches valid participating location state
        u_c_4_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id 
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.state=pl.state
                    WHERE c.imported_claim_file_id IN (%s)
                    GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
 
        u_c_4 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.state=pl.state
                      SET c.provider_location_id=pl.location_id,
                      c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
#        u_c_4 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.state=l.state""" % (whcfg.claims_master_schema,
#                                                 whcfg.claims_master_schema,
#                                                 whcfg.master_schema,
#                                                 whcfg.master_schema,
#                                                 ','.join(icf_ids))
     
        # Update claim location to a valid participating location
        u_c_5 = """UPDATE 
                    %s.claims c,
                    %s.providers_locations_networks pln
                    SET c.provider_location_id=pln.location_id,
                    c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND c.provider_id=pln.provider_id""" % (whcfg.claims_master_schema,
                                                              whcfg.master_schema,
                                                              ','.join(icf_ids))

        # Backfill any remaining locations with the original claims location
        u_c_6 = """UPDATE 
                    %s.claims c,
                    %s.original_claim_locations clb
                    SET c.provider_location_id=clb.provider_location_id,
                    c.updated_at = NOW()
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND c.id=clb.claim_id""" % (whcfg.claims_master_schema,
                                                  whcfg.claims_master_schema,
                                                  ','.join(icf_ids))


        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':u_c_p, 
                                                        'warning_filter':'ignore'},
                                                       {'query':i_ocl,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_clb,
                                                        'warning_filter':'ignore'},
                                                       {'query':d_c_d_p, 
                                                        'warning_filter':'ignore'},
                                                       {'query':c_c_d_p, 
                                                        'warning_filter':'ignore'},
                                                       {'query':d_c_p_l, 
                                                        'warning_filter':'ignore'},
                                                       {'query':c_c_p_l, 
                                                        'warning_filter':'ignore'},
						       {'query':d_t_c_p_l,
							'warning_filter':'ignore'},
                                                       {'query':u_c_1_1, # Very Slow
                                                        'warning_filter':'ignore'},
                                                 #      {'query':u_c_1, # Very Slow
                                                 #       'warning_filter':'ignore'},
                                                       {'query':u_c_2_1, # Slow
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_3_1,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_4_1,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_1_2, # Very Slow
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_5,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_6,
                                                        'warning_filter':'ignore'}
                                                       ], dry_run=False)
                      
    def __resolve_network_id(self, insurance_company_id, employer_id):
        # Use the network_id at the bucket, insurance_company level
        # Single bucket may be used across multiple insurance_companies
        # TODO: Usage of buckets for restricting participations created from claims belonging to one bucket  
        # from being used to show priced/unpriced participations to a user that has access to a different 
        # bucket need to fully be implemented. 
        # The details of how this would be implemented have been hashed out and are available in the 
        # buckets presentation document.
        #
#        if not self.insurance_network_id:
            
        bucket_id = None
        bucket_name = None
        insurance_company = None
        bucket_mapping = """SELECT bm.bucket_id, b.name, ic.name as insurance_company_name FROM %s.bucket_mappings bm, %s.buckets b, %s.insurance_companies ic
                                     WHERE bm.insurance_company_id = ic.id
                                       AND bm.employer_id = %s
                                       AND b.id=bm.bucket_id
                                       AND ic.id = %s""" %(whcfg.master_schema,
                                                           whcfg.master_schema,
                                                           whcfg.master_schema,
                                                           employer_id,
                                                           insurance_company_id
                                                           )
        q_bucket_mapping = Query(self.conn, bucket_mapping)
        if q_bucket_mapping:
            bucket = q_bucket_mapping.next()
            bucket_id = bucket['bucket_id']
            bucket_name = bucket['name']
            insurance_company = bucket['insurance_company_name']
            
        if not bucket_id:
            # Raise and error here
            logutil.log(self.logger, logutil.CRITICAL, 'No bucket/bucket_mapping entry for employer_id: %s and insurance_company_id: %s' % (employer_id,
                                                                                                                                            insurance_company_id))
            raise MasterLoaderException(MasterLoaderException.VALIDATION_ERROR, 'No bucket/bucket_mapping entry for employer_id: %s and insurance_company_id: %s' % (employer_id,
                                                                                                                                                                     insurance_company_id))
            
#            TODO: Re-enable this code once the final bucket-network proposal has been finalized and accepted for implementation
#            bucket_network = """SELECT bn.insurance_network_id 
#                                  FROM %s.bucket_networks bn,
#                                       %s.insurance_networks n
#                                 WHERE bn.bucket_id = %s
#                                   AND bn.insurance_network_id = n.id
#                                   AND n.insurance_company_id = %s
#                                   AND n.active_flag='ACTIVE'""" % (whcfg.master_schema,
#                                                                       whcfg.master_schema,
#                                                                       bucket_id,
#                                                                       self.method_options.insurance_company_id)
#            q_bucket_network = Query(self.conn, bucket_network)
#            if q_bucket_network:
#                insurance_network_id = q_bucket_network.next()

#           TODO: Disable bucket insurance_network_id querying logic below once the above logic is enabled.
        fac_insurance_networks = ModelFactory.get_instance(self.conn, '%s.insurance_networks' % whcfg.master_schema)
        n_entry = {'insurance_company_id':insurance_company_id,
                   'external_network_id': bucket_id,
                   'external_type':'bucket',
                   'active_flag':'ACTIVE'}
        n_entry = fac_insurance_networks.find(n_entry)
        
        if not n_entry:
            # Create bucket_network entry
            n_entry = {'insurance_company_id':insurance_company_id,
                       'name':'Bucket: %s' % bucket_name,
                       'type':'bucket',
                       'external_network_id': bucket_id,
                       'external_type':'bucket',
                       'active_flag':'ACTIVE'
                       }
            n_entry = fac_insurance_networks.find_or_create(n_entry)
        
        insurance_network_id = n_entry['id']    
        # We need to make sure the external_plan_network_mapping_code value for 
        # BCBSMA plans in pantry are created with the value 'BCBSMA'
        
        # Make sure that BCBSMA has been created in the Provider Directory insurance_plans table
        ip_entry = {'insurance_company_id': insurance_company_id,
                    'product_code': insurance_company.upper(),
                    'active_flag': 'ACTIVE'
                    }
        fac_insurance_plans = ModelFactory.get_instance(self.conn, '%s.insurance_plans' % whcfg.master_schema)
        ip_entry = fac_insurance_plans.find_or_create(ip_entry)
        
        ipn_entry = {'insurance_plan_id':ip_entry['id'],
                     'insurance_network_id':n_entry['id']
                     }
        # Add insurance_network to insurance_plans_networks table
        fac_insurance_plans_networks = ModelFactory.get_instance(self.conn, '%s.insurance_plans_networks' % whcfg.master_schema)
        ipn_entry = fac_insurance_plans_networks.find_or_create(ipn_entry)
        
        return insurance_network_id
            
    def process(self):
        
        logutil.log(self.logger, logutil.INFO, 'RefreshClaimParticipations.process(). Mode = %s' % self.mode)
        relink_only = (self.method_options.relink_only == 'True')
        
        if relink_only:
            table_icfid_map = self.__lookup_imported_claim_files(self.imported_claim_file_ids, self.insurance_company_id)
            for table_name, icf_id_map_list in table_icfid_map.iteritems():
                icf_ids = [str(s_id) for s_id in sorted([id.get('id') for id in icf_id_map_list])]
                #  icf_ids = [str(s_id) for s_id in self.imported_claim_file_ids] 
                self.__relink_lab_and_clinic_claims(icf_ids)
        
#         elif self.mode == 'npi':
        elif self.mode:    
#            if self.imported_claim_file_ids:
            table_icfid_map = self.__lookup_imported_claim_files(self.imported_claim_file_ids, self.insurance_company_id)
            for table_name, icf_id_map_list in table_icfid_map.iteritems():
                icf_ids = [str(s_id) for s_id in sorted([id.get('id') for id in icf_id_map_list])]
#                pprint.pprint(icf_id_map_list)
                load_properties = yaml.load(icf_id_map_list[0].get('load_properties'))
                
                # This first step of augmenting the claim_provider_identifiers table is agnstic of insurance company
                self.__refresh_claim_provider_identifiers(icf_ids, table_name, load_properties)
                
                self.__refresh_claim_participations_generic(icf_ids, table_name, load_properties,load_properties.get('external_id_type') if load_properties.get('external_id_type') !=None else self.mode)
                
                # TODO: Should we also make sure that there are no extraneous participations 
                # Created by the claim participations step for the labs of interest?
                self.__relink_lab_and_clinic_claims(icf_ids) 
                
                self.__apply_overrides(icf_ids)
                
        dbutils.close_connections([self.conn, self.master_conn])
    
    def __apply_overrides(self, icf_ids):
        if self.insurance_company_id == 5:
            # Quest override
            q_apply_overrides = """UPDATE %s.claims c
                                   JOIN   %s.locations l ON c.provider_location_id=l.id
                                   SET    provider_id=1078193,
                                          provider_location_id=6141
                                   WHERE  insurance_company_id = 5
                                   AND    imported_claim_file_id IN (%s) 
                                   AND    provider_id=3488157
                                   AND    l.state='MD'""" % (whcfg.claims_master_schema, whcfg.master_schema, ','.join(icf_ids))
            
            self.conn.cursor().execute(q_apply_overrides)
            
        #prevent certain OON Q and LC participations from being created -- by inactivating them
        q_inactivate_oon_quest = """
        UPDATE {master}.providers_locations_networks pln 
        JOIN   {master}.insurance_networks inet 
        ON     inet.id=pln.network_id
        AND    insurance_company_id<11
        AND    inet.type='bucket'
        JOIN   {master}.insurance_companies ic 
        ON     ic.id=inet.insurance_company_id 
        AND    ic.is_bcbs
        JOIN   imported_claim_files_insurance_companies icfic
        ON     icfic.insurance_company_id=ic.id
        AND    icfic.imported_claim_file_id IN ({icfids})
        JOIN   {master}.locations l 
        ON     l.id = pln.location_id 
        JOIN   (SELECT * FROM {master}.providers WHERE display_name like 'quest diag%') p 
        ON     p.id = pln.provider_id 
        SET    pln.active_flag='INACTIVE'
        WHERE  l.state IN ('NJ','CO','NV') 
        AND    pln.active_flag='ACTIVE'
        """.format(master=whcfg.master_schema,
                   icfids=','.join(icf_ids))

        self.conn.cursor().execute(q_inactivate_oon_quest)

        q_inactivate_oon_labcorp = """
        UPDATE {master}.providers_locations_networks pln 
        JOIN   {master}.insurance_networks inet 
        ON     inet.id=pln.network_id
        AND    insurance_company_id<11
        AND    inet.type='bucket'
        JOIN   {master}.insurance_companies ic 
        ON     ic.id=inet.insurance_company_id 
        AND    ic.is_bcbs
        JOIN   imported_claim_files_insurance_companies icfic
        ON     icfic.insurance_company_id=ic.id
        AND    icfic.imported_claim_file_id IN ({icfids})
        JOIN   {master}.locations l 
        ON     l.id = pln.location_id 
        JOIN   (SELECT * FROM {master}.providers WHERE display_name LIKE 'lab corp%' OR display_name LIKE 'labcorp%' or display_name LIKE 'laboratory corp%') p 
        ON     p.id = pln.provider_id 
        SET    pln.active_flag='INACTIVE'
        WHERE  l.state IN ('FL') 
        AND    pln.active_flag='ACTIVE'
        """.format(master=whcfg.master_schema,
                   icfids=','.join(icf_ids))

        self.conn.cursor().execute(q_inactivate_oon_labcorp)

    def __relink_lab_and_clinic_claims(self, icf_ids):
        print "Relink Lab and Clinic Claims: icf_ids (%s)" % ','.join(icf_ids)
        self.__create_zip3_state_table() 
        q_icf_details = """SELECT icf.id as imported_claim_file_id,
                                  icfic.insurance_company_id,
                                  ic.name as insurance_company_name,
                                  icf.employer_id,
                                  icf.table_name,
                                  icf.load_properties
                             FROM %s.imported_claim_files icf
                             JOIN %s.imported_claim_files_insurance_companies icfic ON icf.id=icfic.imported_claim_file_id
                             JOIN %s.insurance_companies ic ON icfic.insurance_company_id=ic.id
                            WHERE icf.id IN (%s)""" % (whcfg.claims_master_schema, 
                                                       whcfg.claims_master_schema, 
                                                       whcfg.master_schema,
                                                       ','.join(icf_ids)) 
                            
        r_icf_details = Query(self.conn, q_icf_details)
        
        for icf in r_icf_details:
            
            master_loader_properties = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml','r'))
            icf_id = icf['imported_claim_file_id']
            insurance_company_id = icf['insurance_company_id']
            insurance_company_name = icf['insurance_company_name']
            employer_id = icf['employer_id']
            load_properties = icf['load_properties']
            table_name = icf['table_name']
            
            if master_loader_properties:
                lab_keys = set(master_loader_properties.get('insurance_companies',{}).get('entries',{}).get(insurance_company_id,{}).get('relink_lab_claims',[]))
                clinic_keys = set(master_loader_properties.get('insurance_companies',{}).get('entries',{}).get(insurance_company_id,{}).get('relink_clinic_claims',[]))
                is_blue = int(master_loader_properties.get('insurance_companies',{}).get('entries',{}).get(insurance_company_id,{}).get('is_bcbs',0))
                if lab_keys or clinic_keys:
                    if not load_properties and insurance_company_name.lower() in claims_load_helper.FIELD_MAPPINGS and relink_only:
                        load_properties = yaml.dump({'field_column_mappings':claims_load_helper.FIELD_MAPPINGS.get(insurance_company_name.lower()),
                                                     'external_id_type':'npi'})
                    logutil.log(self.logger, logutil.INFO, 'Relinking Lab and Clinic Claims. imported_claim_file_id=%d' % icf_id)
                    self.relink_lab_and_clinic_claims(icf_id, insurance_company_id, employer_id, table_name, yaml.load(load_properties), lab_keys, clinic_keys, is_blue)

    def __create_zip3_state_table(self):
        cur = self.conn.cursor()
        
        d_zip3_state = """DROP TABLE IF EXISTS %s.zip3_state""" % whcfg.scratch_schema
        try:
            cur.execute(d_zip3_state) 
        except:
           pass
       
        c_zip3_state = """CREATE TABLE %s.zip3_state (zip3 VARCHAR(3), state VARCHAR(2), INDEX ix_z3(zip3)) AS
                          SELECT DISTINCT SUBSTRING(ZipCode,1,3) as zip3, 
                                          State as state 
                                     FROM zipcodes.zipcodes_deluxe_business""" % (whcfg.scratch_schema)
        cur.execute(c_zip3_state)
        
        d_zip3_collissions = """DROP TABLE IF EXISTS %s.t_zip3_collissions""" % whcfg.scratch_schema
        try:
            cur.execute(d_zip3_collissions) 
        except:
            pass
        
        t_zip3_collissions = """CREATE TABLE %s.t_zip3_collissions AS SELECT DISTINCT z1.zip3 
                            FROM %s.zip3_state z1, 
                                 %s.zip3_state z2
                            WHERE z1.zip3=z2.zip3
                            AND z1.state <> z2.state""" % (whcfg.scratch_schema, whcfg.scratch_schema, whcfg.scratch_schema)
        cur.execute(t_zip3_collissions)
        
        d_zip3_collissions = """DELETE FROM %s.zip3_state WHERE zip3 IN (SELECT t.zip3 FROM %s.t_zip3_collissions t)""" % (whcfg.scratch_schema, whcfg.scratch_schema)
        cur.execute(d_zip3_collissions)
        cur.close()

    def __generate_lab_state_locations_table(self, lab_key, insurance_company_id, q_lab_state_locations_table):
        
        lab_name = RefreshClaimParticipations.LABS.get(lab_key,{}).get('name')
        
        d_plan_network_insurance_company_id_map = """DROP TABLE IF EXISTS %s.t_plan_network_insurance_company_id_map
                                                  """ % whcfg.scratch_schema
        
        c_plan_network_insurance_company_id_map = """CREATE TABLE %s.t_plan_network_insurance_company_id_map(INDEX ix_in(in_insurance_company_id)) 
                                                         AS SELECT * FROM %s.plan_network_insurance_company_id_map""" % (whcfg.scratch_schema,
                                                                                                                         whcfg.master_schema)
        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':d_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'}
                                                       ])
        q_provider_id = """SELECT p.id as provider_id
                            FROM %s.providers p,
                                 %s.providers_locations_networks pln,
                                 %s.insurance_networks inet,
                                 %s.t_plan_network_insurance_company_id_map pnic
                            WHERE p.display_name = '%s' 
                              AND p.id = pln.provider_id
                              AND pln.network_id = inet.id
                              AND p.active_flag='ACTIVE'
                              AND pln.active_flag = 'ACTIVE'
                              AND inet.`insurance_company_id`=pnic.`in_insurance_company_id`
                              AND pnic.`ip_insurance_company_id`= %d
                              AND inet.type IS NULL
                            GROUP BY p.id
                            ORDER BY count(pln.location_id) desc
                            LIMIT 1""" % (whcfg.master_schema,
                                          whcfg.master_schema,
                                          whcfg.master_schema,
                                          whcfg.scratch_schema,
                                          lab_name,
                                          insurance_company_id
                                          ) 
        r_provider_id = Query(self.conn, q_provider_id)
        if r_provider_id:
            quest_provider = r_provider_id.next()
            quest_provider_id = quest_provider['provider_id'] 
            
            cur = self.conn.cursor()
            
            d_q_state_locations = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, q_lab_state_locations_table)
            try:
                cur.execute(d_q_state_locations)
            except:
                pass
            
            c_q_state_locations = """CREATE TABLE %s.%s (INDEX ix_state(state))
                                        SELECT p.id, l.state, min(l.id) as location_id 
                                        FROM 
                                        %s.providers p,
                                        %s.providers_locations_networks pln,
                                        %s.insurance_networks inet ,
                                        %s.locations l
                                        WHERE p.id=pln.provider_id
                                        AND network_id=inet.id 
                                        AND inet.insurance_company_id=%d
                                        AND pln.location_id=l.id
                                        AND p.id=%s
                                        AND p.active_flag='ACTIVE'
                                        AND pln.active_flag = 'ACTIVE'
                                        GROUP BY p.id, l.state""" % (whcfg.scratch_schema,
                                                                     q_lab_state_locations_table,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     insurance_company_id,
                                                                     quest_provider_id)
            cur.execute(c_q_state_locations)

    def __generate_clinic_state_locations_table(self, clinic_key, insurance_company_id, q_clinic_state_locations_table):
        
        clinic_pid = RefreshClaimParticipations.CLINICS.get(clinic_key,{}).get('provider_id')
        
        d_plan_network_insurance_company_id_map = """DROP TABLE IF EXISTS %s.t_plan_network_insurance_company_id_map
                                                  """ % whcfg.scratch_schema
        
        c_plan_network_insurance_company_id_map = """CREATE TABLE %s.t_plan_network_insurance_company_id_map(INDEX ix_in(in_insurance_company_id)) 
                                                         AS SELECT * FROM %s.plan_network_insurance_company_id_map""" % (whcfg.scratch_schema,
                                                                                                                         whcfg.master_schema)
        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':d_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'},
                                                       {'query':c_plan_network_insurance_company_id_map,
                                                        'warning_filter':'ignore'}
                                                       ])
        q_provider_id = """SELECT p.id as provider_id
                            FROM %s.providers p,
                                 %s.providers_locations_networks pln,
                                 %s.insurance_networks inet,
                                 %s.t_plan_network_insurance_company_id_map pnic
                            WHERE p.id = %s
                              AND p.id = pln.provider_id
                              AND pln.network_id = inet.id
                              AND pln.active_flag = 'ACTIVE'
                              AND inet.`insurance_company_id`=pnic.`in_insurance_company_id`
                              AND pnic.`ip_insurance_company_id` = %d
                              AND inet.type IS NULL"""% (whcfg.master_schema,
                                                          whcfg.master_schema,
                                                          whcfg.master_schema,
                                                          whcfg.scratch_schema,
                                                          clinic_pid,
                                                          insurance_company_id) 
        r_provider_id = Query(self.conn, q_provider_id)
        if r_provider_id:
            clinic_provider = r_provider_id.next()
            clinic_provider_id = clinic_provider['provider_id'] 
            
            cur = self.conn.cursor()
            
            d_q_state_locations = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, q_clinic_state_locations_table)
            try:
                cur.execute(d_q_state_locations)
            except:
                pass
            
            c_q_state_locations = """CREATE TABLE %s.%s (INDEX ix_state(state))
                                        SELECT p.id, l.state, min(l.id) as location_id 
                                        FROM 
                                        %s.providers p,
                                        %s.providers_locations_networks pln,
                                        %s.insurance_networks inet ,
                                        %s.locations l
                                        WHERE p.id=pln.provider_id
                                        AND network_id=inet.id 
                                        AND inet.insurance_company_id=%d
                                        AND pln.location_id=l.id
                                        AND p.id=%s
                                        AND pln.active_flag = 'ACTIVE'
                                        GROUP BY p.id, l.state""" % (whcfg.scratch_schema,
                                                                     q_clinic_state_locations_table,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     whcfg.master_schema,
                                                                     insurance_company_id,
                                                                     clinic_provider_id)
            cur.execute(c_q_state_locations)
                    
    def relink_lab_and_clinic_claims(self, icf_id, insurance_company_id, employer_id, table_name, load_properties, lab_keys, clinic_keys, is_blue):
        
        quest_provider_id = None
        
        fcm = load_properties.get('field_column_mappings',{})
        
        raw_claims_map = {}
        
        raw_claims_map['clm_provider_state'] = {'formula':fcm.get('state')} if fcm.get('state') and not isinstance(fcm.get('state'), types.DictType) else fcm.get('state')
        raw_claims_map['clm_provider_zip3'] = {'formula':'SUBSTRING(%s,1,3)' % fcm.get('zip')} if fcm.get('zip') and not isinstance(fcm.get('zip'), types.DictType) else  {'formula':'SUBSTRING(%s,1,3)' % fcm.get('zip').get('formula')}
        
        raw_claims_map['clm_sub_state'] = None
        if fcm.get('employee_state'):
            if isinstance(fcm.get('employee_state'), types.DictType):
                raw_claims_map['clm_sub_state'] = fcm.get('employee_state')
            else:
                raw_claims_map['clm_sub_state'] = {'formula':fcm.get('employee_state')}
                
        raw_claims_map['clm_sub_zip3'] = None
        if fcm.get('employee_zip_code'):
            if isinstance(fcm.get('employee_zip_code'), types.DictType):
                raw_claims_map['clm_sub_zip3'] = {'formula': 'SUBSTRING(%s,1,3)' % fcm.get('employee_zip_code').get('formula')}
            else:
                raw_claims_map['clm_sub_zip3'] = {'formula': 'SUBSTRING(%s,1,3)' % fcm.get('employee_zip_code')}
        
        raw_claims_map['clm_mem_state'] = None
        if fcm.get('member_state'):
            if isinstance(fcm.get('member_state'), types.DictType):
                raw_claims_map['clm_mem_state'] = fcm.get('member_state')
            else:
                raw_claims_map['clm_mem_state'] = {'formula':fcm.get('member_state')}
                
        raw_claims_map['clm_mem_zip3'] = None
        if fcm.get('member_zip_code'):
            if isinstance(fcm.get('member_zip_code'), types.DictType):
                raw_claims_map['clm_mem_zip3'] = {'formula': 'SUBSTRING(%s,1,3)' % fcm.get('member_zip_code').get('formula')}
            else:
                raw_claims_map['clm_mem_zip3'] = {'formula': 'SUBSTRING(%s,1,3)' % fcm.get('member_zip_code')}

        provider_name_column = {'formula':fcm.get('provider_name')} if fcm.get('provider_name') and not isinstance(fcm.get('provider_name'), types.DictType) else fcm.get('provider_name')
            
            
        t_table_name = dbutils.Table(self.conn, table_name)
        
        for lab_key in lab_keys:
            
            logutil.log(self.logger, logutil.INFO, 'Relinking Lab Claims for %s.' % lab_key)
            q_lab_state_locations_table = '%s_lab_state_locations' % lab_key
            
            if lab_key not in self.state_locations_lab_keys:
                self.__generate_lab_state_locations_table(lab_key, insurance_company_id, q_lab_state_locations_table)
                self.state_locations_lab_keys.append(lab_key)
            
            q_lab_claims_table = '%s_lab_claims_%s' % (lab_key, icf_id)
            
            
            state_order = RefreshClaimParticipations.LABS.get(lab_key,{}).get('state_order', ['subscriber', 'npi'])
            
#            print fcm.get('provider_pin')
#            print load_properties.get('external_id_type', '')
            
            npi_column = fcm.get('provider_pin') if ('npi' in state_order and load_properties.get('external_id_type', '').lower() == 'npi') else None 
            if not npi_column: 
                logutil.log(self.logger, logutil.WARNING, 'Relinking Lab Claims for %s.' % lab_key)
                return
            else:
                raw_claims_map['npi'] = fcm.get('provider_pin')            
            
            
            name_like_phrase = ' OR '.join(["%s LIKE '%s'" % ('c.provider_name', s) for s in RefreshClaimParticipations.LABS.get(lab_key,{}).get('search',[])])
            name_regex_phrase = ' OR '.join(["%s REGEXP '%s'" % ('c.provider_name', s) for s in RefreshClaimParticipations.LABS.get(lab_key,{}).get('search_regex',[])])
            name_phrase = name_like_phrase + name_regex_phrase
            
            query_list = []
            
            d_q_lab_claims = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, q_lab_claims_table)
            query_list.append({'query':d_q_lab_claims,
                               'description':'Dropping table %s.%s if it exists.' % (whcfg.scratch_schema, q_lab_claims_table),
                               'warning_filter':'ignore'})
            
            c_q_lab_claims = """CREATE TABLE %s.%s 
                                            (provider_id INT(11) DEFAULT -1,
                                             provider_location_id INT(11) DEFAULT -1,
                                             elig_sub_state VARCHAR(2),
                                             elig_sub_zip3 VARCHAR(3),
                                             elig_mem_state VARCHAR(2),
                                             elig_mem_zip3 VARCHAR(3),
                                             clm_sub_state VARCHAR(2),
                                             clm_sub_zip3 VARCHAR(3),
                                             clm_mem_state VARCHAR(2),
                                             clm_mem_zip3 VARCHAR(3),
                                             clm_provider_state VARCHAR(2),
                                             clm_provider_zip3 VARCHAR(3),
                                             npi_provider_state VARCHAR(2),
                                             npi_provider_zip3 VARCHAR(3),
                                             state VARCHAR(2),
                                             npi VARCHAR(40),
                                             tax_id VARCHAR(40),
                                            INDEX ix_icf_ic(imported_claim_file_id, imported_claim_id),
                                            INDEX ix_c(claim_id),
                                            INDEX ix_p(patient_id))
                                             AS SELECT c.id as claim_id,
                                                       c.imported_claim_id,
                                                       c.imported_claim_file_id,
                                                       c.insurance_company_id,
                                                       c.patient_id,
                                                       c.provider_name
                                            FROM %s.claims c
                                            WHERE c.imported_claim_file_id=%s
                                            AND (%s)""" % (whcfg.scratch_schema, q_lab_claims_table, whcfg.claims_master_schema, icf_id, name_phrase)
            
            query_list.append({'query':c_q_lab_claims,
                               'description':'Creating table %s.%s.' % (whcfg.scratch_schema, q_lab_claims_table),
                               'warning_filter':'ignore'})
            
            set_insert = ', '.join(["qc.%s = %s" % (k,yaml_formula_insert(t_table_name, v, 'ic')) for k,v in raw_claims_map.iteritems() if v]) if ('subscriber' in state_order or 'provider' in state_order) else 'qc.npi = %s' % (yaml_formula_insert(t_table_name, raw_claims_map.get('npi'), 'ic'))
            if set_insert:
                u_q_lab_claims = """UPDATE %s.%s qc
                                      JOIN %s.%s ic 
                                        ON qc.imported_claim_file_id = ic.imported_claim_file_id AND qc.imported_claim_id=ic.id
                                       SET %s""" % (whcfg.scratch_schema, q_lab_claims_table, whcfg.claims_master_schema, table_name, set_insert)
                query_list.append({'query':u_q_lab_claims,
                                   'description':'Update table %s.%s with state/zip data from raw claims.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})

                u_q_lab_state = """UPDATE %s.%s qc 
                                       JOIN %s.zip3_state zs ON IFNULL(clm_mem_zip3,clm_sub_zip3)=zs.zip3
                                        SET qc.state = zs.state
                                      WHERE qc.state is null""" % (whcfg.scratch_schema,
                                                                   q_lab_claims_table,
                                                                   whcfg.scratch_schema)            
                query_list.append({'query':u_q_lab_state,
                                   'description':'Update table %s.%s with state data from raw claims.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})

            if 'subscriber' in state_order:
                u_q_lab_elig_sub = """UPDATE %s.%s qc
                                    JOIN %s.policy_coverages pc ON qc.patient_id=pc.patient_id
                                    JOIN %s.policies pl ON pc.policy_id=pl.id
                                    JOIN %s.patients p ON pl.subscriber_patient_id=p.id
                                    SET qc.elig_sub_state=p.state, qc.elig_sub_zip3=SUBSTRING(p.zip,1,3)""" % (whcfg.scratch_schema,
                                                                                                               q_lab_claims_table,
                                                                                                               whcfg.claims_master_schema,
                                                                                                               whcfg.claims_master_schema,
                                                                                                               whcfg.claims_master_schema,)
                
                query_list.append({'query':u_q_lab_elig_sub,
                                   'description':'Update table %s.%s with subscriber state/zip data from eligibility data.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_lab_elig_mem = """UPDATE %s.%s qc
                                        JOIN %s.patients p ON qc.patient_id=p.id
                                         SET qc.elig_mem_state=p.state, qc.elig_mem_zip3=SUBSTRING(p.zip,1,3)""" % (whcfg.scratch_schema,
                                                                                                                    q_lab_claims_table,
                                                                                                                    whcfg.claims_master_schema)

                query_list.append({'query':u_q_lab_elig_mem,
                                   'description':'Update table %s.%s with member state/zip data from eligibility data.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
               
                u_q_lab_state_1 = """UPDATE %s.%s SET state = IFNULL(elig_mem_state,elig_sub_state) WHERE state is null""" % (whcfg.scratch_schema,
                                                                                                          q_lab_claims_table)
                
                query_list.append({'query':u_q_lab_state_1,
                                   'description':'Update table %s.%s for state: step 1.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_lab_state_2 = """UPDATE %s.%s qc 
                                       JOIN %s.zip3_state zs ON IFNULL(elig_mem_zip3,elig_sub_zip3)=zs.zip3
                                        SET qc.state = zs.state
                                      WHERE qc.state is null""" % (whcfg.scratch_schema,
                                                                   q_lab_claims_table,
                                                                   whcfg.scratch_schema)

                query_list.append({'query':u_q_lab_state_2,
                                   'description':'Update table %s.%s for state: step 2.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
            
            if 'npi' in state_order:
                u_q_lab_npi = """UPDATE %s.%s qc
                                   JOIN %s.npi_raw nr ON qc.npi=nr.npi
                                    SET qc.npi_provider_state=Provider_Business_Practice_Location_Address_State_Name,
                                        qc.npi_provider_zip3=SUBSTRING(Provider_Business_Practice_Location_Address_Postal_Code,1,3)""" % (whcfg.scratch_schema, q_lab_claims_table, whcfg.npi_schema)

                query_list.append({'query':u_q_lab_npi,
                                   'description':'Update table %s.%s with provider state/zip data from npi.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_lab_state_3 = """UPDATE %s.%s SET state = npi_provider_state 
                                      WHERE state is null 
                                         OR state not in (SELECT DISTINCT state FROM %s.%s)"""  % (whcfg.scratch_schema, q_lab_claims_table,
                                                                                                   whcfg.scratch_schema, q_lab_state_locations_table)
                query_list.append({'query':u_q_lab_state_3,
                                   'description':'Update table %s.%s for state: step 3.' % (whcfg.scratch_schema, q_lab_claims_table),
                                   'warning_filter':'ignore'})
        
            u_q_lab_p_l = """UPDATE %s.%s qc
                           JOIN %s.%s qs ON qc.state=qs.state
                            SET qc.provider_id=qs.id, qc.provider_location_id=qs.location_id""" % (whcfg.scratch_schema, q_lab_claims_table,
                                                                                                   whcfg.scratch_schema, q_lab_state_locations_table)
            query_list.append({'query':u_q_lab_p_l,
                           'description':'Update table %s.%s for provider_id, provider_location_id.' % (whcfg.scratch_schema, q_lab_claims_table),
                           'warning_filter':'ignore'})
        
            u_claims = """UPDATE %s.claims c
                        JOIN %s.%s qc ON c.id=qc.claim_id
                         SET c.provider_id=qc.provider_id, c.provider_location_id=qc.provider_location_id
                       WHERE qc.provider_id > -1 AND qc.provider_location_id > -1""" % (whcfg.claims_master_schema, whcfg.scratch_schema, q_lab_claims_table)
        
            query_list.append({'query':u_claims,
                           'description':'Update table %s.claims for provider_id, provider_location_id.' % (whcfg.claims_master_schema),
                           'warning_filter':'ignore'})
                
            utils.execute_queries(self.conn, self.logger, query_list, dry_run = False)   

        
        for clinic_key in clinic_keys:
                
            logutil.log(self.logger, logutil.INFO, 'Relinking Clinic Claims for %s.' % clinic_key)
            q_clinic_state_locations_table = '%s_clinic_state_locations' % clinic_key
            
            if clinic_key not in self.state_locations_clinic_keys:
                self.__generate_clinic_state_locations_table(clinic_key, insurance_company_id, q_clinic_state_locations_table)
                self.state_locations_clinic_keys.append(clinic_key)
            
            q_clinic_claims_table = '%s_clinic_claims_%s' % (clinic_key, icf_id)
            
            
            state_order = RefreshClaimParticipations.CLINICS.get(clinic_key,{}).get('state_order', ['npi'])
            
            npi_column = fcm.get('provider_pin') if ('npi' in state_order and load_properties.get('external_id_type', '').lower() == 'npi') else None 
            if not npi_column: 
                logutil.log(self.logger, logutil.WARNING, 'Relinking Clinic Claims for %s.' % clinic_key)
                return
            else:
                raw_claims_map['npi'] = fcm.get('provider_pin')            
            
            name_phrase = ' OR '.join(["%s REGEXP '%s'" % ('c.provider_name', s) for s in RefreshClaimParticipations.CLINICS.get(clinic_key,{}).get('search_regex',[])])
            
            query_list = []
            
            d_q_clinic_claims = """DROP TABLE IF EXISTS %s.%s""" % (whcfg.scratch_schema, q_clinic_claims_table)
            query_list.append({'query':d_q_clinic_claims,
                               'description':'Dropping table %s.%s if it exists.' % (whcfg.scratch_schema, q_clinic_claims_table),
                               'warning_filter':'ignore'})
            
            c_q_clinic_claims = """CREATE TABLE %s.%s 
                                            (provider_id INT(11) DEFAULT -1,
                                             provider_location_id INT(11) DEFAULT -1,
                                             elig_sub_state VARCHAR(2),
                                             elig_sub_zip3 VARCHAR(3),
                                             elig_mem_state VARCHAR(2),
                                             elig_mem_zip3 VARCHAR(3),
                                             clm_sub_state VARCHAR(2),
                                             clm_sub_zip3 VARCHAR(3),
                                             clm_mem_state VARCHAR(2),
                                             clm_mem_zip3 VARCHAR(3),
                                             clm_provider_state VARCHAR(2),
                                             clm_provider_zip3 VARCHAR(3),
                                             npi_provider_state VARCHAR(2),
                                             npi_provider_zip3 VARCHAR(3),
                                             state VARCHAR(2),
                                             npi VARCHAR(40),
                                             tax_id VARCHAR(40),
                                            INDEX ix_icf_ic(imported_claim_file_id, imported_claim_id),
                                            INDEX ix_c(claim_id),
                                            INDEX ix_p(patient_id))
                                             AS SELECT c.id as claim_id,
                                                       c.imported_claim_id,
                                                       c.imported_claim_file_id,
                                                       c.insurance_company_id,
                                                       c.patient_id,
                                                       c.provider_name
                                            FROM %s.claims c
                                            WHERE c.imported_claim_file_id=%s
                                            AND (%s)""" % (whcfg.scratch_schema, q_clinic_claims_table, whcfg.claims_master_schema, icf_id, name_phrase)
            
            query_list.append({'query':c_q_clinic_claims,
                               'description':'Creating table %s.%s.' % (whcfg.scratch_schema, q_clinic_claims_table),
                               'warning_filter':'ignore'})
            
            set_insert = ', '.join(["qc.%s = %s" % (k,yaml_formula_insert(t_table_name, v, 'ic')) for k,v in raw_claims_map.iteritems() if v]) if ('subscriber' in state_order or 'provider' in state_order) else 'qc.npi = %s' % (yaml_formula_insert(t_table_name, raw_claims_map.get('npi'), 'ic'))
            if set_insert:
                u_q_clinic_claims = """UPDATE %s.%s qc
                                      JOIN %s.%s ic 
                                        ON qc.imported_claim_file_id = ic.imported_claim_file_id AND qc.imported_claim_id=ic.id
                                       SET %s""" % (whcfg.scratch_schema, q_clinic_claims_table, whcfg.claims_master_schema, table_name, set_insert)
                query_list.append({'query':u_q_clinic_claims,
                                   'description':'Update table %s.%s with state/zip data from raw claims.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})

                u_q_clinic_state = """UPDATE %s.%s qc 
                                       JOIN %s.zip3_state zs ON IFNULL(clm_mem_zip3,clm_sub_zip3)=zs.zip3
                                        SET qc.state = zs.state
                                      WHERE qc.state is null""" % (whcfg.scratch_schema,
                                                                   q_clinic_claims_table,
                                                                   whcfg.scratch_schema)            
                query_list.append({'query':u_q_clinic_state,
                                   'description':'Update table %s.%s with state data from raw claims.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})

            if 'subscriber' in state_order:
                u_q_clinic_elig_sub = """UPDATE %s.%s qc
                                    JOIN %s.policy_coverages pc ON qc.patient_id=pc.patient_id
                                    JOIN %s.policies pl ON pc.policy_id=pl.id
                                    JOIN %s.patients p ON pl.subscriber_patient_id=p.id
                                    SET qc.elig_sub_state=p.state, qc.elig_sub_zip3=SUBSTRING(p.zip,1,3)""" % (whcfg.scratch_schema,
                                                                                                               q_clinic_claims_table,
                                                                                                               whcfg.claims_master_schema,
                                                                                                               whcfg.claims_master_schema,
                                                                                                               whcfg.claims_master_schema,)
                
                query_list.append({'query':u_q_clinic_elig_sub,
                                   'description':'Update table %s.%s with subscriber state/zip data from eligibility data.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_clinic_elig_mem = """UPDATE %s.%s qc
                                        JOIN %s.patients p ON qc.patient_id=p.id
                                         SET qc.elig_mem_state=p.state, qc.elig_mem_zip3=SUBSTRING(p.zip,1,3)""" % (whcfg.scratch_schema,
                                                                                                                    q_clinic_claims_table,
                                                                                                                    whcfg.claims_master_schema)

                query_list.append({'query':u_q_clinic_elig_mem,
                                   'description':'Update table %s.%s with member state/zip data from eligibility data.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
               
                u_q_clinic_state_1 = """UPDATE %s.%s SET state = IFNULL(elig_mem_state,elig_sub_state) WHERE state is null""" % (whcfg.scratch_schema,
                                                                                                          q_clinic_claims_table)
                
                query_list.append({'query':u_q_clinic_state_1,
                                   'description':'Update table %s.%s for state: step 1.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_clinic_state_2 = """UPDATE %s.%s qc 
                                       JOIN %s.zip3_state zs ON IFNULL(elig_mem_zip3,elig_sub_zip3)=zs.zip3
                                        SET qc.state = zs.state
                                      WHERE qc.state is null""" % (whcfg.scratch_schema,
                                                                   q_clinic_claims_table,
                                                                   whcfg.scratch_schema)

                query_list.append({'query':u_q_clinic_state_2,
                                   'description':'Update table %s.%s for state: step 2.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
            
            if 'npi' in state_order:
                u_q_clinic_npi = """UPDATE %s.%s qc
                                   JOIN %s.npi_raw nr ON qc.npi=nr.npi
                                    SET qc.npi_provider_state=Provider_Business_Practice_Location_Address_State_Name,
                                        qc.npi_provider_zip3=SUBSTRING(Provider_Business_Practice_Location_Address_Postal_Code,1,3)""" % (whcfg.scratch_schema, q_clinic_claims_table, whcfg.npi_schema)

                query_list.append({'query':u_q_clinic_npi,
                                   'description':'Update table %s.%s with provider state/zip data from npi.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
                
                u_q_clinic_state_3 = """UPDATE %s.%s SET state = npi_provider_state 
                                      WHERE state is null 
                                         OR state not in (SELECT DISTINCT state FROM %s.%s)"""  % (whcfg.scratch_schema, q_clinic_claims_table,
                                                                                                   whcfg.scratch_schema, q_clinic_state_locations_table)
                query_list.append({'query':u_q_clinic_state_3,
                                   'description':'Update table %s.%s for state: step 3.' % (whcfg.scratch_schema, q_clinic_claims_table),
                                   'warning_filter':'ignore'})
        
            u_q_clinic_p_l = """UPDATE %s.%s qc
                           JOIN %s.%s qs ON qc.state=qs.state
                            SET qc.provider_id=qs.id, qc.provider_location_id=qs.location_id""" % (whcfg.scratch_schema, q_clinic_claims_table,
                                                                                                   whcfg.scratch_schema, q_clinic_state_locations_table)
            query_list.append({'query':u_q_clinic_p_l,
                           'description':'Update table %s.%s for provider_id, provider_location_id.' % (whcfg.scratch_schema, q_clinic_claims_table),
                           'warning_filter':'ignore'})
        
            u_claims = """UPDATE %s.claims c
                        JOIN %s.%s qc ON c.id=qc.claim_id
                         SET c.provider_id=qc.provider_id, c.provider_location_id=qc.provider_location_id
                       WHERE qc.provider_id > -1 AND qc.provider_location_id > -1""" % (whcfg.claims_master_schema, whcfg.scratch_schema, q_clinic_claims_table)
        
            query_list.append({'query':u_claims,
                           'description':'Update table %s.claims for provider_id, provider_location_id.' % (whcfg.claims_master_schema),
                           'warning_filter':'ignore'})
                
            utils.execute_queries(self.conn, self.logger, query_list, dry_run = False)
                     
        return
class RefreshClaimParticipationsProviderLocation(RefreshClaimParticipations):
    def __init__(self, input, logger):
        RefreshClaimParticipations.__init__(self, input, logger)

    def __lookup_imported_claim_files(self, icf_ids, ic_id):
        t_icf = dbutils.Table(self.conn, 'imported_claim_files')
        if icf_ids:
            t_icf.search("""id in (%s) and load_properties is not null and claim_file_type = 'M'""" % ','.join([str(a) for a in icf_ids]))
        elif ic_id:
            t_icf.search("""id in (SELECT imported_claim_file_id FROM imported_claim_files_insurance_companies WHERE insurance_company_id=%s) and load_properties is not null and claim_file_type = 'M'""" % ic_id)
        table_icfid_map = t_icf.rows_to_dict_partitioned_by(t_icf[0:len(t_icf)], 'table_name', column_list = ['id', 'table_name','load_properties'])
        return table_icfid_map 
 
    def process(self):
        
        logutil.log(self.logger, logutil.INFO, 'RefreshClaimParticipationsProviderLocation.process(). Mode = %s' % self.mode)
        
        if self.mode:    
#            if self.imported_claim_file_ids:
            table_icfid_map = self.__lookup_imported_claim_files(self.imported_claim_file_ids, self.insurance_company_id)
            for table_name, icf_id_map_list in table_icfid_map.iteritems():
                icf_ids = [str(s_id) for s_id in sorted([id.get('id') for id in icf_id_map_list])]
#                pprint.pprint(icf_id_map_list)
                load_properties = yaml.load(icf_id_map_list[0].get('load_properties'))
                
                # This first step of augmenting the claim_provider_identifiers table is agnstic of insurance company
                #self.__refresh_claim_provider_identifiers(icf_ids, table_name, load_properties)
                
                self.__refresh_claim_participations_generic(icf_ids, table_name, load_properties, self.mode)
                
                # TODO: Should we also make sure that there are no extraneous participations 
                # Created by the claim participations step for the labs of interest?
                #self.__relink_lab_and_clinic_claims(icf_ids) 
                
                #self.__apply_overrides(icf_ids)
                
        dbutils.close_connections([self.conn, self.master_conn])

    def __refresh_claim_participations_generic(self, icf_ids, table_name, load_properties, external_id_type = 'NPI'):
        
        cur = self.conn.cursor()
        field_column_mappings = load_properties.get('field_column_mappings')
        table_name_hash = hashlib.sha1('_'.join(icf_ids)).hexdigest()
        claim_npis_table_name = 't_%s_claim_npis' % (table_name_hash)
        claim_providers_table_name = 't_%s_claim_providers' % (table_name_hash)
        claim_providers_table_name_3m = 't_%s_claim_providers_3m' % (table_name_hash) 
        # Creation of claim participations will require both the insurance company as well as the employer 
        # because the participations would have to be created using the bucket network
        #for e in employers_insurance_companies:
        #    self.__resolve_network_id(e['insurance_company_id'], e['employer_id'])

        #We build a list of the insurance_company ids for which we will be augmenting the provider directory with CCP
        #we can't use self.insurance_company_id here becuase we might be doing this for more than one icid at the same time
        

        i_ocl = """INSERT IGNORE INTO  %s.original_claim_locations
                   SELECT c.id as claim_id,
                           c.provider_id,
                           p.provider_type,
                           c.provider_location_id,
                           l.*,
                           substr(l.match_key,1,5) as mk5
                    FROM %s.claims c,
                         %s.providers p,
                         %s.locations l
                    WHERE c.provider_id=p.id
                      AND c.provider_location_id=l.id
                      AND c.imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, whcfg.claims_master_schema, whcfg.master_schema, whcfg.master_schema, ','.join(icf_ids))
        
        u_clb = """UPDATE %s.claims SET provider_location_id=-1,
                    updated_at = NOW()
                    WHERE imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, ','.join(icf_ids))

        u_c_p ="""UPDATE %s.claims c 
                    JOIN %s.claim_provider_identifiers cpi ON c.id=cpi.claim_id 
                    JOIN %s.%s tp ON cpi.external_id=tp.npi 
                     SET c.provider_id=tp.provider_id
                    WHERE c.imported_claim_file_id IN (%s)""" % (whcfg.claims_master_schema, 
                                                                 whcfg.claims_master_schema, 
                                                                 whcfg.scratch_schema, 
                                                                 claim_providers_table_name, 
                                                                 ','.join(icf_ids))
        
        d_c_d_p = """DROP TABLE IF EXISTS %s.d_c_p_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)
        
        c_c_d_p = """CREATE TABLE %s.d_c_p_%s (index ix_p(provider_id))
                     SELECT DISTINCT provider_id FROM %s.claims WHERE imported_claim_file_id IN (%s)""" % (whcfg.scratch_schema,
                                                                                                           table_name_hash,
                                                                                                           whcfg.claims_master_schema,
                                                                                                           ','.join(icf_ids))
        
        d_c_p_l = """DROP TABLE IF EXISTS %s.c_p_l_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)
        
        c_c_p_l = """CREATE TABLE %s.c_p_l_%s (mk5 VARCHAR(5), INDEX ix_p_z_mk5(provider_id, zip, mk5), INDEX ix_p_c(provider_id, city), INDEX ix_p_s(provider_id, state))
                        AS SELECT pln.provider_id, pln.location_id, l.zip, l.city, l.state, substr(l.match_key, 1, 5) as mk5
                        FROM 
                        %s.d_c_p_%s p 
                        JOIN %s.providers_locations_networks pln ON pln.provider_id=p.provider_id
                        JOIN %s.locations l ON pln.location_id=l.id
                        GROUP BY provider_id, location_id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.master_schema,
                               whcfg.master_schema)

        d_t_c_p_l = """DROP TABLE IF EXISTS %s.t_c_p_l_%s""" % (whcfg.scratch_schema,
                                                                               table_name_hash)

        u_c_1_1 = """CREATE TABLE %s.t_c_p_l_%s (claim_id BIGINT(20) UNSIGNED NOT NULL PRIMARY KEY, provider_location_id INT(11))
                     AS SELECT c.id AS claim_id, pl.location_id as provider_location_id 
                        FROM %s.claims c
                        JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                        JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                        AND ocl.zip=pl.zip
                        AND ocl.mk5=pl.mk5
                        WHERE c.imported_claim_file_id IN (%s)
                        GROUP BY c.id
                        """ % (
                               whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_1_2 = """UPDATE %s.claims c
                        JOIN %s.t_c_p_l_%s tcl ON c.id=tcl.claim_id
                         SET c.provider_location_id=tcl.provider_location_id
                       WHERE c.imported_claim_file_id IN (%s)""" %(whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_1 = """UPDATE %s.claims c
                        JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                        JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                        AND ocl.zip=pl.zip
                        AND ocl.mk5=pl.mk5
                        SET c.provider_location_id=pl.location_id
                        WHERE c.imported_claim_file_id IN (%s)
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
        
        
        
        
        
#        u_c_1 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.id=clb.claim_id
#                      AND c.imported_claim_file_id IN (%s)
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.zip=l.zip
#                      AND substr(clb.match_key,1,5)=substr(l.match_key,1,5)""" % (whcfg.claims_master_schema,
#                                                                                  whcfg.claims_master_schema,
#                                                                                  whcfg.master_schema,
#                                                                                  whcfg.master_schema,
#                                                                                  ','.join(icf_ids))
        
        # If facility update claim location if zip matches valid participating location zip
        u_c_2_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.zip=pl.zip
                    WHERE c.imported_claim_file_id IN (%s)
                      AND ocl.provider_type='facility'
                     GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_2 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.zip=pl.zip
                      SET c.provider_location_id=pl.location_id
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND ocl.provider_type='facility'
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
        
        
#        u_c_2 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers p,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=p.id
#                      AND p.provider_type='facility'
#                      AND p.id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.zip=l.zip"""  % (whcfg.claims_master_schema,
#                                               whcfg.claims_master_schema,
#                                               whcfg.master_schema,
#                                               whcfg.master_schema,
#                                               whcfg.master_schema,
#                                               ','.join(icf_ids))


        # Update claim location if city matches valid participating location city
        u_c_3_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id 
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.city=pl.city
                    WHERE c.imported_claim_file_id IN (%s)
                    GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))

        u_c_3 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.city=pl.city
                      SET c.provider_location_id=pl.location_id
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
#        u_c_3 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.city=l.city""" % (whcfg.claims_master_schema,
#                                                whcfg.claims_master_schema,
#                                                whcfg.master_schema,
#                                                whcfg.master_schema,
#                                                ','.join(icf_ids))

        # Update claim location if state matches valid participating location state
        u_c_4_1 = """INSERT IGNORE INTO %s.t_c_p_l_%s (claim_id, provider_location_id)
                     SELECT c.id, pl.location_id 
                       FROM %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.state=pl.state
                    WHERE c.imported_claim_file_id IN (%s)
                    GROUP BY c.id
                        """ % (whcfg.scratch_schema,
                               table_name_hash,
                               whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
 
        u_c_4 = """UPDATE %s.claims c
                     JOIN %s.original_claim_locations ocl ON c.id=ocl.claim_id
                     JOIN %s.c_p_l_%s pl ON ocl.provider_id=pl.provider_id
                      AND ocl.state=pl.state
                      SET c.provider_location_id=pl.location_id
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                        """ % (whcfg.claims_master_schema,
                               whcfg.claims_master_schema,
                               whcfg.scratch_schema,
                               table_name_hash,
                               ','.join(icf_ids))
        
#        u_c_4 = """UPDATE 
#                    %s.claims c,
#                    %s.original_claim_locations clb,
#                    %s.providers_locations_networks pln,
#                    %s.locations l
#                    SET c.provider_location_id=l.id
#                    WHERE c.imported_claim_file_id IN (%s)
#                      AND c.provider_location_id=-1
#                      AND c.id=clb.claim_id
#                      AND c.provider_id=pln.provider_id
#                      AND pln.location_id=l.id
#                      AND clb.state=l.state""" % (whcfg.claims_master_schema,
#                                                 whcfg.claims_master_schema,
#                                                 whcfg.master_schema,
#                                                 whcfg.master_schema,
#                                                 ','.join(icf_ids))
     
        # Update claim location to a valid participating location
        u_c_5 = """UPDATE 
                    %s.claims c,
                    %s.providers_locations_networks pln
                    SET c.provider_location_id=pln.location_id
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND c.provider_id=pln.provider_id""" % (whcfg.claims_master_schema,
                                                              whcfg.master_schema,
                                                              ','.join(icf_ids))

        # Backfill any remaining locations with the original claims location
        u_c_6 = """UPDATE 
                    %s.claims c,
                    %s.original_claim_locations clb
                    SET c.provider_location_id=clb.provider_location_id
                    WHERE c.imported_claim_file_id IN (%s)
                      AND c.provider_location_id=-1
                      AND c.id=clb.claim_id""" % (whcfg.claims_master_schema,
                                                  whcfg.claims_master_schema,
                                                  ','.join(icf_ids))


        utils.execute_queries(self.conn, self.logger, [
                                                       {'query':i_ocl,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_clb,
                                                        'warning_filter':'ignore'},
                                                       {'query':d_c_d_p, 
                                                        'warning_filter':'ignore'},
                                                       {'query':c_c_d_p, 
                                                        'warning_filter':'ignore'},
                                                       {'query':d_c_p_l, 
                                                        'warning_filter':'ignore'},
                                                       {'query':c_c_p_l, 
                                                        'warning_filter':'ignore'},
                                                       {'query':d_t_c_p_l,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_1_1, # Very Slow
                                                        'warning_filter':'ignore'},
                                                 #      {'query':u_c_1, # Very Slow
                                                 #       'warning_filter':'ignore'},
                                                       {'query':u_c_2_1, # Slow
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_3_1,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_4_1,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_1_2, # Very Slow
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_5,
                                                        'warning_filter':'ignore'},
                                                       {'query':u_c_6,
                                                        'warning_filter':'ignore'}
                                                       ], dry_run=False)
                      
        
def refresh_claim_participations(db_name, insurance_company_id, employer_id, logger = None, time_interval = 180):
    method_input = """-d %s -i %s -e %s -t %s""" % (db_name, insurance_company_id, employer_id, time_interval)
    method_handler = ClaimsUtilFactory.get_instance('refresh_claim_participations', method_input, logger)
    method_handler.process()
            
def refresh_claim_participations_npi(db_name, imported_claim_file_ids = [], logger = None, only_relink = False):
    method_input = """-d %s -f %s -m npi""" % (db_name, ','.join(imported_claim_file_ids))
    if only_relink:
        method_input = method_input + " -r True"
    method_handler = ClaimsUtilFactory.get_instance('refresh_claim_participations', method_input, logger)
    method_handler.process()

def refresh_claim_participations_generic(db_name, imported_claim_file_ids = [], logger = None, only_relink = False, external_id_type='npi'):
    method_input = """-d %s -f %s -m %s""" % (db_name, ','.join(imported_claim_file_ids), external_id_type)
    if only_relink:
        method_input = method_input + " -r True"
    method_handler = ClaimsUtilFactory.get_instance('refresh_claim_participations', method_input, logger)
    method_handler.process()
    
def refresh_claim_participations_provider_location(db_name, imported_claim_file_ids = [], logger = None, only_relink = False, external_id_type='npi'):
    method_input = """-d %s -f %s -m %s""" % (db_name, ','.join(imported_claim_file_ids), external_id_type)
    #if only_relink:
    #    method_input = method_input + " -r True"
    method_handler = ClaimsUtilFactory.get_instance('refresh_claim_participations_provider_location', method_input, logger)
    method_handler.process()

def relink_lab_and_clinic_claims(db_name, icf_id, insurance_company_id, employer_id, table_name, load_properties, lab_keys, clinic_keys, logger, external_id_type='npi'):
    method_input = """-d %s -f %s -m %s -r True""" % (db_name, icf_id, external_id_type) if icf_id else """-d %s -m %s -r True""" % (db_name, external_id_type)
    method_handler = ClaimsUtilFactory.get_instance('refresh_claim_participations', method_input, logger)    
    master_loader_properties = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml','r'))
    method_handler.relink_lab_and_clinic_claims(icf_id, insurance_company_id, employer_id, table_name, load_properties, lab_keys, clinic_keys, None) # Currently is_blue is not being used. So passing None.

def match_claim_providers(db_name, imported_claim_file_ids = [], logger = None):
    method_input = """-d %s -f %s""" % (db_name, ','.join(imported_claim_file_ids))
    method_handler = ClaimsUtilFactory.get_instance('match_claim_providers', method_input, logger)
    method_handler.process()

class PatientIdentifier:
    
    SUPPRESSION_MAP = {}
#    SUPPRESSION_MAP = {'premera':['dependent_identification'],
#                       '22':['dependent_identification']}

    def __init__(self):
        self.patient_dict = {}
        self.dependent_first_name_max = 100
    
    def __fetch_dependent_first_name_max(self, conn, insurance_company_id, employer_id):
        query_string = """SELECT ifpc.dependent_first_name_max as dependent_first_name_max_payor,
                                 ifec.dependent_first_name_max as dependent_first_name_max_employer
                          FROM import_file_payor_config ifpc join import_file_employer_config ifec
                          ON ifpc.id = ifec.payor_info_config_id
                          WHERE ifpc.insurance_company_id = %s and ifpc.file_type = 'medical_claims' and ifec.employer_id = %s
                       """ % (insurance_company_id, employer_id)
        dependent_first_name_max_result = Query(conn, query_string)
        dependent_first_name_max = 100
        if dependent_first_name_max_result:
            dependent_first_name_max_options = dependent_first_name_max_result.next()
            if dependent_first_name_max_options['dependent_first_name_max_payor'] is not None:
                dependent_first_name_max  =  dependent_first_name_max_options['dependent_first_name_max_payor']
            if dependent_first_name_max_options['dependent_first_name_max_employer'] is not None:
                dependent_first_name_max  =  dependent_first_name_max_options['dependent_first_name_max_employer']
        return dependent_first_name_max
        
    def resolve_nonssn_claim_patient(self, conn=None,
                                            subscriber_identifier=None,
                                            subscriber_first_name=None,
                                            subscriber_last_name=None,  
                                            member_identifier=None, 
                                            member_first_name=None, 
                                            member_dob=None,
                                            member_last_name=None, 
                                            is_relationship_available=None,
                                            is_subscriber=None,
                                            is_member_identifier_available=None,
                                            is_subscriber_first_name_available=None,
                                            insurance_company_id=0,
                                            employer_id=0,
                                            identifier_type='ssn'):
        
        # NOTE: non-ssn support is only available for Case A. When this code is made generic
        # add support to Case B also.
        try:
            """
                ** Assumes caller has definitively categorized is_subscriber, when available, into subscriber and non-subscriber
                
                Case A: 'Relationship' is provided (is_relationship_available == True) AND 'Patient SSN' is provided (is_member_ssn_available == True)
                (1) Assign claim subscriber_patient_id using the 'Subscriber SSN' from claim
                    (1.1) No additional validation required beyond trusting the 'Subscriber SSN' (Should be in the contract with source of claims, eligibility data)                
                
                (2) Assign claim patient_id as follows:
                    (2.1) If ('Relationship' is 'subscriber') AND ('Patient SSN' is equal to 'Subscriber SSN') 
                          (2.1.1) Set claim patient_id = subscriber_patient_id
                    (2.2) ElsIf ('Relationship' is 'subscriber') AND ('Patient SSN' is missing)
                          (2.2.1) If 'Subscriber First Name' is provided (is_subscriber_first_name_available == True) AND ('Subscriber First Name' == 'Patient First Name')
                                  (2.2.1.1) Pull eligibility record for subscriber_patient_id
                                  (2.2.1.2) If first name on the record matches 'Subscriber First Name' 
                                            (2.2.1.2.1) Assign claim patient_id
                                  (2.2.1.3) Else
                                            (2.2.1.3.1) Do not assign claim patient_id
                          (2.2.2) Else
                                  (2.2.2.1) Do not assign claim patient_id
                    (2.3) ElsIf ('Relationship' is 'subscriber') AND ('Patient SSN' is not equal to 'Subscriber SSN')
                          (2.3.1) Do not assign claim patient_id
                    (2.4) ElsIf ('Relationship' is not 'subscriber')
                          (2.4.1) Pull all patients (excluding subscriber) on policies subscribed by subscriber_patient_id
                          (2.4.2) Set claim patient_id based on uniquely matching 'Member FN' and 'Member DOB' to covered patient record
                                  (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB')   
                          (2.4.3) Set claim patient_id based on uniquely matching 'Member FN', 'Member DOB' and 'Member LN' to covered patient record
                                  (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB', 'Member LN')                    
                
                Case B: 'Relationship' or 'Patient SSN' is not provided (is_relationship_available == False) or (is_member_ssn_available == True):
                (1) Assign claim subscriber_patient_id using the 'Subscriber SSN' from claim
                    (1.1) No additional validation required beyond trusting the 'Subscriber SSN' (Should be in the contract with source of claims, eligibility data)
                
                (2) Assign claim patient_id as follows:
                    (2.1) Pull all patients (including subscriber) on policies subscribed by subscriber_patient_id
                    (2.2) Set claim patient_id based on uniquely matching 'Member FN' and 'Member DOB' to covered patient record
                          (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB')
                    (2.3) Set claim patient_id based on uniquely matching 'Member FN', 'Member DOB' and 'Member LN' to covered patient record
                          (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB', 'Member LN')
            """
            self.conn = conn
            self.patient_dict['subscriber_patient_id'] = -1
            self.patient_dict['patient_id'] = -1
            self.patient_dict['source_subscriber_member_id'] = subscriber_identifier
            self.patient_dict['source_member_id'] = member_identifier
            self.dependent_first_name_max = self.__fetch_dependent_first_name_max(conn, insurance_company_id, employer_id)
 
            if not subscriber_identifier:
                return self.patient_dict
            else:
                subscriber_patient_result = []
                # identify subscriber
                if identifier_type == 'ssn':
                    q_subscribing_patient = """SELECT p.* 
                                                 FROM patients p,
                                                      policies po
                                                WHERE p.ssn = '{sub_ssn}'
                                                  AND po.subscriber_patient_id = p.id
                                                GROUP BY p.id""".format(sub_ssn=subscriber_identifier)            
                    subscriber_patient_result = Query(self.conn, q_subscribing_patient)
                else:
                    if not employer_id:
                        return self.patient_dict
                    
                    q_subscribing_patient = """SELECT p.* 
                                                 FROM patients p,
                                                      policies po,
                                                      patient_identifiers pi
                                                WHERE po.subscriber_patient_id = p.id
                                                  AND pi.patient_id=p.id
                                                  AND pi.identifier_type='{id_type}'
                                                  AND pi.value='{sub_id}'
                                                  AND pi.employer_id={emp_id}
                                                GROUP BY p.id""".format(id_type=identifier_type,sub_id=subscriber_identifier,emp_id=employer_id)            
                    subscriber_patient_result = Query(self.conn, q_subscribing_patient)                    
                
                if len(subscriber_patient_result) != 1:
                    return self.patient_dict                
                
                subscriber_patient = subscriber_patient_result.next()                
                self.patient_dict['subscriber_patient_id'] = subscriber_patient['id']
            
            # identify patient
            if is_relationship_available and is_member_identifier_available:  ## CASE A
                if is_subscriber and member_identifier and member_identifier == subscriber_identifier: ## (2.1)
                    self.patient_dict['patient_id'] = self.patient_dict['subscriber_patient_id']    ## (2.1.1)
                elif is_subscriber and not member_identifier: ## (2.2)
                    if is_subscriber_first_name_available and subscriber_first_name and member_first_name and subscriber_first_name.lower() == member_first_name.lower():   ## (2.2.1)
                        if subscriber_first_name.lower() == (subscriber_patient['first_name'].lower() if subscriber_patient['first_name'] else None):  ## ((2.2.1.2)
                            self.patient_dict['patient_id'] = subscriber_patient['id'] ## (2.2.1.2.1)
                        else:
                            pass ## (2.2.1.3.1)
                    else:     ## (2.2.2)
                        pass    ## (2.2.2.1)                 
                elif is_subscriber and member_identifier and member_identifier != subscriber_identifier:   ## (2.3)
                    pass    ## (2.3.1)
                elif not is_subscriber:   ## (2.4)
                    if member_first_name and member_dob:
                        q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                              FROM policies po,
                                                   policy_coverages pc,
                                                   patients p
                                             WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                               AND pc.policy_id = po.id
                                               AND pc.patient_id = p.id
                                               AND p.id != {subscriber_patient_id}
                                               AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                               AND p.date_of_birth = {mem_dob}
                                          GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                           mem_fn='%s',
                                                                                           mem_dob='%s',
                                                                                           dependent_first_name_max=self.dependent_first_name_max)
                        patients_result = Query(self.conn, q_patients, (member_first_name, member_dob))  
                        patient = patients_result.next() if patients_result else None
                        if not patient:
                            pass
                        elif patient['num_patients'] == 1:
                            self.patient_dict['patient_id'] = patient['id']
                        elif patient['num_patients'] > 1:
                            if member_last_name:
                                q_patients = """SELECT p.id
                                                      FROM policies po,
                                                           policy_coverages pc,
                                                           patients p,
                                                           accounts a
                                                     WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                       AND pc.policy_id = po.id
                                                       AND pc.patient_id = p.id
                                                       AND p.id != {subscriber_patient_id}
                                                       AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                                       AND p.date_of_birth = {mem_dob}
                                                       AND p.last_name = {mem_ln}
                                                       AND p.id = a.patient_id
                                                       AND a.is_deleted != 1
                                                  GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth, p.last_name
                                                  HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                            mem_fn='%s',
                                                                                            mem_dob='%s',
                                                                                            mem_ln='%s',
                                                                                            dependent_first_name_max=self.dependent_first_name_max)
                                patients_result = Query(self.conn, q_patients, (member_first_name, member_dob, member_last_name))     
                                patient = patients_result.next() if patients_result else None
                                if patient:
                                    self.patient_dict['patient_id'] = patient['id']
                                                
            else:    ## CASE B
                if member_first_name and member_dob:
                    q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                          FROM policies po,
                                               policy_coverages pc,
                                               patients p
                                         WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                           AND pc.policy_id = po.id
                                           AND pc.patient_id = p.id
                                           AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                           AND p.date_of_birth = {mem_dob}
                                      GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                       mem_fn='%s',
                                                                                       mem_dob='%s',
                                                                                       dependent_first_name_max=self.dependent_first_name_max)
                    patients_result = Query(self.conn, q_patients, (member_first_name, member_dob))  
                    patient = patients_result.next() if patients_result else None
                    if not patient:
                        pass
                    elif patient['num_patients'] == 1:
                        self.patient_dict['patient_id'] = patient['id']
                    elif patient['num_patients'] > 1:
                        if member_last_name:
                            q_patients = """SELECT p.id
                                                  FROM policies po,
                                                       policy_coverages pc,
                                                       patients p,
                                                       accounts a
                                                 WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                   AND pc.policy_id = po.id
                                                   AND pc.patient_id = p.id
                                                   AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                                   AND p.date_of_birth = {mem_dob}
                                                   AND p.last_name = {mem_ln}
                                                   AND p.id = a.patient_id
                                                   AND a.is_deleted != 1
                                              GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth, p.last_name
                                              HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                        mem_fn='%s',
                                                                                        mem_dob='%s',
                                                                                        mem_ln='%s',
                                                                                        dependent_first_name_max=self.dependent_first_name_max)
                            patients_result = Query(self.conn, q_patients, (member_first_name, member_dob, member_last_name))     
                            patient = patients_result.next() if patients_result else None
                            if patient:
                                self.patient_dict['patient_id'] = patient['id']
                        
            return self.patient_dict
        except:
            traceback.print_exc()
            return None
        
    def resolve_generic_claim_patient(self, conn=None,
                                            subscriber_ssn=None,
                                            subscriber_first_name=None,
                                            subscriber_last_name=None,  
                                            member_ssn=None, 
                                            member_first_name=None, 
                                            member_dob=None,
                                            member_last_name=None, 
                                            is_relationship_available=None,
                                            is_subscriber=None,
                                            is_member_ssn_available=None,
                                            is_subscriber_first_name_available=None,
                                            insurance_company_id=0,
                                            employer_id=0):
        try:
            """
                ** Assumes caller has definitively categorized is_subscriber, when available, into subscriber and non-subscriber
                
                Case A: 'Relationship' is provided (is_relationship_available == True) AND 'Patient SSN' is provided (is_member_ssn_available == True)
                (1) Assign claim subscriber_patient_id using the 'Subscriber SSN' from claim
                    (1.1) No additional validation required beyond trusting the 'Subscriber SSN' (Should be in the contract with source of claims, eligibility data)                
                
                (2) Assign claim patient_id as follows:
                    (2.1) If ('Relationship' is 'subscriber') AND ('Patient SSN' is equal to 'Subscriber SSN') 
                          (2.1.1) Set claim patient_id = subscriber_patient_id
                    (2.2) ElsIf ('Relationship' is 'subscriber') AND ('Patient SSN' is missing)
                          (2.2.1) If 'Subscriber First Name' is provided (is_subscriber_first_name_available == True) AND ('Subscriber First Name' == 'Patient First Name')
                                  (2.2.1.1) Pull eligibility record for subscriber_patient_id
                                  (2.2.1.2) If first name on the record matches 'Subscriber First Name' 
                                            (2.2.1.2.1) Assign claim patient_id
                                  (2.2.1.3) Else
                                            (2.2.1.3.1) Do not assign claim patient_id
                          (2.2.2) Else
                                  (2.2.2.1) Do not assign claim patient_id
                    (2.3) ElsIf ('Relationship' is 'subscriber') AND ('Patient SSN' is not equal to 'Subscriber SSN')
                          (2.3.1) Do not assign claim patient_id
                    (2.4) ElsIf ('Relationship' is not 'subscriber')
                          (2.4.1) Pull all patients (excluding subscriber) on policies subscribed by subscriber_patient_id
                          (2.4.2) Set claim patient_id based on uniquely matching 'Member FN' and 'Member DOB' to covered patient record
                                  (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB')   
                          (2.4.3) Set claim patient_id based on uniquely matching 'Member FN', 'Member DOB' and 'Member LN' to covered patient record
                                  (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB', 'Member LN')                    
                
                Case B: 'Relationship' or 'Patient SSN' is not provided (is_relationship_available == False) or (is_member_ssn_available == True):
                (1) Assign claim subscriber_patient_id using the 'Subscriber SSN' from claim
                    (1.1) No additional validation required beyond trusting the 'Subscriber SSN' (Should be in the contract with source of claims, eligibility data)
                
                (2) Assign claim patient_id as follows:
                    (2.1) Pull all patients (including subscriber) on policies subscribed by subscriber_patient_id
                    (2.2) Set claim patient_id based on uniquely matching 'Member FN' and 'Member DOB' to covered patient record
                          (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB')
                    (2.3) Set claim patient_id based on uniquely matching 'Member FN', 'Member DOB' and 'Member LN' to covered patient record
                          (To take care of multiple dependents sharing the same 'Member FN', 'Member DOB', 'Member LN')
            """
            self.conn = conn
            self.patient_dict['subscriber_patient_id'] = -1
            self.patient_dict['patient_id'] = -1
            self.patient_dict['source_subscriber_member_id'] = subscriber_ssn
            self.patient_dict['source_member_id'] = member_ssn
            self.dependent_first_name_max = self.__fetch_dependent_first_name_max(conn, insurance_company_id, employer_id)
        
            if not subscriber_ssn:
                return self.patient_dict
            else:
                # identify subscriber
                q_subscribing_patient = """SELECT p.* 
                                             FROM patients p,
                                                  policies po
                                            WHERE p.ssn = '{sub_ssn}'
                                              AND po.subscriber_patient_id = p.id
                                            GROUP BY p.id""".format(sub_ssn=subscriber_ssn)            
                subscriber_patient_result = Query(self.conn, q_subscribing_patient)
                
                if len(subscriber_patient_result) != 1:
                    return self.patient_dict                
                
                subscriber_patient = subscriber_patient_result.next()                
                self.patient_dict['subscriber_patient_id'] = subscriber_patient['id']
            
            # identify patient
            if is_relationship_available and is_member_ssn_available:  ## CASE A
                if is_subscriber and member_ssn and member_ssn == subscriber_ssn: ## (2.1)
                    self.patient_dict['patient_id'] = self.patient_dict['subscriber_patient_id']    ## (2.1.1)
                elif is_subscriber and not member_ssn: ## (2.2)
                    if is_subscriber_first_name_available and subscriber_first_name and member_first_name and subscriber_first_name.lower() == member_first_name.lower():   ## (2.2.1)
                        if subscriber_first_name.lower() == (subscriber_patient['first_name'].lower() if subscriber_patient['first_name'] else None):  ## ((2.2.1.2)
                            self.patient_dict['patient_id'] = subscriber_patient['id'] ## (2.2.1.2.1)
                        else:
                            pass ## (2.2.1.3.1)
                    else:     ## (2.2.2)
                        pass    ## (2.2.2.1)                 
                elif is_subscriber and member_ssn and member_ssn != subscriber_ssn:   ## (2.3)
                    pass    ## (2.3.1)
                elif not is_subscriber:   ## (2.4)
                    if member_first_name and member_dob:
                        q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                              FROM policies po,
                                                   policy_coverages pc,
                                                   patients p
                                             WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                               AND pc.policy_id = po.id
                                               AND pc.patient_id = p.id
                                               AND p.id != {subscriber_patient_id}
                                               AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                               AND p.date_of_birth = {mem_dob}
                                          GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                           mem_fn='%s',
                                                                                           mem_dob='%s',
                                                                                           dependent_first_name_max=self.dependent_first_name_max)
                        patients_result = Query(self.conn, q_patients, (member_first_name, member_dob))  
                        patient = patients_result.next() if patients_result else None
                        if not patient:
                            pass
                        elif patient['num_patients'] == 1:
                            self.patient_dict['patient_id'] = patient['id']
                        elif patient['num_patients'] > 1:
                            if member_last_name:
                                q_patients = """SELECT p.id
                                                      FROM policies po,
                                                           policy_coverages pc,
                                                           patients p,
                                                           accounts a
                                                     WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                       AND pc.policy_id = po.id
                                                       AND pc.patient_id = p.id
                                                       AND p.id != {subscriber_patient_id}
                                                       AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                                       AND p.date_of_birth = {mem_dob}
                                                       AND p.last_name = {mem_ln}
                                                       AND p.id = a.patient_id
                                                       AND a.is_deleted != 1
                                                  GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth, p.last_name
                                                  HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                            mem_fn='%s',
                                                                                            mem_dob='%s',
                                                                                            mem_ln='%s',
                                                                                            dependent_first_name_max=self.dependent_first_name_max)
                                patients_result = Query(self.conn, q_patients, (member_first_name, member_dob, member_last_name))     
                                patient = patients_result.next() if patients_result else None
                                if patient:
                                    self.patient_dict['patient_id'] = patient['id']
                                                
            else:    ## CASE B
                if member_first_name and member_dob:
                    q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                          FROM policies po,
                                               policy_coverages pc,
                                               patients p
                                         WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                           AND pc.policy_id = po.id
                                           AND pc.patient_id = p.id
                                           AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                           AND p.date_of_birth = {mem_dob}
                                      GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                       mem_fn='%s',
                                                                                       mem_dob='%s',
                                                                                       dependent_first_name_max=self.dependent_first_name_max)
                    patients_result = Query(self.conn, q_patients, (member_first_name, member_dob))  
                    patient = patients_result.next() if patients_result else None
                    if not patient:
                        pass
                    elif patient['num_patients'] == 1:
                        self.patient_dict['patient_id'] = patient['id']
                    elif patient['num_patients'] > 1:
                        if member_last_name:
                            q_patients = """SELECT p.id
                                                  FROM policies po,
                                                       policy_coverages pc,
                                                       patients p,
                                                       accounts a
                                                 WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                   AND pc.policy_id = po.id
                                                   AND pc.patient_id = p.id
                                                   AND trim_multiple(left(p.first_name, {dependent_first_name_max}),"`,',.")  = trim_multiple({mem_fn}, "',`,.")
                                                   AND p.date_of_birth = {mem_dob}
                                                   AND p.last_name = {mem_ln}
                                                   AND p.id = a.patient_id
                                                   AND a.is_deleted != 1
                                              GROUP BY left(p.first_name, {dependent_first_name_max}), p.date_of_birth, p.last_name
                                              HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                        mem_fn='%s',
                                                                                        mem_dob='%s',
                                                                                        mem_ln='%s',
                                                                                        dependent_first_name_max=self.dependent_first_name_max)
                            patients_result = Query(self.conn, q_patients, (member_first_name, member_dob, member_last_name))     
                            patient = patients_result.next() if patients_result else None
                            if patient:
                                self.patient_dict['patient_id'] = patient['id']
                        
            return self.patient_dict
        except:
            traceback.print_exc()
            return None

    def resolve_allegis_old_rx_claim_patient(self, conn=None,
                                            subscriber_ssn=None,
                                            subscriber_first_name=None,
                                            subscriber_last_name=None,  
                                            member_ssn=None, 
                                            member_first_name=None, 
                                            member_dob=None,
                                            member_last_name=None, 
                                            is_relationship_available=None,
                                            is_subscriber=None,
                                            is_member_ssn_available=None,
                                            is_subscriber_first_name_available=None,
                                            insurance_company_id=0):
        try:
            """ 
            **** Same logic as generic method except that comparison of First Name is limited to first 5 characters ****
            """
            self.conn = conn
            self.patient_dict['subscriber_patient_id'] = -1
            self.patient_dict['patient_id'] = -1
            self.patient_dict['source_subscriber_member_id'] = subscriber_ssn
            self.patient_dict['source_member_id'] = member_ssn
        
            if not subscriber_ssn:
                return self.patient_dict
            else:
                # identify subscriber
                q_subscribing_patient = """SELECT p.* 
                                             FROM patients p,
                                                  policies po
                                            WHERE p.ssn = '{sub_ssn}'
                                              AND po.subscriber_patient_id = p.id
                                            GROUP BY p.id""".format(sub_ssn=subscriber_ssn)            
                subscriber_patient_result = Query(self.conn, q_subscribing_patient)
                
                if len(subscriber_patient_result) == 0:
                    return self.patient_dict
                elif len(subscriber_patient_result) > 1:          
                    q_subscribing_patient = """SELECT p.* 
                                                 FROM patients p,
                                                      policies po,
                                                      insurance_plan_code_mappings ipcm,
                                                      pantry_master_production.insurance_plans ip
                                                WHERE p.ssn = '{sub_ssn}'
                                                  AND po.subscriber_patient_id = p.id
                                                  AND po.employer_id = ipcm.employer_id
                                                  AND po.plan_code = ipcm.plan_code
                                                  AND ipcm.insurance_plan_key = ip.key
                                                  AND ip.insurance_company_id = {ic_id}
                                                GROUP BY p.id""".format(sub_ssn=subscriber_ssn, ic_id=insurance_company_id) 
                    subscriber_patient_result = Query(self.conn, q_subscribing_patient)                    
            
                if len(subscriber_patient_result) != 1:
                    return self.patient_dict                
                
                subscriber_patient = subscriber_patient_result.next()                
                self.patient_dict['subscriber_patient_id'] = subscriber_patient['id']
            
            # identify patient
            if is_relationship_available and is_member_ssn_available:  ## CASE A
                if is_subscriber and member_ssn and member_ssn == subscriber_ssn: ## (2.1)
                    self.patient_dict['patient_id'] = self.patient_dict['subscriber_patient_id']    ## (2.1.1)
                elif is_subscriber and not member_ssn: ## (2.2)
                    if is_subscriber_first_name_available and subscriber_first_name and member_first_name and subscriber_first_name.lower() == member_first_name.lower():   ## (2.2.1)
                        if subscriber_first_name.lower() == (subscriber_patient['first_name'].lower() if subscriber_patient['first_name'] else None):  ## ((2.2.1.2)
                            self.patient_dict['patient_id'] = subscriber_patient['id'] ## (2.2.1.2.1)
                        else:
                            pass ## (2.2.1.3.1)
                    else:     ## (2.2.2)
                        pass    ## (2.2.2.1)                 
                elif is_subscriber and member_ssn and member_ssn != subscriber_ssn:   ## (2.3)
                    pass    ## (2.3.1)
                elif not is_subscriber:   ## (2.4)
                    if member_first_name and member_dob:
                        q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                              FROM policies po,
                                                   policy_coverages pc,
                                                   patients p
                                             WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                               AND pc.policy_id = po.id
                                               AND pc.patient_id = p.id
                                               AND p.id != {subscriber_patient_id}
                                               AND LEFT(p.first_name, 5) = {mem_fn}
                                               AND p.date_of_birth = {mem_dob}
                                          GROUP BY p.first_name, p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                           mem_fn='%s',
                                                                                           mem_dob='%s')
                        patients_result = Query(self.conn, q_patients, (member_first_name[:5], member_dob))  
                        patient = patients_result.next() if patients_result else None
                        if not patient:
                            pass
                        elif patient['num_patients'] == 1:
                            self.patient_dict['patient_id'] = patient['id']
                        elif patient['num_patients'] > 1:
                            if member_last_name:
                                q_patients = """SELECT p.id
                                                      FROM policies po,
                                                           policy_coverages pc,
                                                           patients p
                                                     WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                       AND pc.policy_id = po.id
                                                       AND pc.patient_id = p.id
                                                       AND p.id != {subscriber_patient_id}
                                                       AND LEFT(p.first_name, 5) = {mem_fn}
                                                       AND p.date_of_birth = {mem_dob}
                                                       AND p.last_name = {mem_ln}
                                                  GROUP BY p.first_name, p.date_of_birth, p.last_name
                                                  HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                            mem_fn='%s',
                                                                                            mem_dob='%s',
                                                                                            mem_ln='%s')
                                patients_result = Query(self.conn, q_patients, (member_first_name[:5], member_dob, member_last_name))     
                                patient = patients_result.next() if patients_result else None
                                if patient:
                                    self.patient_dict['patient_id'] = patient['id']
                                                
            else:    ## CASE B
                if member_first_name and member_dob:
                    q_patients = """SELECT p.id, COUNT(DISTINCT p.id) as num_patients
                                          FROM policies po,
                                               policy_coverages pc,
                                               patients p
                                         WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                           AND pc.policy_id = po.id
                                           AND pc.patient_id = p.id
                                           AND LEFT(p.first_name, 5) = {mem_fn}
                                           AND p.date_of_birth = {mem_dob}
                                      GROUP BY p.first_name, p.date_of_birth""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                       mem_fn='%s',
                                                                                       mem_dob='%s')
                    patients_result = Query(self.conn, q_patients, (member_first_name[:5], member_dob))  
                    patient = patients_result.next() if patients_result else None
                    if not patient:
                        pass
                    elif patient['num_patients'] == 1:
                        self.patient_dict['patient_id'] = patient['id']
                    elif patient['num_patients'] > 1:
                        if member_last_name:
                            q_patients = """SELECT p.id
                                                  FROM policies po,
                                                       policy_coverages pc,
                                                       patients p
                                                 WHERE po.subscriber_patient_id = {subscriber_patient_id}
                                                   AND pc.policy_id = po.id
                                                   AND pc.patient_id = p.id
                                                   AND LEFT(p.first_name, 5) = {mem_fn}
                                                   AND p.date_of_birth = {mem_dob}
                                                   AND p.last_name = {mem_ln}
                                              GROUP BY p.first_name, p.date_of_birth, p.last_name
                                              HAVING COUNT(DISTINCT p.id) = 1""".format(subscriber_patient_id=self.patient_dict['subscriber_patient_id'],
                                                                                        mem_fn='%s',
                                                                                        mem_dob='%s',
                                                                                        mem_ln='%s')
                            patients_result = Query(self.conn, q_patients, (member_first_name[:5], member_dob, member_last_name))     
                            patient = patients_result.next() if patients_result else None
                            if patient:
                                self.patient_dict['patient_id'] = patient['id']
                        
            return self.patient_dict
        except:
            traceback.print_exc()
            return None
            

load_helpers = {"medical": lambda claims_master_conn, provider_master_conn, imported_claim_file_id: claims_load_helper.ClaimsLoaderFactory.get_instance(claims_master_conn, provider_master_conn, imported_claim_file_id), 
                "pharma": lambda claims_master_conn, provider_master_conn, imported_claim_file_id: rx_claims_load_helper.RxClaimsLoaderFactory.get_instance(claims_master_conn, provider_master_conn, imported_claim_file_id),
                "dental": lambda claims_master_conn, provider_master_conn, imported_claim_file_id: dental_claims_load_helper.DentalClaimsLoaderFactory.get_instance(claims_master_conn, provider_master_conn, imported_claim_file_id)
               }

class ClaimTypeLoaderFactory:

    def get_instance(claim_type, claims_master_conn, provider_master_conn, imported_claim_file_id):
        return load_helpers[claim_type](claims_master_conn, provider_master_conn, imported_claim_file_id)

    get_instance = _Callable(get_instance)

class ClaimsValidator:
    """

    """    
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m claims_validation_report
  -H, --Help                                              show this help message and exit
  -d DB_NAME, --db_name=DB_NAME                           name of claims master database
  -i INSURANCE_COMPANY_ID, --insurance_company_id         ID of the insurance company
  -e EMPLOYER_ID, --employer_id                           ID of the employer
  -f IMPORTED_CLAIM_FILE_IDS --imported_claim_file_ids    List of Imported Claim File IDs for augmentation
  -v VALIDATION_FILE --validation-file                    Validation YAML file
  -t CLAIM_TYPE --claim-type                              Claim Type [medical/pharma/dental]""")

        p.add_option("-d", "--db_name", type="string",
                      dest="db_name",
                      help="Name of claims master database.")
        p.add_option("-i", "--insurance_company_id", type="string",
                      dest="insurance_company_id",
                      help="Insurance Company ID.")
        p.add_option("-e", "--employer_id", type="string",
                      dest="employer_id",
                      help="Employer ID.")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids",
                      help="Imported Claim File IDs. For e.g. 1,3,7")
        p.add_option("-v", "--validation-file", type="string",
                      dest="validation_file",  default="/claims/import/util/claims_validations.yml",
                      help="Validation YAML file")
        p.add_option("-t", "--claim-type", type="string",
                      dest="claim_type", default="medical",
                      help="Claim Type [medical/pharma/dental]")

        if not input:
            print p.usage
            sys.exit(2)
        (self.method_options, args) = p.parse_args(input.split(' '))
    
    def __init__(self, input, logger):
        
        self.logger = logger if logger else logutil.initlog('importer')
        
        self.method_options = None
#        self.insurance_network_id = None
        self.scratch_tables_created = set([])
        
        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)
        
        self.conn = getDBConnection(dbname = self.method_options.db_name,
                              host = whcfg.claims_master_host,
                              user = whcfg.claims_master_user,
                              passwd = whcfg.claims_master_password,
                              useDictCursor = True)

        self.master_conn = getDBConnection(dbname = whcfg.master_schema,
                              host = whcfg.master_host,
                              user = whcfg.master_user,
                              passwd = whcfg.master_password,
                              useDictCursor = True)
        
        self.imported_claim_file_ids = None
        if self.method_options.imported_claim_file_ids:
            self.imported_claim_file_ids = [int(i) for i in self.method_options.imported_claim_file_ids.split(',')]
        
        self.claims_validations = yaml.load(open(whcfg.providerhome + self.method_options.validation_file))
        self.field_mappings = {}
        
    def __raw_validations(self):
        return 

    def process(self):
        # Steps
        # (1) Input may be imported_claim_file_ids and/or insurance_company_id and/or employer_id or nothing       
        # (2) Look up imported_claim_files, imported_claim_file_insurance_companies
        # (3) If cigna or aetna or yml file exists - call raw validation
        # (4) If yml does not exist - Report as un-normalized
        # (5) If normalized, run validation of normalized data
        #       
        try: 
            icf_query =  """SELECT icf.id, insurance_company_id, employer_id, table_name, load_properties, ic.name as insurance_company_name, normalized
                              FROM imported_claim_files icf,
                                   imported_claim_files_insurance_companies icfic, 
                                   %s.insurance_companies ic
                            WHERE icfic.imported_claim_file_id=icf.id
                              AND ic.id=icfic.insurance_company_id""" % (whcfg.master_schema)
                              
            if self.imported_claim_file_ids:
                icf_q1 = """ AND icf.id IN (%s)""" % self.method_options.imported_claim_file_ids
                icf_query = icf_query + icf_q1
                
            if self.method_options.employer_id:
                icf_q2 = """ AND icf.employer_id = %s""" % self.method_options.employer_id
                icf_query = icf_query + icf_q2
            
            if self.method_options.insurance_company_id:
                icf_q3 = """ AND icfic.insurance_company_id = %s""" % self.method_options.insurance_company_id
                icf_query = icf_query + icf_q3
            
            logutil.log(self.logger, logutil.INFO, 'Running Claims Validation Report...')
                
            icf_query_raw = icf_query 
            
            pre_normalized_files = {} 
            r_query_raw = Query(self.conn, icf_query_raw)
            if r_query_raw: logutil.log(self.logger, logutil.INFO, 'Running Raw Validations for pre-normalized claims set...')
            for res in r_query_raw:
                icf_id = res['id']
                ic_id = res['insurance_company_id']
                ic_name = res['insurance_company_name']
                e_id = res['employer_id']
                clh = ClaimTypeLoaderFactory.get_instance(self.method_options.claim_type, self.conn, self.master_conn, icf_id)
                filter_condition = None

                if ic_id > 2 or self.method_options.validation_file.find("rx_claims_validations.yml") > -1:
		    yaml_mappings = clh.load_properties.get('field_column_mappings')
                    filter_condition = clh.load_properties.get('filter_condition')
                else:
		    yaml_mappings = claims_load_helper.FIELD_MAPPINGS.get(ic_name.lower())

		## create dictionary of field mappings
		field_mappings = { k:v for (k,v) in yaml_mappings.items() if isinstance(v, basestring) } 
		## add mappings that use a formula
		field_mappings.update({k:v['formula'] for (k,v) in yaml_mappings.items() if isinstance(v, dict) and 'formula' in v } )
		## add mappings that use a dictionary
		field_mappings.update({k:v for (k,v) in yaml_mappings.items() if isinstance(v, dict) and not 'formula' in v } )
		self.field_mappings = field_mappings

                t_raw_icf_table = dbutils.Table(self.conn, res['table_name'])
                for q in self.claims_validations.get('raw_validations',{}).get('queries',[]):
		    try:
			q = q.format(**self.field_mappings)
		    except KeyError as e:
                        logutil.log(self.logger, logutil.INFO, 'KeyError when preparing query: %s' % e)
                        logutil.log(self.logger, logutil.INFO, 'Skipping query as it could not completely be resolved: %s' % q)
			continue

                    q = q.replace('INSURANCE_COMPANY_ID',str(ic_id)).replace('EMPLOYER_ID',str(e_id)).replace('IMPORTED_CLAIM_FILE_ID',str(icf_id))
                    if q != q.replace('IMPORTED_CLAIMS_TABLE',clh.stage_claim_table):
                        # Apply filter condition only if query contains icf table
                        q = q.replace('IMPORTED_CLAIMS_TABLE',clh.stage_claim_table) 
                        if filter_condition:
                            q = q + ' AND %s' % filter_condition
                    print q
                    
                    if q.find('ict.None') > 0:
                        logutil.log(self.logger, logutil.INFO, 'Skipping query as it could not completely be resolved: %s' % q)
                        continue
                    try: 
                        self.conn.cursor().execute(q)
		    except Warning as e:
			logutil.log(self.logger, logutil.INFO, 'Warnings logged while executing query: %s' % q)

        finally:        
            self.conn.close()
            self.master_conn.close()
        return

class ClaimsTestInstanceCreator:
    """

    """    
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m create_test_claims_master_instance
  -H, --Help                                              show this help message and exit
  -s SOURCE_DB_NAME, --source_db_name=SOURCE_DB_NAME      Name of source claims master database
  -t TARGET_DB_NAME, --target_db_name=TARGET_DB_NAME      Name of target claims master database
  [-c DATA_TO_COPY, --data_to_copy=DATA_TO_COPY ]         Data Artifacts to copy. Valid values are raw|normalized|raw,normalized
  [-f IMPORTED_CLAIM_FILE_IDS, --imported_claim_file_ids=IMPORTED_CLAIM_FILE_IDS]    List of Imported Claim File IDs to copy data""")
        p.add_option("-s", "--source_db_name", type="string",
                      dest="source_db_name",
                      help="Name of source claims master database.")
        p.add_option("-t", "--target_db_name", type="string",
                      dest="target_db_name",
                      help="Name of target claims master database.")
        p.add_option("-c", "--data_to_copy", type="string",
                      dest="data_to_copy", default="raw",
                      help="Data Artifacts to copy. Valid values are raw|normalized|raw,normalized")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids", 
                      help="List of Imported Claim File IDs to copy data. For e.g. 1,3,7")
        

        if not input:
            print p.usage
            sys.exit(2)
        
        (self.method_options, args) = p.parse_args(input.split(' '))
        
        if not self.method_options.source_db_name or not self.method_options.target_db_name:
            print p.usage
            sys.exit(2)            

        if self.method_options.data_to_copy:
            supported_values = set(['raw','normalized'])
            data_to_copy = set(self.method_options.data_to_copy.lower().split(','))
            
            if data_to_copy - supported_values:
                logutil.log(self.logger, logutil.CRITICAL, 'Unsupported values for data_to_copy: %s' % (list(data_to_copy - supported_values)))
                sys.exit(2)  
    
    def __init__(self, input, logger):
        
        self.logger = logger if logger else logutil.initlog('importer')
        
        self.method_options = None
        
        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)
        
        self.master_conn = getDBConnection(dbname = whcfg.master_schema,
                              host = whcfg.master_host,
                              user = whcfg.master_user,
                              passwd = whcfg.master_password,
                              useDictCursor = True)
        
        self.imported_claim_file_ids = None
        if self.method_options.imported_claim_file_ids:
            self.imported_claim_file_ids = [int(i) for i in self.method_options.imported_claim_file_ids.split(',')]

        self.cm_table_list = {'empty':['anti_transparency_list',
                                        'anti_transparency_providers',
                                        'betos',
                                        'claim_attributes',
                                        'claim_attributes_bob',
                                        'claim_provider_exceptions',
                                        'claim_provider_identifiers',
                                        'claim_specialties',
                                        'claim_specialties_bob',
                                        'claims',
                                        'claims_bob',
                                        'claims_grouper',
                                        'claims_run_logs',
                                        'data_quality_results',
                                        'eligible_employees',
                                        'eligible_members',
                                        'employee_geographies',
                                        'employee_geographies_summary',
                                        'external_procedure_code_types',
                                        'external_service_places',
                                        'external_service_types',
                                        'historical_hra_data',
                                        'icf_icdf_applied',
                                        'import_file_config',
                                        'imported_claim_dimension_files',
                                        'imported_claim_files',
                                        'imported_claim_files_insurance_companies',
                                        'insurance_company_data_files',
                                        'internal_member_ids',
                                        'labels',
                                        'metrics_benchmarks',
                                        'new_procedure_code_to_procedure_mappings',
                                        'original_claim_locations',
                                        'patients',
                                        'policies',
                                        'policy_coverages',
                                        'procedure_code_types',
                                        'procedure_codes',
                                        'procedure_labels',
                                        'procedure_modifiers',
                                        'rx_claims',
                                        'service_descriptions',
                                        'service_places',
                                        'service_types',
                                        'uploaded_files',
                                        'validation_sql'],
                              'copy':['anti_transparency_list',
                                        'anti_transparency_providers',
                                        'betos',
                                        'eligible_employees',
                                        'eligible_members',
                                        'external_procedure_code_types',
                                        'external_service_places',
                                        'external_service_types',
                                        'imported_claim_dimension_files',
                                        'imported_claim_files',
                                        'imported_claim_files_insurance_companies',
                                        'insurance_company_data_files',
                                        'metrics_benchmarks',
                                        'patients',
                                        'policies',
                                        'policy_coverages',
                                        'procedure_code_types',
                                        'procedure_codes',
                                        'procedure_labels',
                                        'procedure_modifiers',
                                        'service_descriptions',
                                        'service_places',
                                        'service_types',
                                        'validation_sql']}

    def __schemata_check(self):
        r_schemata_check = Query(self.master_conn, """SELECT count(1) as num 
                                                        FROM information_schema.SCHEMATA
                                                       WHERE SCHEMA_NAME=%s""", self.method_options.target_db_name)
        if r_schemata_check:
            n = r_schemata_check.next().get('num')
            if n:
                return True
        return False

    def __create_target_schema(self):
        cur = self.master_conn.cursor()
        cur.execute("""CREATE DATABASE %s""" % self.method_options.target_db_name)
        cur.close()

    def __resolve_icf_tables(self):
        
        icf_tables = {}
        
        if self.imported_claim_file_ids:
            r_icf_tables = Query(self.master_conn, """SELECT GROUP_CONCAT(id) as icf_ids, 
                                                            table_name 
                                                       FROM %s.imported_claim_files 
                                                      WHERE id IN (%s) 
                                                      GROUP BY table_name""" % (self.method_options.source_db_name, self.method_options.imported_claim_file_ids))
            for icf_table in r_icf_tables:
                icf_tables[icf_table.get('table_name')] = icf_table.get('icf_ids')
            
        return icf_tables

    def __initialize_target_database(self):
        
        cur = self.master_conn.cursor()
        
        for table_name in self.cm_table_list['empty']:
            logutil.log(self.logger, logutil.INFO, "Creating table: {%s.%s}" % (self.method_options.target_db_name,table_name))
            cur.execute("CREATE TABLE %s.%s LIKE %s.%s" % (self.method_options.target_db_name, table_name,
                                                           self.method_options.source_db_name, table_name))
        
        for table_name in self.cm_table_list['copy']:
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.%s}" % (self.method_options.target_db_name,table_name))
            cur.execute("INSERT INTO %s.%s SELECT * FROM %s.%s" % (self.method_options.target_db_name, table_name,
                                                                   self.method_options.source_db_name, table_name))
        
        cur.close()

    def __copy_raw_claims(self, icf_tables):
        
        cur = self.master_conn.cursor()
        
        for table_name, icf_ids in icf_tables.iteritems():
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.%s(%s)}" % (self.method_options.target_db_name, table_name, icf_ids))
            cur.execute("INSERT INTO %s.%s SELECT * FROM %s.%s WHERE imported_claim_file_id IN (%s)" % (self.method_options.target_db_name, table_name,
                                                                                                        self.method_options.source_db_name, table_name,
                                                                                                        icf_ids))
        
        cur.close()

        
    def __copy_normalized_claims(self, icf_tables):
        
        medical_icfs = [v for k,v in icf_tables.iteritems() if k <> 'medco_imported_claims']
        rx_icfs = [v for k,v in icf_tables.iteritems() if k == 'medco_imported_claims']
        
        
        cur = self.master_conn.cursor()
        
        if (medical_icfs):
        
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.claims}" % (self.method_options.target_db_name))
            cur.execute("INSERT INTO %s.claims SELECT * FROM %s.claims WHERE imported_claim_file_id IN (%s)" % (self.method_options.target_db_name,
                                                                                                                self.method_options.source_db_name,
                                                                                                                ','.join(medical_icfs)))
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.claim_attributes}" % (self.method_options.target_db_name))
            cur.execute("""INSERT INTO %s.claim_attributes 
                         SELECT ca.* 
                           FROM %s.claim_attributes ca 
                           JOIN %s.claims c ON c.id=ca.claim_id
                          WHERE c.imported_claim_file_id IN (%s)""" % (self.method_options.target_db_name,
                                                                       self.method_options.source_db_name,
                                                                       self.method_options.source_db_name,
                                                                       ','.join(medical_icfs)))
            
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.claim_specialties}" % (self.method_options.target_db_name))
            cur.execute("""INSERT INTO %s.claim_specialties 
                         SELECT cs.* 
                           FROM %s.claim_specialties cs 
                           JOIN %s.claims c ON c.id=cs.claim_id
                          WHERE c.imported_claim_file_id IN (%s)""" % (self.method_options.target_db_name,
                                                                       self.method_options.source_db_name,
                                                                       self.method_options.source_db_name,
                                                                       ','.join(medical_icfs)))
        if rx_icfs:
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.rx_claims}" % (self.method_options.target_db_name))
            cur.execute("INSERT INTO %s.rx_claims SELECT * FROM %s.rx_claims WHERE imported_claim_file_id IN (%s)" % (self.method_options.target_db_name,
                                                                                                                      self.method_options.source_db_name,
                                                                                                                      ','.join(rx_icfs)))       
        cur.close()
        
    def process(self):
        if not self.__schemata_check():
            logutil.log(self.logger, logutil.INFO, "Creating database: {%s}" % self.method_options.target_db_name)
            self.__create_target_schema()
            if self.__schemata_check():
                logutil.log(self.logger, logutil.INFO, "Successfully created database: {%s}" % self.method_options.target_db_name)
                
                icf_tables = self.__resolve_icf_tables()
                if 'raw' in self.method_options.data_to_copy and icf_tables:
                    self.cm_table_list['empty'] = self.cm_table_list['empty'] + icf_tables.keys()
                
                logutil.log(self.logger, logutil.INFO, "Initializing: {%s}" % self.method_options.target_db_name)
                self.__initialize_target_database()
                
                if 'raw' in self.method_options.data_to_copy:
                    logutil.log(self.logger, logutil.INFO, "Copying raw claims data: {%s}" % self.method_options.target_db_name)
                    self.__copy_raw_claims(icf_tables)
                
                if 'normalized' in self.method_options.data_to_copy:
                    logutil.log(self.logger, logutil.INFO, "Copying normalized claims data: {%s}" % self.method_options.target_db_name)
                    self.__copy_normalized_claims(icf_tables)
                    
        else:
            logutil.log(self.logger, logutil.CRITICAL, 'The target database {%s} already exists. Please drop the database manually if you would like to refresh it. Exiting!'% self.method_options.target_db_name) 
                 
        return

class ProviderTestInstanceCreator:
    """

    """    
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m create_test_claims_master_instance
  -H, --Help                                              show this help message and exit
  -s SOURCE_DB_NAME, --source_db_name=SOURCE_DB_NAME      Name of source claims master database
  -t TARGET_DB_NAME, --target_db_name=TARGET_DB_NAME      Name of target claims master database
  [-c DATA_TO_COPY, --data_to_copy=DATA_TO_COPY ]         Data Artifacts to copy. Valid values are raw|normalized|raw,normalized
  [-f IMPORTED_CLAIM_FILE_IDS, --imported_claim_file_ids=IMPORTED_CLAIM_FILE_IDS]    List of Imported Claim File IDs to copy data""")
        p.add_option("-s", "--source_db_name", type="string",
                      dest="source_db_name",
                      help="Name of source claims master database.")
        p.add_option("-t", "--target_db_name", type="string",
                      dest="target_db_name",
                      help="Name of target claims master database.")
        
        if not input:
            print p.usage
            sys.exit(2)
        
        (self.method_options, args) = p.parse_args(input.split(' '))
        
        if not self.method_options.source_db_name or not self.method_options.target_db_name:
            print p.usage
            sys.exit(2)            

    
    def __init__(self, input, logger):
        
        self.logger = logger if logger else logutil.initlog('importer')
        
        self.method_options = None
        
        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)
        
        self.master_conn = getDBConnection(dbname = self.method_options.source_db_name,
                              host = whcfg.master_host,
                              user = whcfg.master_user,
                              passwd = whcfg.master_password,
                              useDictCursor = True)
        
        self.cm_table_list = {'empty':['access_privileges',
                                        'audit',
                                        'bucket_mappings',
                                        'buckets',
                                        'buckets_insurance_networks',
                                        'data_source',
                                        'data_source_run',
                                        'data_source_run_step',
                                        'employer_insurer_pooling_preferences',
                                        'employers',
                                        'employers_plans',
                                        'epn',
                                        'external_procedures_map',
                                        'external_provider_exceptions',
                                        'external_specialties',
                                        'external_specialties_map',
                                        'facilities',
                                        'groups',
                                        'insurance_companies',
                                        'insurance_networks',
                                        'insurance_plan_network_mappings',
                                        'insurance_plans',
                                        'insurance_plans_networks',
                                        'internal_facility_assignments',
                                        'languages',
                                        'location_geocodes',
                                        'locations',
                                        'msi_rates',
                                        'pln_pm_attribute_value',
                                        'practitioners',
                                        'properties',
                                        'provider_external_ids',
                                        'provider_external_specialties',
                                        'providers',
                                        'providers_locations_networks',
                                        'providers_specialties',
                                        'related_specialties',
                                        'sources',
                                        'specialties',
                                        'text_labels'],
                              'copy':['access_privileges',
                                        'audit',
                                        'bucket_mappings',
                                        'buckets',
                                        'buckets_insurance_networks',
                                        'data_source',
                                        'data_source_run',
                                        'data_source_run_step',
                                        'employer_insurer_pooling_preferences',
                                        'employers',
                                        'employers_plans',
                                        'epn',
                                        'external_procedures_map',
                                        'external_provider_exceptions',
                                        'external_specialties',
                                        'external_specialties_map',
                                        'facilities',
                                        'groups',
                                        'insurance_companies',
                                        'insurance_networks',
                                        'insurance_plan_network_mappings',
                                        'insurance_plans',
                                        'insurance_plans_networks',
                                        'internal_facility_assignments',
                                        'languages',
                                        'location_geocodes',
                                        'locations',
                                        'practitioners',
                                        'properties',
                                        'provider_external_ids',
                                        'provider_external_specialties',
                                        'providers',
                                        'providers_locations_networks',
                                        'providers_specialties',
                                        'related_specialties',
                                        'sources',
                                        'specialties',
                                        'text_labels']}

    def __schemata_check(self):
        r_schemata_check = Query(self.master_conn, """SELECT count(1) as num 
                                                        FROM information_schema.SCHEMATA
                                                       WHERE SCHEMA_NAME=%s""", self.method_options.target_db_name)
        if r_schemata_check:
            n = r_schemata_check.next().get('num')
            if n:
                return True
        return False

    def __create_target_schema(self):
        cur = self.master_conn.cursor()
        cur.execute("""CREATE DATABASE %s""" % self.method_options.target_db_name)
        cur.close()



    def __initialize_target_database(self):
        
        cur = self.master_conn.cursor()
        
        for table_name in self.cm_table_list['empty']:
            logutil.log(self.logger, logutil.INFO, "Creating table: {%s.%s}" % (self.method_options.target_db_name,table_name))
            cur.execute("CREATE TABLE %s.%s LIKE %s.%s" % (self.method_options.target_db_name, table_name,
                                                           self.method_options.source_db_name, table_name))
        
        for table_name in self.cm_table_list['copy']:
            logutil.log(self.logger, logutil.INFO, "Copying data for table: {%s.%s}" % (self.method_options.target_db_name,table_name))
            cur.execute("INSERT INTO %s.%s SELECT * FROM %s.%s" % (self.method_options.target_db_name, table_name,
                                                                   self.method_options.source_db_name, table_name))
        
        cur.close()
        
    def process(self):
        if not self.__schemata_check():
            logutil.log(self.logger, logutil.INFO, "Creating database: {%s}" % self.method_options.target_db_name)
            self.__create_target_schema()
            if self.__schemata_check():
                logutil.log(self.logger, logutil.INFO, "Successfully created database: {%s}" % self.method_options.target_db_name)
                
                logutil.log(self.logger, logutil.INFO, "Initializing: {%s}" % self.method_options.target_db_name)
                self.__initialize_target_database()

        else:
            logutil.log(self.logger, logutil.CRITICAL, 'The target database {%s} already exists. Please drop the database manually if you would like to refresh it. Exiting!'% self.method_options.target_db_name) 
                 
        return



def claims_validation_report(db_name, imported_claim_file_ids=None, insurance_company_id=None, employer_id=None, \
validation_file=None, claim_type=None):
    # Identifies claims that have been normalized and raw claims that are yet to be normalized
    # Generates a validation report of raw and normalize claims
    # Requires whcfg.master_schema and whcfg.claims_master_schema to be set
    method_input = """-d %s %s %s %s %s %s""" % (db_name,
                                                '-i %s' % insurance_company_id  if insurance_company_id else '',
                                                '-e %s' % employer_id  if employer_id else '',
                                                '-f %s' % imported_claim_file_ids  if imported_claim_file_ids else '',
                                                '-v %s' % validation_file  if validation_file else '',
                                                '-t %s' % claim_type  if claim_type else ''
                                                )
    method_handler = ClaimsUtilFactory.get_instance('claims_validation_report', method_input)
    method_handler.process()
              


def profile_raw_claims(raw_claims_db=None, raw_claims_table=None, employer_id=None, imported_claim_file_ids=None):
    # Produces statistics such as fill rate and unique value distribution upon profiling raw claims table
    # Assumes a table raw_claims_profile_stats to be available in the raw_claims_db database to output stats to
    method_input = """%s %s %s %s""" % ('-f %s' % imported_claim_file_ids if imported_claim_file_ids else '',
                                        '-e %s' % employer_id if employer_id else '',
                                        '-d %s' % raw_claims_db if raw_claims_db else '',
                                        '-t %s' % raw_claims_table if raw_claims_table else ''
                                    )
    method_handler = ClaimsUtilFactory.get_instance('profile_raw_claims', method_input)
    method_handler.process()


class RawClaimsProfiler:
    """
    Generates fill rates and column value distribution stats on raw claims data
    """
    def __init_options(self, input):
        p = OptionParser(usage="""Usage: claims_util.py -m profile_raw_claims
       -H, --Help                                              show this help message and exit
       -d DB_NAME, --db_name                                   db containing the table to be profiled
       -t TABLE_NAME --table_name                              table to profile
       -e EMPLOYER_ID, --employer_id                           employer id
       -f IMPORTED_CLAIM_FILE_IDS --imported_claim_file_ids    list of imported claim file ids""")

        p.add_option("-d", "--db_name", type="string",
                      dest="db_name",
                      help="Name of claims master database")
        p.add_option("-t", "--table-name", type="string",
                      dest="table_name",
                      help="name of raw claims table")
        p.add_option("-e", "--employer_id", type="string",
                      dest="employer_id",
                      help="Employer ID")
        p.add_option("-f", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids",
                      help="comma-separated list of imported claim file IDs. e.g. 1,3,7")

        if not input:
            print p.usage
            sys.exit(2)
        (self.method_options, args) = p.parse_args(input.split(' '))

    def __init__(self, input, logger):
        self.logger = logger if logger else logutil.initlog('importer')

        self.method_options = None
        self.scratch_tables_created = set([])

        logutil.log(self.logger, logutil.INFO, '')
        self.__init_options(input)

        self.conn = getDBConnection(dbname = self.method_options.db_name,
                                    host = whcfg.master_host,
                                    user = whcfg.master_user,
                                    passwd = whcfg.master_password,
                                    useDictCursor = True)

        self.imported_claim_file_ids = None
        if self.method_options.imported_claim_file_ids:
            self.imported_claim_file_ids = [int(i) for i in self.method_options.imported_claim_file_ids.split(',')]

    def process(self):
        """
        Convert input to imported_claim_file_ids and profile the data
        """
        try:
            icf_query =  """SELECT DISTINCT icf.id
                              FROM {db}.imported_claim_files icf
                            WHERE 1=1""".format(db=self.method_options.db_name)

            if self.imported_claim_file_ids:
                icf_q1 = """ AND icf.id IN (%s)""" % self.method_options.imported_claim_file_ids
                icf_query = icf_query + icf_q1

            if self.method_options.employer_id:
                icf_q2 = """ AND icf.employer_id = %s""" % self.method_options.employer_id
                icf_query = icf_query + icf_q2

            r_query = Query(self.conn, icf_query)

            icf_ids = ''
            if r_query:
                logutil.log(self.logger, logutil.INFO, "Profiling raw claims in %s.%s..." % (self.method_options.db_name, self.method_options.table_name))
                for icf_id in r_query: icf_ids = icf_ids + ',' + str(icf_id['id'])
                self.__profile_table_data(db=self.method_options.db_name, table=self.method_options.table_name, filter_condition='imported_claim_file_id IN (%s)' % (icf_ids.strip(',')))
        except:
            traceback.print_exc()

    def __profile_table_data(self, db, table, filter_condition, column_filter=''):
        c1 = self.conn.cursor()
        c2 = self.conn.cursor()
	try:
	    imported_claim_file_id = re.findall('[0-9]+', filter_condition).pop()
	except IndexError:
	    imported_claim_file_id = ''

        tot_stmt = """SELECT COUNT(1) AS total
                      FROM {schema}.{table} mic
                      WHERE 1=1""".format(schema=db, table=table)

        results = Query(self.conn, "%s %s" % (tot_stmt, "AND %s" % filter_condition if filter_condition else ''))
        total = results.next()['total']

        stmt = """SELECT column_name
                  FROM information_schema.columns
                  WHERE table_schema = '{schema}'
                  AND table_name = '{table}'
                  AND column_name NOT LIKE '{column_filter}'""".format(schema=db, table=table, column_filter=column_filter)
        c1.execute(stmt)

        stmt = """CREATE TABLE IF NOT EXISTS {schema}.raw_claims_profile_stats (
                  id int(11) NOT NULL AUTO_INCREMENT,
                  schema_name varchar(200) DEFAULT NULL,
                  table_name varchar(200) DEFAULT NULL,
                  column_name varchar(200) DEFAULT NULL,
                  pct_fill_rate decimal(12,6) DEFAULT NULL,
                  num_distinct_values int(11) DEFAULT NULL,
                  value_distribution varchar(2000) DEFAULT NULL,
                  filter_condition varchar(2000) DEFAULT NULL,
		  imported_claim_file_id varchar(255) DEFAULT NULL,
                  create_date datetime DEFAULT NULL,
                  PRIMARY KEY (id)
                ) ENGINE=MyISAM DEFAULT CHARSET=latin1""".format(schema=db)
        c2.execute(stmt)
        while(1):
            row = c1.fetchone()
            if not row:
                c1.close()
                break
            current_column = row['column_name']
            warnings.filterwarnings("ignore", "Incorrect date value*")
            warnings.filterwarnings("ignore", "Incorrect datetime value")
            logutil.log(self.logger, logutil.INFO, "Profiling column: %s" % (current_column))

            s1 = """SELECT COUNT(DISTINCT {column}) AS num_dist_values
                    FROM {schema}.{table} t
                    WHERE 1=1""".format(column=current_column, schema=db, table=table)
            r1 = Query(self.conn, "%s %s" % (s1, "AND %s" % filter_condition if filter_condition else ''))
            num_dist_values = r1.next()['num_dist_values']

            s2 = """SELECT 100*COUNT(1)/{total} AS fill_rate
                    FROM {schema}.{table} t
                    WHERE {column} IS NOT NULL
                    AND {column} != ''""".format(total=total,column=current_column, schema=db, table=table)
            r2 = Query(self.conn, "%s %s" % (s2, "AND %s" % filter_condition if filter_condition else ''))
            fill_rate = r2.next()['fill_rate']

            distribution = ''
            if num_dist_values < 51:
                s3 = """SELECT GROUP_CONCAT(
			    DISTINCT CONCAT(' ', 
				IF(a.{column} IS NULL OR a.{column} = '', 
				    '(NULL)', 
				    a.{column}
				), 
				': ', 
				ROUND(100*a.cnt/{total}, 2), 
				'%') 
			    ORDER BY a.{column} SEPARATOR '\n'
			) AS distribution
                        FROM (SELECT {column}, COUNT(1) AS cnt
                              FROM {schema}.{table}
                              WHERE 1=1
                              {filter}
                              GROUP BY {column}) a"""\
		    .format(total=total,
			    column=current_column, 
			    schema=db, 
			    table=table, 
			    filter="AND %s" % filter_condition if filter_condition else '')
                r3 = Query(self.conn, s3)
                distribution = r3.next()['distribution']

            s5 = """INSERT INTO raw_claims_profile_stats (
			    schema_name, 
			    table_name, 
			    column_name, 
			    pct_fill_rate, 
			    num_distinct_values, 
			    value_distribution, 
			    filter_condition, 
			    imported_claim_file_id,
			    create_date)
                    VALUES ("{schema_name}", 
			    "{table_name}", 
			    "{column}", 
			    "{pct_fill_rate}", 
			    "{num_distinct_values}", 
			    "{distribution}", 
			    "{filter_condition}", 
			    "{imported_claim_file_id}", 
			    NOW())"""\
		.format(schema_name=db, 
			table_name=table, 
			column=current_column, 
			num_distinct_values=num_dist_values, 
			pct_fill_rate=fill_rate, 
			distribution=distribution, 
			filter_condition=filter_condition, 
			imported_claim_file_id = imported_claim_file_id)
            c2.execute(s5)
        c1.close()
        c2.close()


def __init_options():
    
    supported_methods = ','.join(helpers.keys())
    
    usage="""claims_util.py
            -m method_name
               Supported methods: %s
            -i method_inputs
            [-H] help for method_name""" % (supported_methods)
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--method_name", type="string",
                      dest="method_name",
                      help="Name of method to run. Supported methods: %s" % supported_methods)
    parser.add_option("-i", "--method_inputs", type="string",
                      dest="method_inputs",
                      help="Inputs to method.")
    parser.add_option("-H", "--method_help", 
                      action="store_true",
                      default=False,
                      dest="method_help",
                      help="Help for method.")    
    (claims_util_options, args) = parser.parse_args()

    if ((not claims_util_options.method_name)):
        print 'usage:', usage
        sys.exit(2)

    return claims_util_options

if __name__ == "__main__":

    logutil.log(LOG, logutil.INFO, "Claims Util Start!\nCreating Connection objects...")

    claims_util_options = __init_options()
    
    method_name = claims_util_options.method_name
    
    method_handler = ClaimsUtilFactory.get_instance(method_name, claims_util_options.method_inputs, LOG)
    method_handler.process() 
