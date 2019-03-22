import whcfg
import logutil
import MySQLdb
import traceback
import datetime
from dbutils import *
from model import *
from statsutil import *
import pprint
from cStringIO import StringIO
from claims_load_helper import *
from argparse import ArgumentParser
from math import ceil
import utils
import types
import whcfg
import subprocess
import pickle
import shlex
from django_email import DjangoEmail

LOG = logutil.initlog('idc_sync')
st = Stats("idc_sync_ventana")
template_dir = '%s/claims/import/common/templates' % whcfg.providerhome
BATCH_SIZE = 100000


class SyncController(object):
    """Object of this class is responsible for creating connection objects,
    initialising source and destination host,schema,table and controlling entire sync operation.
    """
    
    def __init__(self, table, s_host, s_db, s_user, s_password, d_host, d_db, d_user, d_password, filter):
        logutil.log(LOG, logutil.INFO, "Started initialising parameters.")
        self.s_host = s_host
        self.s_db = s_db
        self.s_user = s_user
        self.s_password = s_password
        
        self.d_host = d_host
        self.d_db = d_db
        self.d_user = d_user
        self.d_password = d_password
        
        self.table = table
        self.filter = filter
        self.create_db_connection()
        self.sync_failures = []
        
        self.notification_email = ""
        self.failure_email_subject = 'Sync process failed'
        
    def create_db_connection(self):
        
        cid = st.start("connections", "Creating Connection Objects.")
        self.s_conn = getDBConnection(dbname = self.s_db,
                                  host = self.s_host,
                                  user = self.s_user,
                                  passwd = self.s_password,
                                  useDictCursor = True)
        
        self.d_conn = getDBConnection(dbname = self.d_db,
                                  host = self.d_host,
                                  user = self.d_user,
                                  passwd = self.d_password,
                                  useDictCursor = True)
        st.end(cid)
        
    
    def sync(self):
            
        sync_status = self.dump_restore()
        if sync_status !=0 :
            self.sync_failures.append({'table': self.table, 'error': sync_status})  
          
    def dump_restore(self, create_table = False, auto_delete = False):
        """Takes the backup, restores it to a temporary table, inserts the data from temporary table to main table and then deletes the temporary table
        """            

        sync_status = 0
        d_query = []

        for table in self.table: 
            where_clause = self.filter[table] if self.filter and self.filter[table] else None
            c_query = Query(self.s_conn, "SELECT count(*) FROM %s.%s %s" % (self.s_db, table, "where %s" % where_clause if where_clause else ""))
            count = c_query.next()['count(*)']
            n_batches = ceil(float(count)/BATCH_SIZE)
            #s_query = """ select * from %s where %s """ % (table, '%s')
            for batch in xrange(int(n_batches)):
                offset =  (batch) * BATCH_SIZE 
                
                backup_options = """-h %s -u %s --password='%s' %s %s --where="%s" -e %s""" %(self.s_host, self.s_user, self.s_password, self.s_db, table, where_clause+" limit %s,%s" %(offset, BATCH_SIZE) if where_clause else "TRUE limit %s,%s" %(offset, BATCH_SIZE),"-t" if create_table == False else "")
                restore_options = "-h %s -u %s --password='%s' -C -D %s" %(self.d_host, self.d_user, self.d_password, self.d_db)
                dump_cmd = "mysqldump %s | mysql %s" % (backup_options, restore_options)
                
                try:
                    cid = st.start("sync_process", "Sync process started.")
                    
                    logutil.log(LOG, logutil.INFO, "Syncing batch %s  of table %s::Command--" % (batch + 1,table))
                    sync_process = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    output, err = sync_process.communicate()
                    st.end(cid)
                    
                    for error in err.split('\n'):
                        if error.strip() == "Warning: Using a password on the command line interface can be insecure." or error.strip() == "":
                            continue
                        else:
                            sync_status = err
                            break
                    if sync_status == 0:
                        continue
                    break
                except:
                    i = sys.exc_info()
                    sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                    break
            if sync_status == 0 and auto_delete:    
                d_query.append({'query': 'delete FROM %s.%s %s'%(self.d_db, table, "where %s" % where_clause if where_clause else ""),
                            'description': 'reverting data from table %s' % table,
                            'warning_filter':'ignore'})

            if sync_status != 0:
                try:
                    logutil.log(LOG, logutil.INFO, "Some error occured in sync process. Reverting already synced data. Table: %s" % (table))
                    self.d_conn.ping( True )
                    if d_query:
                        utils.execute_queries(self.d_conn, LOG, d_query, dry_run = False)
                except:
                    i = sys.exc_info()
                    sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                break
            
        return sync_status

    def run_update_sql(self, sql_location):
        sync_status = 0
        try:
            file = open(sql_location, 'r')
            sql = s = " ".join(file.readlines())
            x = sql.find('drop')
            y = sql.find('delete')
            if (x != -1 or y != -1):
                logutil.log(LOG, logutil.INFO, "Found drop/delete in sql file. Not proceeding with update.")
                sync_status = "error"
                return sync_status
            print "Start executing: " + sql_location + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")) + "\n" + sql
            self.d_conn.ping( True )
            d_cursor = self.d_conn.cursor()
            d_cursor.execute(sql)
        except:
            i = sys.exc_info()
            sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
        #print type(update_queries)
        #utils.execute_queries(self.d_conn, LOG, update_queries, dry_run = False) 
        #cursor = connection.cursor()
        #cursor.execute(sql) 
        return sync_status
    
    def __del__(self):
        """ Close the created db connections
        """
        self.s_conn.close()
        self.d_conn.close()
    
    
    def notify_sync_failure(self):
        """ Sends user alerts via email messages 
        """
        recipients = [each_user.strip() for each_user in self.notification_email.split(',')]
         
        for failure in self.sync_failures:
            
            from_email = '<wh_ops@castlighthealth.com>'
            template_name = 'sync_failure_email'
            context_data = failure
            DjangoEmail(template_dir, logutil, LOG).send_email_template(template_name, context_data, self.failure_email_subject, recipients, from_email)
        conn_cursor.close()
        conn.close()
        
        
        
class IDCSyncFilesController(SyncController):
    proc_icf_ids = []
    def __init__(self, input):
        self.__init_options(input)
        
        self.s_host = whcfg.claims_master_host
        self.s_db = whcfg.claims_master_export_schema
        self.s_user = whcfg.claims_master_user
        self.s_password = whcfg.claims_master_password
        
        self.d_host = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'host')
        self.d_db = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'schema')
        self.d_user = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'user')
        self.d_password = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'password')
        
        self.table = ['exported_claim_files']    
        if self.sync_options.claim_type == 'medical_claims':
            self.table.append("identified_claims")
        elif self.sync_options.claim_type == 'pharmacy_claims':
            self.table.append("rx_claims")
        else:
            self.table.append("dental_claims")   

        #if self.sync_options.claim_type == 'pharmacy_claims':
         #   self.table = ['rx_claims'] 

        self.filter = {}
        self.icf_ids = []
        self.sync_failures = []
        self.create_db_connection()
        self.claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = self.s_host,
                                  user = self.s_user,
                                  passwd = self.s_password,
                                  useDictCursor = True)
                
    def __init_options(self, input):
        """Initialises the input parameters
        """
        parser = ArgumentParser(description='IDC sync claim files to ventana.')
    
        parser.add_argument("-p", "--payer", type=str,
                          dest="payer_id",
                          help="payer id for which to export identified claims to ventana")
        
        parser.add_argument("-e", "--employer", type=str,
                          dest="employer_id",
                          help="employer id for which to export identified claims to ventana")
    
        parser.add_argument("-i", "--imported_claim_file_ids", type=str,
                          dest="imported_claim_file_ids",
                          help="Comma separated list of Imported Claim File IDs whose claims are to be exported")
        
        parser.add_argument("-a", "--all_claims",
                          action="store_true",
                          dest="all_claims",
                          default=False,
                          help="Export ALL Claims for specific payer or employer or both")
        
        parser.add_argument("-t", "--claim_type",
                          type=str,
                          dest="claim_type",
                          default="medical_claims",
                          help="Claim File Type",
                          choices = ['medical_claims','pharmacy_claims','dental_claims'])
        
        parser.add_argument("-d", "--dest_env",
                          type=str,
                          dest="dest_env",
                          help="Destination environment",
                          choices = ['prod','preprod','qa'])

        parser.add_argument("-n", "--notify_failure",
                          action = "store_true",
                          default = False,
                          dest = "notify_failure",
                          help = "Send failure notifications")

        parser.add_argument("-m", "--notification_email",
                          type = str,
                          dest = "notification_email",
                          help = "Comma separated list of recipients")

        self.sync_options = parser.parse_args(shlex.split(input))
        if ((not self.sync_options.payer_id and not self.sync_options.employer_id and not self.sync_options.imported_claim_file_ids) or not self.sync_options.dest_env):
            parser.print_help()
            sys.exit(2)
        if (self.sync_options.imported_claim_file_ids and self.sync_options.all_claims):
            logutil.log(LOG, logutil.INFO, 'Ambiguous input! Not clear whether to sync for imported_claim_file_id: %s, or whether to run for ALL files.' % self.sync_options.imported_claim_file_ids)
            logutil.log(LOG, logutil.INFO, '%s'% parser.print_help())
            sys.exit(2)
        if self.sync_options.notify_failure == True and not self.sync_options.notification_email:
            logutil.log(LOG, logutil.INFO, "Email notification ids not provided")
            logutil.log(LOG, logutil.INFO, '%s'% parser.print_help())
            sys.exit(2)
        elif self.sync_options.notify_failure == True and self.sync_options.notification_email:
            self.notification_email = self.sync_options.notification_email
            
        if self.sync_options.imported_claim_file_ids and self.sync_options.imported_claim_file_ids.split(','):
            self.sync_options.imported_claim_file_ids = set([int(x) for x in self.sync_options.imported_claim_file_ids.split(',')])
            
        
    def get_icfids_to_sync(self):
        
        """Filters imported claim file ids depending on input parameters
        """
        logutil.log(LOG, logutil.INFO, "Getting imported claim file ids that need to be synced")
        icf_query = """SELECT GROUP_CONCAT(DISTINCT icf.id ORDER BY icf.id) as icf_ids
                    FROM imported_claim_files icf
                    JOIN uploaded_files uf ON uf.prod_imported_claim_file_id = icf.id
                    JOIN import_file_payor_config ipc ON uf.insurance_company_id = ipc.insurance_company_id and uf.file_type = ipc.file_type {payor_config}
                    JOIN import_file_employer_config iec ON iec.employer_id = uf.employer_id and ipc.id =  iec.payor_info_config_id {employer_config} 
                    where {file_config}
                    AND uf.file_type='%s'""" % self.sync_options.claim_type.lower()

                                                
        if self.sync_options.dest_env == 'preprod':
            icf_query = icf_query.format(payor_config = '',employer_config = '',file_config = 'True')
        elif self.sync_options.dest_env == 'qa':
            icf_query = icf_query.format(payor_config = 'and ipc.sync_flag = 1',employer_config = 'and iec.load_state = 4',file_config = "uf.prod_status = 'claims-exported' and icf.sync_flag = 1")
        else:
            icf_query = icf_query.format(payor_config = 'and ipc.sync_flag = 1',employer_config = 'and (iec.load_state = 5 or iec.load_state = 0)',file_config = "uf.prod_status = 'claims-exported' and icf.sync_flag = 1")
        
           
        if self.sync_options.employer_id:
            icf_query = icf_query + """ AND uf.employer_id=%d""" % (self.sync_options.employer_id)
    
        if self.sync_options.payer_id:
            icf_query = icf_query + """ AND uf.insurance_company_id=%d""" % (self.sync_options.payer_id)
                
        if self.sync_options.imported_claim_file_ids:
            icf_query = icf_query + """ AND icf.id IN (%s)""" % ','.join([str(x) for x in self.sync_options.imported_claim_file_ids])
        print icf_query
        
        r_icf_query = Query(self.claims_master_conn, icf_query)
        
        s_all_icf_ids = r_icf_query.next().get('icf_ids') if r_icf_query else ''
        all_icf_ids = [int(x) for x in s_all_icf_ids.split(',')] if s_all_icf_ids else None
        
        self.icf_ids  = all_icf_ids    

    def __update_filter(self, icf_id):
        for table in self.table:
            self.filter[table] = "imported_claim_file_id = %s" % icf_id
            if table == 'identified_claims' or table == 'rx_claims':
                self.filter[table] += " and subscriber_patient_id <> -1"
     
    def sync(self):
        """Main function which calls other function and carries out sync operation
        """    
            
        sync_status = None
        self.get_icfids_to_sync()
        print self.icf_ids
        if self.icf_ids:
            for icf_id in self.icf_ids:
                #if self.sync_options.claim_type == 'medical_claims':
                #    table_name = 'identified_claims'
               #     claim_type =  'medical'
                #elif self.sync_options.claim_type == 'pharmacy_claims':
                #    table_name = 'rx_claims'
                #    claim_type = 'rx'
                #elif self.sync_options.claim_type == 'dental_claims':
                #    table_name = 'dental_claims' 
                #    claim_type = 'dental'

            #for icf_id in self.icf_ids:
                log_insert = """INSERT INTO idc_sync_log
                                (sync_type, file_label_id, environment, count, date)
                                SELECT '%s File', %s, '%s', count(*), CURDATE() from %s.%s where imported_claim_file_id = %s and subscriber_patient_id <> -1
                            """ % ('Medical' if self.sync_options.claim_type == 'medical_claims' else 'Rx', icf_id,
                             self.sync_options.dest_env, self.s_db,
                             'identified_claims' if self.sync_options.claim_type == 'medical_claims' else 'rx_claims',
                              icf_id)
                Query(self.claims_master_conn, log_insert)            
                self.__class__.proc_icf_ids.append(icf_id)
                self.__update_filter(icf_id)
                with Timer() as t:
                    sync_status = self.dump_restore(auto_delete = True)
                u_sync_status = """update uploaded_files set prod_status = %s where 
                                       prod_imported_claim_file_id = %s"""
                u_log_record = """update idc_sync_log set status = %s, error_msg= %s, run_time= SEC_TO_TIME(FLOOR('{time}')) where id = LAST_INSERT_ID()""".format(time = t.interval)
                if sync_status == 0:
                    #log sync process completed
                    logutil.log(LOG, logutil.INFO, "Sync process completed...Now updating the status of the files that are synced")
                    try:                
                        self.claims_master_conn.ping( True )
                        cm_cursor = self.claims_master_conn.cursor()
                        if self.sync_options.dest_env == 'qa' or self.sync_options.dest_env == 'prod':
                            cm_cursor.execute( u_sync_status, ('loaded-production' if self.sync_options.dest_env == 'prod' else 'loaded-qa', icf_id))
                        cm_cursor.execute(u_log_record, ('success', ''))
                        
                    except:
                        i = sys.exc_info()
                        sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR, "Error occured while updating the status of file: %s" % sync_status)
                        
                if sync_status != 0:
                    logutil.log(LOG, logutil.ERROR, "Error occured during sync process: %s" % sync_status)
                    try:                
                        self.claims_master_conn.ping( True )
                        cm_cursor = self.claims_master_conn.cursor()
                        if self.sync_options.dest_env == 'qa' or self.sync_options.dest_env == 'prod':
                            cm_cursor.execute( u_sync_status, ('loading-production-failed' if self.sync_options.dest_env == 'prod' else 'loading-qa-failed', icf_id))
                        cm_cursor.execute(u_log_record, ('failure', sync_status))
                        
                    except:
                        i = sys.exc_info()
                        sync_status += ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR, "Error occured while updating the status of file: %s" % sync_status)
                    
                    self.sync_failures.append({'file_id': icf_id, 'error': sync_status })
                sync_status = None
            
        else:
            logutil.log(LOG, logutil.INFO, "No claim file to be synceddfghdgfhjdgfjdhgfjhg")
        
        if self.sync_options.notify_failure == True:
            self.notify_sync_failure()
    
    
    def notify_sync_failure(self):
        s_query ="""select id,insurance_company_name,employer_key,file_type from uploaded_files where prod_imported_claim_file_id = %s """
        recipients = [each_user.strip() for each_user in self.notification_email.split(',')]
        from_email = '<wh_ops@castlighthealth.com>'
        template_name = 'sync_failure_email'
        
        cm_cursor = self.claims_master_conn.cursor()
        
        for failure in self.sync_failures:
            timestamp = datetime.date.today()
            cm_cursor.execute(s_query , (failure['file_id'], ))
            f_info = cm_cursor.fetchone()
            subject = "Claims Push To %s - Failed - %s - %s - %s" % (self.sync_options.dest_env, f_info['employer_key'], f_info['insurance_company_name'], str(timestamp))
            subject = " ".join(subject.split())
            
            context_data = {
                                'Name of Employer': f_info['employer_key'],
                                'Name of Payer': f_info['insurance_company_name'],
                                'File Id': f_info['id'],
                                'Imported Claim File Id': failure['file_id'],
                                'Date': str(timestamp),
                                'File Type': f_info['file_type'],
                                'Error': failure['error'],
                                }
        
            DjangoEmail(template_dir, logutil, LOG).send_email_template(template_name, context_data, subject, recipients, from_email)
        cm_cursor.close()

    def __del__(self):
        """ Close the created db connections
        """
        self.s_conn.close()
        self.d_conn.close()
        self.claims_master_conn.close()
    
    @staticmethod
    def sent_sync_summary(claims_master_conn, recipients):
        if len(IDCSyncFilesController.proc_icf_ids) != 0:
            s_query ="""SELECT uf.prod_imported_claim_file_id as icf_id,
                        uf.insurance_company_name, 
                        uf.employer_key, 
                        uf.file_type, 
                        isl.status,
                        isl.count,
                        isl.run_time 
                        FROM uploaded_files uf
                        JOIN idc_sync_log isl ON isl.file_label_id = uf.prod_imported_claim_file_id
                        JOIN (select max(id) as id from idc_sync_log group by file_label_id) temp ON temp.id = isl.id
                        WHERE uf.prod_imported_claim_file_id in (%s) and (isl.sync_type = 'Medical File' or isl.sync_type = 'Rx File') order by uf.file_type,uf.insurance_company_name,uf.employer_key,uf.prod_imported_claim_file_id """ % ",".join(str(icf_id) for icf_id in IDCSyncFilesController.proc_icf_ids)
            print s_query
                    
            cm_cursor = claims_master_conn.cursor()
            cm_cursor.execute(s_query)
            file_info = cm_cursor.fetchall()
        else:
            file_info = []

        timestamp = datetime.date.today()
        subject = "IDC sync summary - %s" % str(timestamp)
        from_email = '<wh_ops@castlighthealth.com>'
        template_name = 'sync_summary'
    
        
        DjangoEmail(template_dir, logutil, LOG).send_email_template(template_name, file_info, subject, recipients, from_email)

class IDCSyncClaimsController(SyncController):
    proc_label_ids = []
    def __init__(self, input):
        self.__init_options(input)
        
        self.s_host = whcfg.claims_master_host
        self.s_db = whcfg.claims_master_export_schema
        self.s_user = whcfg.claims_master_user
        self.s_password = whcfg.claims_master_password
        
        self.d_host = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'host')
        self.d_db = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'schema')
        self.d_user = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'user')
        self.d_password = whcfg.__get__(whcfg.cfg,'idc_sync_%s' % self.sync_options.dest_env, 'password')
        
            
        self.filter = {}
        self.table = {'identified_claims','rx_claims'}
        self.l_ids = self.sync_options.l_ids
        self.l_type = self.sync_options.l_type
        self.sync_failures = []
        self.create_db_connection()
        self.claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = self.s_host,
                                  user = self.s_user,
                                  passwd = self.s_password,
                                  useDictCursor = True)
                
    def __init_options(self, input):
        """Initialises the input parameters
        """
        parser = ArgumentParser(description='IDC sync claim files to ventana.')
    
        parser.add_argument("-l", "--lids", type=str,
                          dest="l_ids",
                          help="Label id the claims to be synced")

        parser.add_argument("-t", "--label_type", type=str,
                          dest="l_type",
                          help="Label type")
        
        parser.add_argument("-d", "--dest_env",
                          type=str,
                          dest="dest_env",
                          help="Destination environment",
                          choices = ['prod','preprod','qa'])

        parser.add_argument("-n", "--notify_failure",
                          action = "store_true",
                          default = False,
                          dest = "notify_failure",
                          help = "Send failure notifications")

        parser.add_argument("-m", "--notification_email",
                          type = str,
                          dest = "notification_email",
                          help = "Comma separated list of recipients")

        self.sync_options = parser.parse_args(shlex.split(input))
        if (not self.sync_options.l_ids or not self.sync_options.dest_env):
            parser.print_help()
            sys.exit(2)
            
        if self.sync_options.notify_failure == True and not self.sync_options.notification_email:
            logutil.log(LOG, logutil.INFO, "Email notification ids not provided")
            logutil.log(LOG, logutil.INFO, '%s'% parser.print_help())
            sys.exit(2)
        elif self.sync_options.notify_failure == True and self.sync_options.notification_email:
            self.notification_email = self.sync_options.notification_email
        
        if self.sync_options.l_ids and self.sync_options.l_ids.split(','):
            self.sync_options.l_ids = set([int(x) for x in self.sync_options.l_ids.split(',')])
            
        
    def __create_dump_table(self, label_id):
        
        """Creates table under scratch schema to dump
        """
        logutil.log(LOG, logutil.INFO, "Creating temporary tables to sync")
        
        dump_table_queries = []
        d_ic = """DROP TABLE IF EXISTS {scratch_schema}.identified_claims""".format(scratch_schema = whcfg.scratch_schema)
        c_ic = """CREATE TABLE {scratch_schema}.identified_claims LIKE {s_schema}.identified_claims""".format(scratch_schema = whcfg.scratch_schema, s_schema = self.s_db)
        i_ic = """INSERT into {scratch_schema}.identified_claims\
                  SELECT ic.* FROM {s_schema}.identified_claims ic
                  JOIN idc_claim_label_mapping iclm ON ic.id = iclm.claim_id
                  WHERE iclm.label_id = {label_id} and iclm.claim_type = 'M'""".format(scratch_schema = whcfg.scratch_schema, s_schema = self.s_db, label_id = label_id)
        d_rc = """DROP TABLE IF EXISTS {scratch_schema}.rx_claims""".format(scratch_schema = whcfg.scratch_schema)
        c_rc = """CREATE TABLE {scratch_schema}.rx_claims LIKE {s_schema}.rx_claims""".format(scratch_schema = whcfg.scratch_schema, s_schema = self.s_db)
        i_rc = """INSERT into {scratch_schema}.rx_claims\
                  SELECT rc.* FROM {s_schema}.rx_claims rc
                  JOIN idc_claim_label_mapping iclm ON rc.id = iclm.claim_id
                  WHERE iclm.label_id = {label_id} and iclm.claim_type = 'R'""".format(scratch_schema = whcfg.scratch_schema, s_schema = self.s_db, label_id = label_id)
                  
        dump_table_queries.extend([{'query':d_ic,
                                      'description':'Drop table if exists {scratch_schema}.identified_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},
                                     {'query':c_ic,
                                      'description':'Create table {scratch_schema}.identified_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},
                                     {'query':i_ic,
                                      'description':'Insert into {scratch_schema}.identified_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},
                                     {'query':d_rc,
                                      'description':'Drop table if exists {scratch_schema}.rx_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},
                                     {'query':c_rc,
                                      'description':'Create table {scratch_schema}.rx_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},
                                     {'query':i_rc,
                                      'description':'Insert into {scratch_schema}.rx_claims'.format(scratch_schema = whcfg.scratch_schema),
                                      'warning_filter':'ignore'},])
        utils.execute_queries(self.claims_master_conn, LOG, dump_table_queries)
        self.__set_s_db(whcfg.scratch_schema)
                  
    def __set_s_db(self, db):
        self.s_db = db    
                  
                   
                    
            
         

    def sync(self):
        """Main function which calls other function and carries out sync operation
        """   
            
        sync_status = None
        if self.l_ids:
            for label_id in self.l_ids:
                log_insert = """INSERT INTO idc_sync_log
                                (sync_type, file_label_id, environment, count, date)
                                SELECT 'Label', %s, '%s', count(*), CURDATE() from %s where label_id = %s
                             """ % (label_id,
                                    self.sync_options.dest_env, 
                                    'idc_claim_label_mapping',
                                    label_id)
                Query(self.claims_master_conn, log_insert)            
                self.__class__.proc_label_ids.append(label_id)
                
                if self.l_type == 'insert':
                    self.__create_dump_table(label_id)
                    with Timer() as t:
                        sync_status = self.dump_restore()
                    self.__set_s_db(whcfg.claims_master_export_schema)
                else:
                    sql_location_query = """select sql_location from idc_claim_labels where id = %s""" % (label_id)
                    try:
                        self.claims_master_conn.ping( True )
                        cm_cursor = self.claims_master_conn.cursor()
                        cm_cursor.execute(sql_location_query)
                        sql_location_info = cm_cursor.fetchone()
                        sql_location = sql_location_info['sql_location']
                        logutil.log(LOG, logutil.INFO, "sql locaition : %s" % (sql_location))
                        with Timer() as t:
                            sync_status = self.run_update_sql(sql_location)
                    except:
                        i = sys.exc_info()
                        sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR, "Error occured while fetching sql location for : %s" % label_id) 
                    #logutil.log(LOG, logutil.INFO, "Skipping sync process for %s" % (label_id))
                u_log_record = """update idc_sync_log set status = %s, error_msg= %s, run_time= SEC_TO_TIME(FLOOR('{time}')) where id = LAST_INSERT_ID()""".format(time = t.interval)
                u_sync_status = """update idc_claim_labels set sync_status = 1 where id = %s""" % (label_id)
                if sync_status == 0:
                    #log sync process completed
                    logutil.log(LOG, logutil.INFO, "Sync process completed...Now updating log status")
                    try:                
                        self.claims_master_conn.ping( True )
                        cm_cursor = self.claims_master_conn.cursor()
                        cm_cursor.execute(u_sync_status)
                        cm_cursor.execute(u_log_record, ('success', ''))
                        cm_cursor.execute(u_sync_status)
                    except:
                        i = sys.exc_info()
                        sync_status = ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR, "Error occured while updating the status of file: %s" % sync_status)
                        
                if sync_status != 0:
                    logutil.log(LOG, logutil.ERROR, "Error occured during sync process: %s" % sync_status)
                    try:                
                        self.claims_master_conn.ping( True )
                        cm_cursor = self.claims_master_conn.cursor()
                        cm_cursor.execute(u_log_record, ('failure', sync_status))
                        
                    except:
                        i = sys.exc_info()
                        sync_status += ''.join(traceback.format_exception(i[1],i[0],i[2]))
                        logutil.log(LOG, logutil.ERROR, "Error occured while updating the status of file: %s" % sync_status)
                    
                    self.sync_failures.append({'Label_Id': label_id, 'error': sync_status })
                sync_status = None
            
        else:
            logutil.log(LOG, logutil.INFO, "No claim labels to be synced")
        
        if self.sync_options.notify_failure == True:
            self.notify_sync_failure()
    
    def notify_sync_failure(self):
        s_query ="""select id, label from idc_claim_labels where id = %s """
        recipients = [each_user.strip() for each_user in self.notification_email.split(',')]
        from_email = '<wh_ops@castlighthealth.com>'
        template_name = 'sync_failure_email'

        cm_cursor = self.claims_master_conn.cursor()

        for failure in self.sync_failures:
            timestamp = datetime.date.today()
            cm_cursor.execute(s_query , (failure['Label_Id'], ))
            l_info = cm_cursor.fetchone()
            subject = "Claims Push To %s - Failed - Label:%s - %s " % (self.sync_options.dest_env, l_info['label'], str(timestamp))
            subject = " ".join(subject.split())

            context_data = {
                                'Label Id': failure['Label_Id'],
                                'Label': l_info['label'],
                                'Date': str(timestamp),
                                'Error': failure['error'],
                                }

            DjangoEmail(template_dir, logutil, LOG).send_email_template(template_name, context_data, subject, recipients, from_email)
        cm_cursor.close()
    
    def __del__(self):
        """ Close the created db connections
        """
        self.s_conn.close()
        self.d_conn.close()
        self.claims_master_conn.close()
        
    @staticmethod
    def sent_sync_summary(claims_master_conn, recipients):
        if len(IDCSyncClaimsController.proc_label_ids) != 0:
            s_query ="""SELECT isl.file_label_id,
                        icl.label,
                        isl.count,
                        isl.run_time,
                        isl.status 
                        FROM idc_sync_log isl 
                        JOIN (select max(id) as id from idc_sync_log group by file_label_id) temp ON temp.id = isl.id
                        JOIN idc_claim_labels icl ON icl.id = isl.file_label_id
                        WHERE isl.file_label_id in (%s) and isl.sync_type = 'Label' """ % ",".join(str(label_id) for label_id in IDCSyncClaimsController.proc_label_ids)
                   
            print s_query 
            cm_cursor = claims_master_conn.cursor()
            cm_cursor.execute(s_query)
            label_info = cm_cursor.fetchall()
        else:
            label_info = []

        timestamp = datetime.date.today()
        subject = "IDC sync summary - %s" % str(timestamp)
        from_email = '<wh_ops@castlighthealth.com>'
        template_name = 'sync_summary_labels'
    
        
        DjangoEmail(template_dir, logutil, LOG).send_email_template(template_name, label_info, subject, recipients, from_email)

        
class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

helpers = {'sync_files' : lambda input : IDCSyncFilesController(input),
           'sync_claims' : lambda input : IDCSyncClaimsController(input)}

def __init_options__(args):
    parser = ArgumentParser(description='Sync input parser')
    
    parser.add_argument("-m", "--method_name", type=str,
                          dest="method_name",
                          help="Sync method name", choices = ['sync_files','sync_claims'])
        
    parser.add_argument("-i", "--method_input", type=str,
                          dest="method_input",
                          help="Input to sync method")
    
    sync_input = parser.parse_args(args)
    
    if( not sync_input.method_name or not sync_input.method_input):
        parser.print_help()
        sys.exit(2)
    return sync_input

def main(args=None):
    logutil.log(LOG, logutil.INFO, "IDC sync start.")
    stats_report = open('idc_test.stats','a')
    cid_all = st.start("everything", "Track how long it takes for the entire script to run!")
    
    sync_input = __init_options__(args)
    sync_handler = helpers[sync_input.method_name](sync_input.method_input)
    sync_status = sync_handler.sync()

    st.end(cid_all)
    stats_report.write(st.report())
    stats_report.close() 
    
if __name__ == "__main__":
    main()
