from cStringIO import StringIO
from claims_load_helper import *
from dental_bulk_loader import *
from dbutils import *
from model import *
from optparse import OptionParser
from statsutil import *
import MySQLdb
import datetime
import logutil
import os
import pprint
import signal
import sys
import traceback
import whcfg
import dbutils
from claims_util import *


LOG = logutil.initlog('importer')
st = Stats("load_dental_claims_to_master")


def __init_options__(args):
    usage="""load_claims_to_master.py
            -i <imported_claim_file_id>
            -p <load properties file>
            [-d]
            """
            
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "--imported_claim_file_id", type="string",
                      dest="imported_claim_file_id",
                      help="ID of the Imported Claim File")

    parser.add_option("-p", "--load_properties_file", type="string",
                      dest="load_properties_file", default = "",
                      help="Load Properties File")

    parser.add_option("-d", "--dry_run",
                      action="store_true",
                      dest="dry_run",
                      default=False,
                      help="Dry Run only. Will not write to database.")  

    (dental_claims_load_options, args) = parser.parse_args(args)

    if ((not dental_claims_load_options.imported_claim_file_id)):
        print 'usage:', usage
        sys.exit(2)

    return dental_claims_load_options

def __import_dental_claims_bulk_generic(claims_master_conn, master_conn, imported_claims_file_id, load_properties_file = None, dry_run = False):

    load_properties_text = None
    if (load_properties_file):
        load_properties = open(load_properties_file, 'r')
        load_properties_text = load_properties.read()
        update_lpt = """UPDATE %s.imported_claim_files SET load_properties = %s where id = %s""" % (whcfg.claims_master_schema, '%s', imported_claims_file_id)
        claims_master_conn.cursor().execute(update_lpt, load_properties_text) 
        
    #fac_claims_loader = DClaimsBulkLoaderFactory.get_instance(claims_master_conn, master_conn, imported_claims_file_id, dry_run)
    fac_claims_loader = DentalClaimsBulkLoaderFactory.get_instance(claims_master_conn, master_conn, imported_claims_file_id, dry_run)
    fac_claims_loader.process_claims(st)
    summary_query_1 = """SELECT min(payment_date) as min_date, max(payment_date) as max_date 
                           FROM %s 
                          WHERE imported_claim_file_id=%s""" % ('dental_claims' , imported_claims_file_id)
                        
    summary_results_1 = Query(claims_master_conn, summary_query_1)
    
    summary_result_1 = summary_results_1.next()
    
    claims_master_conn.cursor().execute("""UPDATE imported_claim_files 
                                              SET oldest_paid_date=%s, newest_paid_date=%s 
                                            WHERE id=%s""", (summary_result_1['min_date'], summary_result_1['max_date'], imported_claims_file_id))

    update_imported_claim_file_status(claims_master_conn, [imported_claims_file_id],)
def update_imported_claim_file_status(conn, imported_claim_file_ids = None):
    q_insert = ''
    if imported_claim_file_ids and len(imported_claim_file_ids) > 0:
        str_imported_claim_file_ids = [str(v) for v in imported_claim_file_ids]
        q_insert = "icf.id IN (%s) AND " % (','.join(str_imported_claim_file_ids))

    query_norm = """UPDATE imported_claim_files icf 
                       SET icf.normalized = 1 
                     WHERE %s EXISTS (SELECT 1 FROM dental_claims c WHERE icf.id = c.imported_claim_file_id)""" % (q_insert)                

    c = conn.cursor()                      
    c.execute(query_norm)
    c.close()
    conn.close()
def main(args=None):

    logutil.log(LOG, logutil.INFO, "Dental Claims Loader Start!\nCreating Connection objects...")
    cid_all = st.start("everything", "Track how long it takes for the entire script to run!")
    
    # Read command line options
    dental_claims_load_options = __init_options__(args)
    
    # Start the ClaimsRunLogger to keep track of status of run
    crl = ClaimsRunLogger('load', yaml.dump(dental_claims_load_options.__dict__).strip('\n'))
    status = 'success'
    status_message = None
    lock_file = None
    lock_flag = True
    claims_master_conn = None
    master_conn = None 
        
    try:
        # Try acquiring lock
        lock_file = ClaimsLockFileHelper.get_instance()
        if lock_file.acquire_lock():
            logutil.log(LOG, logutil.INFO, "Successfully acquired lock file: %s." % lock_file.lock_file_location)
            
            cid = st.start("connections", "Creating Connection Objects")
        
            claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                          host = whcfg.claims_master_host,
                                          user = whcfg.claims_master_user,
                                          passwd = whcfg.claims_master_password,
                                          useDictCursor = True)
        
            master_conn = getDBConnection(dbname = whcfg.master_schema,
                                          host = whcfg.master_host,
                                          user = whcfg.master_user,
                                          passwd = whcfg.master_password,
                                          useDictCursor = True)
        
            st.end(cid)


            cid = st.start("load_dental_claims_bulk", "Load Dental Claims in Bulk")
                
                    
            __import_dental_claims_bulk_generic(claims_master_conn, master_conn, dental_claims_load_options.imported_claim_file_id, dental_claims_load_options.load_properties_file, dental_claims_load_options.dry_run)
                    
            st.end(cid)
    except:
        
        status = 'fail'
        i = sys.exc_info()
        status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
        logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
        raise            

    finally:
       dbutils.close_connections([claims_master_conn, master_conn])
       if lock_file:
           if not lock_file.release_lock():
               logutil.log(LOG, logutil.WARNING, "Unable to release lock file: %s." % lock_file.lock_file_location)
           else:
               logutil.log(LOG, logutil.INFO, "Successfully released lock file: %s." % lock_file.lock_file_location)
               
       crl.finish(status, status_message) 
       st.end(cid_all)
    return lock_flag

if __name__ == "__main__":
    main()
