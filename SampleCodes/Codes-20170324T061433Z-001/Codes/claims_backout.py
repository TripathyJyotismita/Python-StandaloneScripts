import whcfg
import logutil
import MySQLdb
import traceback
import datetime
from dbutils import *
from model import *
from statsutil import *
from optparse import OptionParser
import utils

LOG = logutil.initlog('Claims Backout')
st = Stats("claims_backout")

def __init_options__(args):
    usage="""claims_backout.py
            [-i <imported_claim_file_id>]
            [-a]
            [-e <employer_key>]
            [-p <specify payer>]
            [-t <claim type(medical/pharmacy)>]"""
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "--imported_claim_file_ids", type = "string",
                      dest="imported_claim_file_ids",
                      help="Comma separated list of Imported Claim Files to be removed from prod")

    parser.add_option("-a", "--all_claims",
                      action = "store_true",
                      dest = "all_claims",
                      default = False,
                      help = "Backout all claims")
    
    parser.add_option("-e", "--employer_key",
                      type = 'string',
                      dest = "employer_key",
                      help = "Backout claims for specific employer")

    parser.add_option("-p", "--payer",
                      type = 'string',
                      dest = "payer",
                      help = "Backout claims for specific payer")
    
    parser.add_option("-t", "--claim-type",
                      type = "string",
                      dest = "claim_type",
                      default = "medical",
                      help = "Claim Type [medical/pharmacy]")
    
    (claims_backout_options, args) = parser.parse_args(args)

    if ((not claims_backout_options.imported_claim_file_ids) and (not claims_backout_options.all_claims)):
        logutil.log(LOG, logutil.INFO, 'usage:%s' % usage)
        sys.exit(2)

    if (claims_backout_options.imported_claim_file_ids and claims_backout_options.all_claims):
        logutil.log(LOG, logutil.INFO, 'Ambiguous input! Not clear whether to backout claims for given imported claims file ids or for all claims' % claims_backout_options.imported_claim_file_id)
        logutil.log(LOG, logutil.INFO, 'usage:%s'% usage)
        sys.exit(2)
        
    if(claims_backout_options.claim_type != 'medical' and claims_backout_options.claim_type != 'pharmacy'):
        logutil.log(LOG, logutil.INFO, '%s is invalid claim type' % claims_backout_options.claim_type)
        logutil.log(LOG, logutil.INFO, 'usage:%s'% usage)
        sys.exit(2)
    icf_ids = None
    if claims_backout_options.imported_claim_file_ids:
        icf_ids = claims_backout_options.imported_claim_file_ids.split(',') if claims_backout_options.imported_claim_file_ids else None
    if icf_ids:
        claims_backout_options.imported_claim_file_ids = set([int(x) for x in claims_backout_options.imported_claim_file_ids.split(',')])

    return claims_backout_options

def main(args = None):
    """
    Backout normalized claims from database
    """
    logutil.log(LOG, logutil.INFO, "Claims backout start!!")
    cid_all = st.start("everything", "Track run time for script.")
    claims_backout_options = __init_options__(args)
    cid = st.start("connections", "Started creating connection objects")
    claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = whcfg.claims_master_host,
                                  user = whcfg.claims_master_user,
                                  passwd = whcfg.claims_master_password,
                                  useDictCursor = True)
    st.end(cid)
    employer_id = None
    payer_id = None
    if claims_backout_options.employer_key:
        employer = Query(claims_master_conn, "SELECT id FROM %s.employers where `key`='%s'" % (whcfg.master_schema, claims_backout_options.employer_key))
        employer_id = employer.next()['id']

    if claims_backout_options.payer:
        payer = Query(claims_master_conn, "SELECT id FROM %s.insurance_companies where `name`='%s'" % (whcfg.master_schema, claims_backout_options.payer))
        payer_id = payer.next()['id']
    
    if claims_backout_options.claim_type == 'medical':
        file_type = 'M'
        claims_table = 'claims'
    else:
        file_type = 'R'
        claims_table = 'rx_claims'
    
    icf_query = """SELECT GROUP_CONCAT(icf.id ORDER BY icf.id DESC) as icf_ids
                    FROM imported_claim_files icf 
                    JOIN imported_claim_files_insurance_companies icfic on icf.id = icfic.imported_claim_file_id
                    WHERE icf.table_name LIKE '%%_imported_claims'
                    and icf.claim_file_type = '%s'
                    and icf.id not in (select imported_claim_file_id from %s.exported_claim_files )""" %(file_type, whcfg.claims_master_export_schema)
    if employer_id:
        icf_query = icf_query + """ AND icf.employer_id=%d""" % (employer_id)

    if payer_id:
        icf_query = icf_query + """ AND icfic.insurance_company_id=%d""" % (payer_id)
            
    if claims_backout_options.imported_claim_file_ids:
        icf_query = icf_query + """ AND icf.id IN (%s)""" % ','.join([str(x) for x in claims_backout_options.imported_claim_file_ids])
    
    r_icf_query = Query(claims_master_conn, icf_query)
    
    s_all_icf_ids = r_icf_query.next().get('icf_ids') if r_icf_query else ''
    if s_all_icf_ids == None:
        s_all_icf_ids = 'NULL' 
    backout_query = """delete cs.* from %s c, claim_specialties cs where c.id = cs.claim_id and c.imported_claim_file_id in (%s);
                    delete ca.* from %s c, claim_attributes ca where c.id = ca.claim_id and c.imported_claim_file_id in (%s);
                    delete cpi.* from %s c, claim_provider_identifiers cpi where c.id = cpi.claim_id and c.imported_claim_file_id in (%s);
                    delete cpe.* from %s c, claim_provider_exceptions cpe where c.id = cpe.claim_id and c.imported_claim_file_id in (%s);
                    delete ocl.* from %s c, original_claim_locations ocl where c.id = ocl.claim_id and c.imported_claim_file_id in (%s);
                    delete from %s where imported_claim_file_id in (%s);""" %(claims_table, s_all_icf_ids, \
                                             claims_table, s_all_icf_ids, \
                                             claims_table, s_all_icf_ids, \
                                             claims_table, s_all_icf_ids, \
                                             claims_table, s_all_icf_ids, \
                                             claims_table, s_all_icf_ids )
    backout_cursor = claims_master_conn.cursor()
    cid = st.start("Backout", "Backout production claims")
    logutil.log(LOG, logutil.INFO, "Backing out claims for imported claim file id %s" % s_all_icf_ids)
    try:
        backout_cursor.execute(backout_query)
    except:
        logutil.log(LOG, logutil.ERROR, 'Error occured while backing out claims for imported claim file id (%s)' % s_all_icf_ids)
    finally:
        backout_cursor.close()
        claims_master_conn.close()
        st.end(cid)
    st.end(cid_all)
    return s_all_icf_ids
    
if __name__ == "__main__":
    main()
