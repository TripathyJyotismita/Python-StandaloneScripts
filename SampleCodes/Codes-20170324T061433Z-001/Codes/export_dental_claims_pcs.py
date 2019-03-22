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
from dental_bulk_loader import *
from optparse import OptionParser
import utils
import types

LOG = logutil.initlog('importer')
st = Stats("load_claims_to_master")

FALLBACK_ACTION_CODE_MAP = {'action_code_1':{},
                  'action_code_2':{},
                  'action_code_3':{},
                  'action_code_4':{},
                  }

def __init_options__():
    usage="""export_dental_claims.py
            [-i <imported_claim_file_ids>]
            [-a]
            [-e <employer_key>]
            [-p <specify payer>]
            [-o <output directory>]
            [-v <export version>]
            [-t <identified_claims_table_name>]
            [-x <export action>]"""
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "--imported_claim_file_ids", type="string",
                      dest="imported_claim_file_ids",
                      help="Comma separated list of Imported Claim Files to be exported")

    parser.add_option("-a", "--all_claims",
                      action="store_true",
                      dest="all_claims",
                      default=False,
                      help="Export ALL Claims into dental_claims table")
    
    parser.add_option("-e", "--employer_key",
                      type='string',
                      dest="employer_key",
                      help="Export Claims for specific employer. Used along with -a option.")


    parser.add_option("-x", "--export_action",
                      type='string',
                      dest="export_action",
                      help="Export Action if icf_ids already exist in export table. Valid values skip/refresh")
        
    parser.add_option("-o", "--output_folder", type="string",
                      dest="output_folder",
                      default='/tmp/',
                      help="Output Folder to put export dump file")

    parser.add_option("-t", "--identified_claims_table", type="string",
                      dest="identified_claims_table",
                      default='dental_claims',
                      help="Identified Claims Table Name")
    
    parser.add_option("-v", "--export_version", type="string",
                      dest="export_version",
                      default='',
                      help="Export Version")

    parser.add_option("-p", "--payer",
                      type='string',
                      dest="payer",
                      help="Export claims for a specific payer.  Used along with -a option.")
                
    (claims_load_options, args) = parser.parse_args()

    if ((not claims_load_options.imported_claim_file_ids) and (not claims_load_options.all_claims)):
        logutil.log(LOG, logutil.INFO, 'usage:%s' % usage)
        sys.exit(2)

    if (claims_load_options.imported_claim_file_ids and claims_load_options.all_claims):
        logutil.log(LOG, logutil.INFO, 'Ambiguous input! Not clear whether to load claims for imported_claim_file_id: %s, or whether to load ALL claims.' % claims_load_options.imported_claim_file_id)
        logutil.log(LOG, logutil.INFO, 'usage:%s'% usage)
        sys.exit(2)
        
    icf_ids = None
    if claims_load_options.imported_claim_file_ids:
        icf_ids = claims_load_options.imported_claim_file_ids.split(',') if claims_load_options.imported_claim_file_ids else None
    if icf_ids:
        claims_load_options.imported_claim_file_ids = set([int(x) for x in claims_load_options.imported_claim_file_ids.split(',')])

    return claims_load_options
        
def __yaml_formula_insert(stage_claims_table_columns, value):
    if isinstance(value, types.DictType):
        formula = value.get('formula')
        if formula:
            for v in stage_claims_table_columns:
                formula = formula.replace(',%s' % v, ',IC.%s' % v).replace(' %s' % v, ' IC.%s' % v).replace('(%s' % v, '(IC.%s' % v)
            return formula
    else:
        value = "IC.%s" % value
        return value

def __export_all_claims__(master_conn, prod_conn, imported_claim_file_ids, imported_claims_table, identified_claims_table = None, exported_claim_files_table = None):

    if not identified_claims_table:
        suffix = '_test'
        identified_claims_table = 'dental_claims%s' % suffix
    
    s_imported_claim_file_ids = ','.join([str(x) for x in imported_claim_file_ids])
    icf_id = imported_claim_file_ids[0]
    print icf_id
    fac_claims_loader = DentalClaimsBulkLoaderFactory.get_instance(master_conn, prod_conn, icf_id, False)
    stage_claims_table = Table(master_conn, fac_claims_loader.stage_claim_table)
    stage_claims_table_columns = stage_claims_table.columns()
    normalization_map = fac_claims_loader.normalization_rules['M']

    provider_name = __yaml_formula_insert(stage_claims_table_columns, normalization_map['provider_name'])

    sql_insert_cols = """(id,
                            imported_claim_id,
                            imported_claim_file_id,
                            source_claim_id,
                            source_claim_line_number,
                            employer_id,
                            insurance_company_id,
                            provider_id,
                            provider_name,
                            provider_location_id,
                            subscriber_patient_id,
                            patient_id,
                            procedure_label_id,
                            out_of_network,
                            provider_network_id,
                            service_place_id,
                            service_date,
                            payer_load_date,
                            benefit_level_percentage,
                            cob_amount,
                            allowed_amount,
                            charged_amount,
                            approved_amount,
                            patient_paid_amount,
                            copay_amount,
                            coinsurance_amount,
                            deductible_amount,
                            paid_amount,
                            not_covered_amount,
                            savings_amount,
                            finalized_status)"""

    sql_insert_script = """INSERT INTO %s %s %s""" % (identified_claims_table, sql_insert_cols, '%s')

    sql_select_script = """SELECT C.id,
            C.imported_claim_id,
            C.imported_claim_file_id,
            C.source_claim_number, -- as source_claim_id
            C.source_claim_line_number,
            C.employer_id,
            C.insurance_company_id,
            C.provider_id,
            C.provider_name as provider_name,
            C.provider_location_id,
            C.subscriber_patient_id,
            C.patient_id,
            C.procedure_label_id,
            C.out_of_network,
            C.provider_network_id,
            C.service_place_id,
            IF(C.service_date IS NULL, '0000-00-00', C.service_date),
            IF(C.payer_load_date IS NULL, '0000-00-00', C.payer_load_date),
            C.benefit_level_percentage,
            C.cob_amount,
            C.allowed_amount,
            C.charged_amount,
            C.approved_amount,
            C.patient_paid_amount,
            C.copay_amount,
            C.coinsurance_amount,
            C.deductible_amount,
            C.paid_amount,
            C.not_covered_amount,
            C.savings_amount,
            C.finalized_status
        FROM dental_claims C
      INNER JOIN %s IC
       ON C.imported_claim_id = IC.id
      AND C.imported_claim_file_id IN (%s)
      WHERE C.service_date < IC.created_at
    """ % (imported_claims_table, s_imported_claim_file_ids)


    sql_update_procedures = """UPDATE %s IC, procedure_labels PL
                                set IC.procedure_code_id=PL.procedure_code_id,
                                IC.procedure_code_type_id=PL.procedure_code_type_id,
                                IC.procedure_modifier_id=PL.procedure_modifier_id
                              WHERE IC.procedure_label_id=PL.id AND IC.imported_claim_file_id IN (%s)""" % (identified_claims_table, s_imported_claim_file_ids)

    #SD.generic_description, SD.detailed_description
    sql_update_services = """UPDATE %s IC, service_descriptions SD
                                set IC.generic_description=SD.generic_description,
                                    IC.detailed_description=SD.detailed_description,
                                    IC.category=SD.category
                              WHERE IC.procedure_code_id=SD.procedure_code_id AND IC.imported_claim_file_id IN (%s)""" % (identified_claims_table, s_imported_claim_file_ids)
   
    action_code_map = fac_claims_loader.load_properties.get('action_code_map',None) if fac_claims_loader.load_properties else None
    print action_code_map
 
    _ac_1 = action_code_map.get('action_code_1') if action_code_map and action_code_map.get('action_code_1') else FALLBACK_ACTION_CODE_MAP.get('action_code_1',{}).get(fac_claims_loader.insurance_company_name.lower(),['action_code_1'])
    print _ac_1

    ac_1 = "','".join(_ac_1) if isinstance(_ac_1, list) == True else _ac_1
    sql_update_action_code1 = """UPDATE %s IC, dental_claim_attributes CA set IC.action_code1=CA.value WHERE IC.id=CA.claim_id AND CA.name IN ('%s') AND IC.imported_claim_file_id IN (%s)""" % (identified_claims_table, ac_1, s_imported_claim_file_ids)
   
    _ac_2 = action_code_map.get('action_code_2') if action_code_map and action_code_map.get('action_code_2') else FALLBACK_ACTION_CODE_MAP.get('action_code_2',{}).get(fac_claims_loader.insurance_company_name.lower(),['action_code_2'])
    ac_2 = "','".join(_ac_2) if isinstance(_ac_2, list) == True else _ac_2
    sql_update_action_code2 = """UPDATE %s IC, dental_claim_attributes CA set IC.action_code2=CA.value WHERE IC.id=CA.claim_id AND CA.name IN ('%s') AND IC.imported_claim_file_id IN (%s)""" % (identified_claims_table, ac_2, s_imported_claim_file_ids)

    _ac_3 = action_code_map.get('action_code_3') if action_code_map and action_code_map.get('action_code_3') else FALLBACK_ACTION_CODE_MAP.get('action_code_3',{}).get(fac_claims_loader.insurance_company_name.lower(),['action_code_3'])
    ac_3 = "','".join(_ac_3) if isinstance(_ac_3, list) == True else _ac_3
    sql_update_action_code3 = """UPDATE %s IC, dental_claim_attributes CA set IC.action_code3=CA.value WHERE IC.id=CA.claim_id AND CA.name IN ('%s') AND IC.imported_claim_file_id IN (%s)""" % (identified_claims_table, ac_3, s_imported_claim_file_ids)
 
    sql_update_sd_with_gen_svc = """UPDATE %s SET generic_description='General Service', detailed_description='General Service', procedure_label_id=-1, procedure_code_id=-1, procedure_code_type_id = -1, procedure_modifier_id = -1 WHERE procedure_label_id=-1 AND imported_claim_file_id IN (%s)""" % (identified_claims_table, s_imported_claim_file_ids)
    
    sql_insert_exported_claim_files = None
    if exported_claim_files_table:
        sql_insert_exported_claim_files = """INSERT IGNORE INTO %s 
                                            (imported_claim_file_id,
                                             insurance_company_id,
                                             employer_id,
                                             latest_payment_date,
                                             display_flag,
                                             exported_at,
                                             claim_file_type)
                                            SELECT
                                            imported_claim_file_id,
                                            insurance_company_id,
                                            idc.employer_id,
                                            max(service_date),
                                            1,
                                            NOW(),
                                            icf.claim_file_type 
                                            FROM %s idc join %s.imported_claim_files icf on idc.`imported_claim_file_id`=icf.id 
                                            WHERE idc.imported_claim_file_id IN (%s) 
                                            group by idc.imported_claim_file_id, insurance_company_id, employer_id""" % (exported_claim_files_table, identified_claims_table, whcfg.claims_master_schema, s_imported_claim_file_ids)

    c = master_conn.cursor()

    query = Query(master_conn, "SELECT ICF.claim_file_source_name as name FROM imported_claim_files ICF WHERE ICF.claim_file_source_type='payer' AND ICF.id IN (%s)" % (s_imported_claim_file_ids))
    insurance_company_name = query.next()['name']

    logutil.log(LOG, logutil.INFO, "Populating identified_claims table! Processing imported_claim_file_ids: (%s)" % s_imported_claim_file_ids)
    logutil.log(LOG, logutil.INFO, (sql_insert_script % sql_select_script))
    
    x_claims_queries = [{'query':sql_insert_script % sql_select_script,
                             'description':'Insert into dental_claims.',
                             'warning_filter':'ignore'}]
    utils.execute_queries(master_conn, LOG, x_claims_queries)     

    c.execute(sql_update_procedures)
    c.execute(sql_update_services)
    c.execute(sql_update_action_code1)
    c.execute(sql_update_action_code2)
    c.execute(sql_update_action_code3)
    c.execute(sql_update_sd_with_gen_svc)
    
    if sql_insert_exported_claim_files:
        c.execute(sql_insert_exported_claim_files)
    
    master_conn.commit()

def __build_query_for_export(employer_key,payer,imported_claim_file_ids,claims_master_conn):
    employer_id = None
    payer_id = None
    if employer_key:
        employer = Query(claims_master_conn, "SELECT id FROM %s.employers where `key`='%s'" % (whcfg.master_schema, employer_key))
        employer_id = employer.next()['id']

    if payer:
        payer = Query(claims_master_conn, "SELECT id FROM %s.insurance_companies where `name`='%s'" % (whcfg.master_schema, payer))
        payer_id = payer.next()['id']
        
    icf_query = """SELECT icf.table_name, GROUP_CONCAT(icf.id ORDER BY icf.id DESC) as icf_ids
                    FROM imported_claim_files icf, imported_claim_files_insurance_companies icfic 
                   WHERE icf.id=icfic.imported_claim_file_id
                     AND icf.table_name LIKE '%_imported_claims'
                     AND icf.claim_file_type = 'D'
                     AND icfic.insurance_company_id <> 8"""
                
    if employer_id:
        icf_query = icf_query + """ AND icf.employer_id=%d""" % (employer_id)

    if payer_id:
        icf_query = icf_query + """ AND icfic.insurance_company_id=%d""" % (payer_id)
            
    if imported_claim_file_ids:
        icf_query = icf_query + """ AND icf.id IN (%s)""" % ','.join([str(x) for x in imported_claim_file_ids])
    return icf_query

def get_icf_ids_for_export(employer_key, payer, imported_claim_file_ids):
    """
    Get imported claim file ids which can be exported based on payer, employer and imported claim file id.
    """
    claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = whcfg.claims_master_host,
                                  user = whcfg.claims_master_user,
                                  passwd = whcfg.claims_master_password,
                                  useDictCursor = True)
    
    icf_query = __build_query_for_export(employer_key,payer,imported_claim_file_ids,claims_master_conn)
    group_by_clause =  """ GROUP BY icf.table_name"""
    
    r_icf_query = Query(claims_master_conn, icf_query + group_by_clause)
    all_icf_ids = []
    for icf_result in r_icf_query:
        all_icf_ids.extend([int(x) for x in icf_result.get('icf_ids').split(',')])
    claims_master_conn.close()
    return all_icf_ids

def export_dental_claims(employer_key, payer, imported_claim_file_ids, export_action = None, identified_claims_table='dental_claims'):
    """
    Export claims to dental claims table.
    """
    logutil.log(LOG, logutil.INFO, "Claims Export Start!\nStarted exporting dental claims")
    
    exported_claim_files_table = None
    if identified_claims_table == 'dental_claims':
        exported_claim_files_table = '%s.exported_claim_files' % whcfg.claims_master_export_schema
        
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
        # Check how many of these are already in the export table & exit (skip/refresh option must be passed)
    if imported_claim_file_ids:
        already_exported_icf_ids = []
        s_all_icf_ids = ','.join([str(x) for x in imported_claim_file_ids])
        q_already_exported_icf_ids = """SELECT DISTINCT imported_claim_file_id FROM %s.%s WHERE imported_claim_file_id IN (%s)""" % \
                                            (whcfg.claims_master_export_schema, identified_claims_table, s_all_icf_ids)
        print q_already_exported_icf_ids
        r_already_exported_icf_ids = Query(claims_master_conn, q_already_exported_icf_ids)
        
        if r_already_exported_icf_ids:
            already_exported_icf_ids = [x['imported_claim_file_id'] for x in r_already_exported_icf_ids]
            
        if already_exported_icf_ids:
            s_already_exported_icf_ids = ','.join([str(x) for x in already_exported_icf_ids])
            if export_action == 'skip':
                logutil.log(LOG, logutil.WARNING, 'Skipping: (%s).' % s_already_exported_icf_ids)
                imported_claim_file_ids = list(set(imported_claim_file_ids) - set(already_exported_icf_ids))
            elif export_action == 'refresh':
                logutil.log(LOG, logutil.WARNING, 'Refreshing: (%s).' % s_already_exported_icf_ids)
                d_identified_claims = """DELETE FROM %s.%s WHERE imported_claim_file_id in (%s)""" % \
                                            (whcfg.claims_master_export_schema, identified_claims_table, s_already_exported_icf_ids)
                claims_master_conn.cursor().execute(d_identified_claims)
            else:
                logutil.log(LOG, logutil.WARNING, 'The following imported_claim_file_ids are already in the export table: (%s). \
                                            Please specify option -x for skip/refresh' % s_already_exported_icf_ids)
                sys.exit(2)
    group_by_clause =  """ GROUP BY icf.table_name"""
    if imported_claim_file_ids:
        s_all_icf_ids = ','.join([str(x) for x in imported_claim_file_ids])
        r_icf_query = Query(claims_master_conn, __build_query_for_export(employer_key, payer, imported_claim_file_ids, claims_master_conn) \
                            + ' AND icf.id IN (%s)' % s_all_icf_ids + group_by_clause)
        for icf_result in r_icf_query:
            cid = st.start("import_claims", "Exporting Claims")
            __export_all_claims__(claims_master_conn, master_conn, [int(x) for x in icf_result.get('icf_ids').split(',')], \
                                            icf_result.get('table_name'),'%s.%s' % (whcfg.claims_master_export_schema, \
                                            identified_claims_table), exported_claim_files_table)
            st.end(cid)
    claims_master_conn.close()
    master_conn.close()
    
def create_export_dump(icf_ids, output_folder = '/tmp/', export_version = '', identified_claims_table='dental_claims'):
    """
    Create dump file for export.
    """
    s_icf_ids = ','.join([str(x) for x in icf_ids])
    export_file_suffix = export_version if export_version else s_icf_ids.replace(',','_')
    logutil.log(LOG, logutil.INFO, 'Creating Export Dump for icf_ids: (%s) to %s/dental_claims_export_%s.dmp.' % \
                                            (s_icf_ids, output_folder, export_file_suffix))
    

    claims_master_conn = getDBConnection(dbname = whcfg.claims_master_schema,
                                  host = whcfg.claims_master_host,
                                  user = whcfg.claims_master_user,
                                  passwd = whcfg.claims_master_password,
                                  useDictCursor = True)
    
    #get export filter from import_file_employer_config
    query="""select distinct ifec.export_filter from import_file_employer_config ifec,uploaded_files uf 
             where ifec.file_detection_rule=uf.file_detection_rule and uf.prod_imported_claim_file_id in (%s)"""%s_icf_ids 
                    
    employers_config=Query(claims_master_conn,query)
    employer_config = employers_config.next()   
    export_filter= employer_config.get('export_filter')
    
    if export_filter:               
        logutil.log(LOG, logutil.INFO,'Filter Condition: %s'%export_filter)  
        where_clause = """--where="imported_claim_file_id IN (%s) and subscriber_patient_id <> -1 and %s" """ %(s_icf_ids if s_icf_ids else '-1',export_filter)
    else:
        where_clause = """--where="imported_claim_file_id IN (%s) and subscriber_patient_id <> -1" """ %s_icf_ids if s_icf_ids else '-1'
    
    claims_master_conn.close()
    ##commenting this following to the remove request--no-create-info while taking mysqldump to include create table in dump file"
    stmt = """mysqldump -u%s -p'%s' --no-create-info --opt %s %s %s  > %s/dental_claims_export_%s.dmp""" % \
                                            (whcfg.claims_master_user, whcfg.claims_master_password, where_clause, \
                                            whcfg.claims_master_export_schema, identified_claims_table, \
                                            output_folder, export_file_suffix)
    os.system(stmt)
    #stmt = """mysqldump -u%s -p'%s' --no-data --opt %s %s %s  > %s/dental_claims_export_%s.dmp""" % \
    #                                        (whcfg.claims_master_user, whcfg.claims_master_password, where_clause, \
    #                                        whcfg.claims_master_export_schema, identified_claims_table, \
    #                                        output_folder, export_file_suffix)
    #os.system(stmt)

    stmt = """mysqldump -u%s -p'%s' %s exported_claim_files  >> %s/dental_claims_export_%s.dmp""" % \
                                            (whcfg.claims_master_user, whcfg.claims_master_password, whcfg.claims_master_export_schema, \
                                            output_folder, export_file_suffix)
    os.system(stmt)
    
def export_claims_helper(employer_key, payer, imported_claim_file_ids, export_action=None, identified_claims_table='dental_claims', \
export_version='', output_folder='/tmp/'):

    logutil.log(LOG, logutil.INFO, "Claims Loader Start!\nCreating Connection objects...")
    cid_all = st.start("everything", "Track how long it takes for the entire script to run!")
    all_icf_ids = get_icf_ids_for_export(employer_key, payer, imported_claim_file_ids)
    export_dental_claims(employer_key, payer, all_icf_ids, export_action, identified_claims_table)
    create_export_dump(all_icf_ids, output_folder, export_version, identified_claims_table)
    st.end(cid_all)

if __name__ == "__main__":
    claims_load_options = __init_options__()
    
    export_claims_helper(claims_load_options.employer_key, claims_load_options.payer, claims_load_options.imported_claim_file_ids, \
                                            claims_load_options.export_action, claims_load_options.identified_claims_table, \
                                            claims_load_options.export_version, claims_load_options.output_folder)


