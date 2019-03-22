import whcfg
import logutil
import traceback
import pprint
from cStringIO import StringIO
from dbutils import *
from optparse import OptionParser
from model import *
import tarfile
import os
from claims_file_helper import *
from claims_util import *
import warnings

LOG = logutil.initlog('importer')

def __init_options__(args):
    usage="""stage_claims_from_file.py
            -s <claims_master_schema>
            -t <imported_claims_table_name>
            -l <claims_data_layout_file_location>
            -f <raw_claims_data_file_location>
            -o <temp_extract_folder>
            -e <employer_key>
            -i <insurance_company_name>
            -n <line terminating character>
            -g <file delimiter>
            -j <number of lines to omit>
            -p <load properties file>
            [-d]
            [-m]
            [-r <replace/ignore>]
            [-x <decrypt_extract_password>]
            """
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--schema", type="string",
                      dest="schema_name",
                      help="name of schema (database) to use")
    parser.add_option("-t", "--table", type="string",
                      dest="table_name",
                      help="name of table to be created in database")
    parser.add_option("-l", "--layout", type="string",
                      dest="layout_file",
                      help="name of table to be created in database")
    parser.add_option("-f", "--file", type="string",
                      dest="data_file",
                      help="name of table to be created in database")
    parser.add_option("-w", "--fixed-width",
                      action="store_true",
                      dest="fixed_width",
                      default=False,
                      help="fixed width data layout")      
    parser.add_option("-g", "--file_delimiter", type="string",
                      dest="file_delimiter",
                      default="\\t",
                      help="file delimiter value")
    parser.add_option("-n","--line_terminator", type="string",
                      dest="line_terminator",
                      default="\\r\\n",
                      help="line terminator value")
    parser.add_option("-j", "--file_omit_lines", type="string",
                      dest="omit_lines",
                      default=0,
                      help="file omit lines")
    parser.add_option("-k", "--file_omit_lines_end", type="string",
                      dest="omit_lines_end",
                      default=0,
                      help="file omit lines at end")
    parser.add_option("-o", "--output-dir", type="string",
                      dest="extract_dir",
		      default='/tmp/',
                      help="location of output extract directory")
    parser.add_option("-x", "--decrypt_extract_password", type="string",
                      dest="decrypt_extract_password", 
                      help="Password to decrypt/extract file")    
    parser.add_option("-e", "--employer_key", type="string",
                      dest="employer_key",
                      help="name of the employer")
    parser.add_option("-i", "--insurance_company_or_thirdparty_datastore", type="string",
                      dest="insurance_company",
                      help="name of the insurance company or third-party datastore")
    parser.add_option("-d", "--datastore",
                      action="store_true",
                      dest="thirdparty_datastore",
                      default=False,
                      help="source of data is a third-party datastore") 
    parser.add_option("-p", "--load_properties_file", type="string",
                      dest="load_properties_file",
                      help="Load Properties File")    
    parser.add_option("-m", "--mark_duplicates_only",
                      action="store_true",
                      dest="mark_duplicates_only",
                      default=False,
                      help="Do not load any data. Only mark duplicates.")
    parser.add_option("-r", "--replace_ignore", type="string",
                      dest="replace_ignore",
                      help="Replace or Ignore. Use this option only where there exist data columns that have uniqueness constraints on them. Valid values replace/ignore.")

    (claims_load_options, args) = parser.parse_args(args)

    if ((not claims_load_options.schema_name)
        or (not claims_load_options.table_name)
        or (not claims_load_options.layout_file)
        or (not claims_load_options.data_file)
        or (not claims_load_options.extract_dir)
        or (not claims_load_options.insurance_company) 
        or (not claims_load_options.load_properties_file)
        or (claims_load_options.replace_ignore and claims_load_options.replace_ignore.lower() not in ['replace','ignore'])):
        print 'usage:', usage
        sys.exit(2)

    return claims_load_options

def __get_insurance_companies__(conn):

    insu_companies = {}
    tab_insurance_companies = Table(conn, 'insurance_companies')

    for insu_comp in tab_insurance_companies:
        insu_companies[insu_comp['name'].lower()] = insu_comp['id']

    return insu_companies

def main(args=None):
    claims_load_options = __init_options__(args)
    crl = ClaimsRunLogger('stage', yaml.dump(claims_load_options.__dict__).strip('\n'))
    status = 'success'
    status_message = None
    claims_file_helper = None
    conn = None
    master_conn = None
    imported_claim_file_id = -1
    try:
        claims_file_options = {'file_delimiter':claims_load_options.file_delimiter, 'omit_lines':claims_load_options.omit_lines, 'fixed_width':claims_load_options.fixed_width,'line_terminator':claims_load_options.line_terminator}
        if claims_load_options.replace_ignore:
            claims_file_options['replace_ignore'] = claims_load_options.replace_ignore
        if claims_load_options.decrypt_extract_password:
            claims_file_options['decrypt_extract_password'] = claims_load_options.decrypt_extract_password

        conn = getDBConnection(dbname = claims_load_options.schema_name,
                                      host = whcfg.claims_master_host,
                                      user = whcfg.claims_master_user,
                                      passwd = whcfg.claims_master_password,
                                      useDictCursor = False)
    
        master_conn = getDBConnection(dbname = whcfg.master_schema,
                                      host = whcfg.master_host,
                                      user = whcfg.master_user,
                                      passwd = whcfg.master_password,
                                      useDictCursor = False)
    
    
        fac_employers = ModelFactory.get_instance(conn, "%s.employers" % whcfg.master_schema)
        emp_entry = {'key':claims_load_options.employer_key}
        
        emp_entry = fac_employers.find(emp_entry)
#        if not emp_entry:
            
#            raise ClaimsLoaderException(1000, "Unknown Employer: %s" % claims_load_options.employer_key, True)
        employer_id = emp_entry['id'] if emp_entry else -1
        claim_file_source_type = 'PAYER'
        insurance_company_id = None
        
        if not claims_load_options.thirdparty_datastore:
            fac_insurers = ModelFactory.get_instance(master_conn, "%s.insurance_companies" % whcfg.master_schema)
            insu_entry = {'name':claims_load_options.insurance_company}
            insu_entry = fac_insurers.find(insu_entry)
            if not insu_entry:
                raise ClaimsLoaderException(1000, "Unknown Insurance Company: %s" % claims_load_options.insurance_company, True)
                
            insurance_company_id = insu_entry['id']        
        else:
            claim_file_source_type = 'DATASTORE'
            
        fac_imported_claim_files = ModelFactory.get_instance(conn, "imported_claim_files")
        
        load_properties_text = None
    
        load_properties  = None
        load_properties = open(claims_load_options.load_properties_file, 'r')
        load_properties_text = load_properties.read()
        
        imp_claim_file_entry = {}
    #    imp_claim_file_entry['insurance_company_id'] = insurance_company_id
        imp_claim_file_entry['claim_file_type'] = 'D'
        imp_claim_file_entry['file_name'] = claims_load_options.data_file[claims_load_options.data_file.rfind('/')+1::]
        imp_claim_file_entry['file_path'] = claims_load_options.data_file[0:claims_load_options.data_file.rfind('/')]
        imp_claim_file_entry['table_name'] = claims_load_options.table_name
        imp_claim_file_entry['employer_id'] = employer_id
        imp_claim_file_entry['claim_file_source_name'] = claims_load_options.insurance_company
        imp_claim_file_entry['claim_file_source_type'] = claim_file_source_type
        imp_claim_file_entry['load_properties'] = load_properties_text
        imp_claim_file = fac_imported_claim_files.find(imp_claim_file_entry)
        if (not imp_claim_file):
            timevalue = datetime.datetime.now()
            imp_claim_file_entry['imported_at'] = timevalue.isoformat(' ').split('.')[0]
            imp_claim_file = fac_imported_claim_files.create(imp_claim_file_entry)
        imported_claim_file_id = imp_claim_file['id']
    
        if not claims_load_options.thirdparty_datastore:
            # Create Entry in imported_claim_file_insurance_companies if this is not a third-party datastore 
            fac_imported_claim_file_insurance_companies = ModelFactory.get_instance(conn, "imported_claim_files_insurance_companies")
            icfic_entry = {'imported_claim_file_id':imported_claim_file_id,
                           'insurance_company_id':insurance_company_id}
            fac_imported_claim_file_insurance_companies.find_or_create(icfic_entry)
    
        claims_file_helper = ClaimsFileHelperFactory.get_instance(claims_load_options.insurance_company, claims_load_options.employer_key, claims_load_options.layout_file, claims_file_options)
        claims_create_table = claims_file_helper.get_create_imported_claims_file_table(claims_load_options.table_name)
        claims_load_simple = claims_file_helper.get_load_claims_from_file(claims_load_options.table_name, claims_load_options.data_file, claims_load_options.extract_dir, imported_claim_file_id,claims_load_options.omit_lines_end)
        

        if not claims_load_options.mark_duplicates_only:
            queries = [{'query':claims_create_table,
                        'description':"Creating %s" % (claims_load_options.table_name),
                        'warning_filter':'ignore'},
                       {'query':claims_load_simple,
                        'description':"Loading Data into %s" % (claims_load_options.table_name),
                        'warning_filter':'error'}
                       ]
#        print claims_load_simple
        execute_queries(conn, LOG, queries)
        
        logutil.log(LOG, logutil.INFO, "Raw Claims have been imported with imported_claim_file_id: %s" % imported_claim_file_id)
        
        return imported_claim_file_id
    
    except:
        status = 'fail'
        i = sys.exc_info()
        status_message = ''.join(traceback.format_exception(i[1],i[0],i[2]))
        logutil.log(LOG, logutil.WARNING,"SEVERE ERROR: %s" % status_message)
        raise
    finally:
        if imported_claim_file_id > 0:
            mark_duplicates = """UPDATE %s.%s ic1, %s.%s ic2
                                    SET ic1.duplicate_of_claim_id=ic2.id
                                  WHERE ic1.md5_checksum=ic2.md5_checksum
                                    AND ic1.created_at > ic2.created_at
                                    AND ic1.imported_claim_file_id=%s""" % (claims_load_options.schema_name, claims_load_options.table_name, claims_load_options.schema_name, claims_load_options.table_name, imported_claim_file_id)
            
            mark_duplicates_within_file = """UPDATE %s.%s ic1, %s.%s ic2
                                               SET ic1.duplicate_of_claim_id=ic2.id
                                             WHERE ic1.duplicate_of_claim_id is null
                                               AND ic1.md5_checksum=ic2.md5_checksum
                                               AND ic1.imported_claim_file_id=ic2.imported_claim_file_id
                                               AND ic1.id > ic2.id
                                               AND ic1.imported_claim_file_id=%s""" % (claims_load_options.schema_name, claims_load_options.table_name, claims_load_options.schema_name, claims_load_options.table_name, imported_claim_file_id)

            if claims_load_options.load_properties_file:
                #load properties from corresponding load properties file.
                load_properties = yaml.load(open(claims_load_options.load_properties_file, 'r'))

                #fetch mapping for payment date.
                field_column_mappings = load_properties.get('field_column_mappings')
            else:
                field_column_mappings = claims_load_helper.FIELD_MAPPINGS.get(claims_load_options.insurance_company.lower())

            if isinstance(field_column_mappings.get('payment_date'),dict):
                payment_date = field_column_mappings.get('payment_date').get('formula')
            else:
                payment_date = field_column_mappings.get('payment_date')

            #claims_volume data fetching and inserstion.            


            claims_volume_distribution_data = """INSERT INTO %(fdb_schema_name)s.raw_claim_volume_distribution_stats (imported_claim_file_id, employer_id, insurance_company_id, year, month, file_type, claim_count)
                                                    SELECT irt.imported_claim_file_id, icf.employer_id, icfic.insurance_company_id, year(%(payment_date)s) year, month(%(payment_date)s) month, "dental_claim" file_type, count(*) claim_count 
                                                    FROM %(schema_name)s.%(table_name)s irt JOIN %(schema_name)s.imported_claim_files icf
                                                    ON irt.imported_claim_file_id = icf.id JOIN %(schema_name)s.imported_claim_files_insurance_companies icfic
                                                    ON icf.id = icfic.imported_claim_file_id
                                                    WHERE irt.imported_claim_file_id=%(imported_claim_file_id)s AND irt.duplicate_of_claim_id is NULL
                                                    GROUP BY year(%(payment_date)s), month(%(payment_date)s)""" % {'fdb_schema_name' : whcfg.files_dashboard_ui_schema,
                                                                                                                    'schema_name' : claims_load_options.schema_name,
                                                                                                                    'table_name' : claims_load_options.table_name,
                                                                                                                    'payment_date' : payment_date,
                                                                                                                    'imported_claim_file_id' : imported_claim_file_id}


            logutil.log(LOG, logutil.INFO, "claims_volume_distribution_data query: %s" % claims_volume_distribution_data)

            queries = [{'query':mark_duplicates,
                        'description':"Marking Duplicates in %s" % (claims_load_options.table_name),
                        'warning_filter':'error'},
                       {'query':mark_duplicates_within_file,
                        'description':"Marking Duplicates within file in %s" % (claims_load_options.table_name),
                        'warning_filter':'error'},
                       {'query':claims_volume_distribution_data,
                        'description':"Generating claims volume distribution data from %s" % (claims_load_options.table_name),
                        'warning_filter':'error'}
                       ]
            execute_queries(conn, LOG, queries)
            
##            if claims_load_options.omit_lines_end:
##               r_ids_to_delete = dbutils.Query(conn, '''SELECT id FROM %s.%s 
##                                                WHERE imported_claim_file_id=%s
##                                                ORDER BY id desc
##                                                LIMIT %s''' % (whcfg.claims_master_schema,
##                                                               claims_load_options.table_name,
##                                                               imported_claim_file_id,
##                                                               claims_load_options.omit_lines_end))
##                if r_ids_to_delete:
##                    ids_to_delete = ','.join([str(r.get('id')) for r in r_ids_to_delete])
##                    
##                   d_raw_claims = '''DELETE FROM %s.%s WHERE id IN (%s)''' % (whcfg.claims_master_schema,
##                                                               claims_load_options.table_name,
##                                                               ids_to_delete)
##                    
##                    conn.cursor().execute(d_raw_claims)
                    
        dbutils.close_connections([conn, master_conn])
        if claims_file_helper:
            claims_file_helper.cleanup()    
        crl.finish(status, status_message)

if __name__ == "__main__":
    main()

