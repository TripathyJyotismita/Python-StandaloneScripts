import re
import tarfile
import os
import sys
import pprint
import whcfg
from regex_utils import *
from parse_name import *
from dbutils import *
from model import *
from statsutil import *
from cStringIO import StringIO
import datetime
import logutil
import pprint
import csv
import yaml
import time
from claims_util import *
import claims_util
import hashlib
import utils
import location_utils
import import_utils

LOG = logutil.initlog('importer')
st = Stats("dental_claims_load_helper")

# TODO: Extend for stats and logging
""" Known Custom Factories """
helpers = {"generic": lambda conn, p_conn, icf, load_properties, dry_run: DentalBaseBulkClaimsLoader(conn, p_conn, icf, load_properties, dry_run)
           }
LIMIT_CLAUSE = """ """

helper_instances = {}

def field_mappings():
    return claims_load_helper.FIELD_MAPPINGS

class _Callable:
    def __init__(self, anycallable):
        self.__call__ = anycallable

class DentalClaimsBulkLoaderFactory:

    def get_instance(claims_master_conn, provider_master_conn, imported_claim_file_id, dry_run = False):

        if (claims_master_conn is not None):

            if not claims_master_conn in helper_instances:
                helper_instances[claims_master_conn] = {}
            if imported_claim_file_id in helper_instances[claims_master_conn]:
                return helper_instances[claims_master_conn][imported_claim_file_id]
            else:
                fac_imported_claim_files = ModelFactory.get_instance(claims_master_conn, 'imported_claim_files')
                fac_imported_claim_files.table.select('id, claim_file_source_name, claim_file_source_type, employer_id, table_name, load_properties')
                icf_entry = {'id':imported_claim_file_id}
                icf_entry = fac_imported_claim_files.find(icf_entry)
                load_properties_text = None
                if icf_entry:
                    claim_file_source_type = icf_entry['claim_file_source_type'].upper() 
                    icf_entry['is_payer'] = True if claim_file_source_type == 'PAYER' else False
                    load_properties_text = icf_entry['load_properties']
                    
                insurance_company_name = icf_entry['claim_file_source_name'].lower() if icf_entry and helpers.get(icf_entry['claim_file_source_name'].lower()) else 'generic'
                fac = helpers[insurance_company_name](claims_master_conn, provider_master_conn, icf_entry, load_properties_text, dry_run)
                helper_instances[claims_master_conn][imported_claim_file_id] = fac
                return fac


    get_instance = _Callable(get_instance)

class DentalBaseBulkClaimsLoader:

    def __init__(self, claims_master_conn, provider_master_conn, imported_claim_file_details, load_properties_text = None, dry_run = False):

        self.dry_run = dry_run 
                
        self.conn = claims_master_conn
        self.prod_conn = provider_master_conn
        
        self.imported_claim_file_id = imported_claim_file_details['id']
        self.employer_id = imported_claim_file_details['employer_id']
        
        self.stage_claim_table = imported_claim_file_details['table_name']
        self.stage_claim_table_columns = dbutils.Table(self.conn, self.stage_claim_table).columns()
        self.stage_claims_table_alias = 'cic'
        
        self.claim_file_source_name = imported_claim_file_details['claim_file_source_name']
        self.claim_file_source_type = imported_claim_file_details['claim_file_source_type']
        self.is_source_payer = imported_claim_file_details['is_payer']
        self.claims_table_suffix = ''
        
        
        self.insurance_company_id = None
        self.insurance_company_name = None
        
        self.external_procedure_code_types = None
        self.procedure_code_types = None
        self.procedure_code_type_values = None
        self.procedure_code_type_lengths  = {'1':5,'2':6,'3':4}
        self.procedure_codes = None
        self.procedure_code_to_type_map = None
        self.procedures = None
        self.procedure_code_modifiers = None
        
        if self.is_source_payer:
            # Expect to find insurance_company_id in imported_claim_file_insurance_companies
            query = """SELECT icfic.insurance_company_id, ic.name 
                         FROM %s.imported_claim_files_insurance_companies icfic, %s.insurance_companies ic
                        WHERE icfic.imported_claim_file_id=%s
                          AND icfic.insurance_company_id=ic.id""" % (whcfg.claims_master_schema, 
                                                                     whcfg.master_schema,
                                                                     self.imported_claim_file_id)
            results = Query(claims_master_conn, query)
            insurance_company = results.next() if results else {}
            insurance_company_id = insurance_company.get('insurance_company_id')
            
            if not insurance_company_id:
                logutil.log(LOG, logutil.INFO, "Invalid imported_claim_file_id: %s passed. Exiting!" % self.imported_claim_file_id)
                sys.exit()
            else:
                self.insurance_company_id = insurance_company_id
                self.insurance_company_name = insurance_company['name']

        self.load_properties = yaml.load(load_properties_text)
        self.static_entries = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml', 'r')) if self.insurance_company_id else None
        self.__query_procedure_code_types__()
        self.__query_external_procedure_code_types__()
        self.__query_procedure_codes__()
        self.__query_procedure_code_modifiers__()
        self.__query_procedure_labels__()

#        self.normalization_rules = {'M':self.load_properties.get('field_column_mappings'),
#                                    'L':{
#                                        'source':'%s_CLAIM_%s' % (self.claim_file_source_name.upper(), self.imported_claim_file_id),
#                                        'employer_id':self.employer_id
#                                        }
#                                   }

        self.normalization_rules = {'M':{k:claims_util.yaml_formula_insert_simple(self.stage_claim_table, self.stage_claim_table_columns, v, self.stage_claims_table_alias) for k,v in self.load_properties.get('field_column_mappings').iteritems()},
                                    'L':{
                                        'source':'%s_D_CLAIM_%s' % (self.claim_file_source_name.upper(), self.imported_claim_file_id),
                                        'employer_id':self.employer_id
                                        }
                                   }        

    def __query_insurance_companies__(self):
        if not self.insurance_companies:
            self.insurance_companies = {}
            fac_insurance_companies = ModelFactory.get_instance(self.conn, '%s.insurance_companies' % whcfg.master_schema)
            for ic in fac_insurance_companies.table:
                self.insurance_companies[ic['name'].lower()] = ic['id']
        return self.insurance_companies
    
    
    def __initialize__(self):
        return None

    def __parse_raw_address__(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        
        dict_raw_address = {}
        dict_raw_address['street_address'] = normalized_claim.get('street_address')
        dict_raw_address['unit'] = normalized_claim.get('unit')
        dict_raw_address['city'] = normalized_claim.get('city')
        dict_raw_address['state'] = normalized_claim.get('state')
        dict_raw_address['zip'] = normalized_claim.get('zip')
        dict_raw_address['source'] = normalized_claim['source'].rpartition('_')[0]
        dict_raw_address['active_flag'] = 'active'
        dict_raw_address['good_location'] = (normalized_claim.get('street_address') and normalized_claim.get('street_address').strip() <> ''
           and normalized_claim.get('city') and normalized_claim.get('city').strip() <> ''
           and normalized_claim.get('state') and normalized_claim.get('state').strip() <> ''
           and normalized_claim.get('zip') and normalized_claim.get('zip').strip() <> '')
        
        if normalized_claim.get('street_address') and (
              normalized_claim.get('street_address').upper().find('PO BOX') > -1 
           or normalized_claim.get('street_address').upper().find('BILLING ') > -1 
           or normalized_claim.get('street_address').upper().find('NO ADDRESS ') > -1) :
            dict_raw_address['is_billing'] = True
 
        return dict_raw_address

    def process_claims_generic(self, st):
        
        if self.__validation_check(LOG):
            self.__init_x_claims_metadata_generic(LOG)
            self.__refresh_x_claims_generic(LOG)
            self.__refresh_x_claim_procedures_generic(LOG)
            self.__refresh_x_claim_provider_generic(LOG)
            self.__refresh_x_claim_locations_generic(LOG)
            self.__refresh_x_claim_patients_generic(LOG)
            self.insert_claims_generic(LOG)
            self.insert_claim_attributes_generic(LOG) 
        else:
            sys.exit(1)

    def insert_claim_attributes_generic(self, logger):
        
        claim_attributes = self.load_properties.get('claim_attributes', {}) 

        q_insert_claim_attr_list = []
        
        q_insert_claim_attr = """INSERT INTO {claims_master_schema}.dental_claim_attributes (claim_id, name, value, created_at, updated_at)
                                SELECT c.id, '{attribute_name}', {attribute_column_name}, NOW(), NOW()
                                  FROM {claims_master_schema}.{icf_table} aic
                                  JOIN {claims_master_schema}.dental_claims c 
                                    ON aic.imported_claim_file_id=c.imported_claim_file_id 
                                   AND aic.id=c.imported_claim_id
                                 WHERE {attribute_column_name} <> '' AND {attribute_column_name} is not null
                                 AND   aic.imported_claim_file_id = {icf_id}
                                """
        if isinstance(claim_attributes, list):
            for attribute_name in  claim_attributes:
                q_insert = q_insert_claim_attr.format(claims_master_schema=whcfg.claims_master_schema,
                                                       attribute_name=attribute_name,
                                                       icf_table=self.stage_claim_table,                                                      
                                                       attribute_column_name=claims_util.yaml_formula_insert(dbutils.Table(self.conn, self.stage_claim_table), attribute_name, 'aic'),
                                                       icf_id=self.imported_claim_file_id)
                q_insert_claim_attr_list.append({'query':q_insert,
                                                 'description':'Inserting claims_attribute for %s.' % attribute_name,
                                                 'warning_filter':'ignore'})

        else:
            for mapped_attribute_name, attribute_name in  claim_attributes.iteritems():
                q_insert = q_insert_claim_attr.format(claims_master_schema = whcfg.claims_master_schema,
                                                       attribute_name = mapped_attribute_name,
                                                       icf_table=self.stage_claim_table,                                                       
                                                       attribute_column_name=claims_util.yaml_formula_insert(dbutils.Table(self.conn, self.stage_claim_table), attribute_name, 'aic'),
                                                       icf_id=self.imported_claim_file_id)
                q_insert_claim_attr_list.append({'query':q_insert,
                                                 'description':'Inserting claims_attribute for %s.' % mapped_attribute_name,
                                                 'warning_filter':'ignore'})
        
        utils.execute_queries(self.conn, logger, q_insert_claim_attr_list)

    def insert_claims_generic(self,LOG):   
        t_claims = dbutils.Table(self.conn, '%s.dental_claims' % whcfg.claims_master_schema)     
        claim_cols = t_claims.columns()
        
        insert_cols = ','.join([cc for cc in claim_cols if cc <> 'id'])
        insert_stmt = """INSERT INTO {claims_master_schema}.dental_claims ({insert_cols}) 
                         SELECT {insert_cols}
                           FROM {scratch_schema}.x_dental_claims_{imported_claim_file_id}""".format(claims_master_schema=whcfg.claims_master_schema,
                                                                                             scratch_schema=whcfg.scratch_schema,
                                                                                             insert_cols=insert_cols,
                                                                                             imported_claim_file_id=self.imported_claim_file_id)
        cur = self.conn.cursor()
        cur.execute(insert_stmt)
        cur.close()

            
    def __refresh_x_claim_provider_generic(self, LOG):
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)

        update_providers = """UPDATE {x_claims_table} x
                            JOIN {master_schema}.provider_external_ids pei ON pei.external_id_type = 'NPI' and x.provider_pin = pei.external_id
                            SET x.provider_id = pei.provider_id""".format(master_schema=whcfg.master_schema,x_claims_table=x_claims_table, 
                                                                          scratch_schema=whcfg.scratch_schema,
                                                                          imported_claim_file_id=self.imported_claim_file_id)
        cur = self.conn.cursor()
        cur.execute(update_providers)
        cur.close()
         
    def __validation_check(self, LOG):
        q_validation_check = """SELECT count(1) as num FROM dental_claims WHERE imported_claim_file_id=%s""" % self.imported_claim_file_id 
        if isinstance(claim_attributes, list):
            for attribute_name in  claim_attributes:
                q_insert = q_insert_claim_attr.format(claims_master_schema=whcfg.claims_master_schema,
                                                       attribute_name=attribute_name,
                                                       icf_table=self.stage_claim_table,                                                      
                                                       attribute_column_name=claims_util.yaml_formula_insert(dbutils.Table(self.conn, self.stage_claim_table), attribute_name, 'aic'),
                                                       icf_id=self.imported_claim_file_id)
                q_insert_claim_attr_list.append({'query':q_insert,
                                                 'description':'Inserting claims_attribute for %s.' % attribute_name,
                                                 'warning_filter':'ignore'})

        else:
            for mapped_attribute_name, attribute_name in  claim_attributes.iteritems():
                q_insert = q_insert_claim_attr.format(claims_master_schema=whcfg.claims_master_schema,
                                                       attribute_name=mapped_attribute_name,
                                                       icf_table=self.stage_claim_table,                                                       
                                                       attribute_column_name=claims_util.yaml_formula_insert(dbutils.Table(self.conn, self.stage_claim_table), attribute_name, 'aic'),
                                                       icf_id=self.imported_claim_file_id)
                q_insert_claim_attr_list.append({'query':q_insert,
                                                 'description':'Inserting claims_attribute for %s.' % mapped_attribute_name,
                                                 'warning_filter':'ignore'})
        
        utils.execute_queries(self.conn, logger, q_insert_claim_attr_list)

    def __refresh_x_claim_provider_generic(self, LOG):
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)

        update_providers = """UPDATE {x_claims_table} x
                            JOIN {master_schema}.provider_external_ids pei ON pei.external_id_type = 'NPI' and x.provider_pin = pei.external_id
                            SET x.provider_id = pei.provider_id""".format(master_schema=whcfg.master_schema,x_claims_table=x_claims_table, 
                                                                          scratch_schema=whcfg.scratch_schema,
                                                                          imported_claim_file_id=self.imported_claim_file_id)
        cur = self.conn.cursor()
        cur.execute(update_providers)
        cur.close()
         
    def __validation_check(self, LOG):
        q_validation_check = """SELECT count(1) as num FROM dental_claims WHERE imported_claim_file_id=%s""" % self.imported_claim_file_id 
        res = dbutils.Query(self.conn, q_validation_check)
        if res and res.next().get('num') > 0:
            logutil.log(LOG, logutil.CRITICAL, "Claims for imported_claim_file_id:%s have already been normalized. If the intent is to re-normalize, please re-run after performing a cleanup.")
            return False
        
        return True

    def process_claims(self, st):
        self.process_claims_generic(st)
        self.update_claims(LOG)
        
    def update_claims(self, logger):
        cur=self.conn.cursor()
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,self.imported_claim_file_id)
        print (self.stage_claim_table)
        u_network_id = """UPDATE %s.x_dental_claims_%s x join %s.%s dic on (x.imported_claim_id=dic.id) join %s.insurance_networks sp on dic.participating_status=sp.external_network_id
                          SET x.provider_network_id=IF(dic.participating_status != 'NON',sp.id,-1)
                          WHERE dic.participating_status=sp.external_network_id
                          AND sp.insurance_company_id=%s
                          AND sp.type is null""" % (whcfg.scratch_schema,self.imported_claim_file_id,whcfg.claims_master_schema,self.stage_claim_table, whcfg.master_schema, self.insurance_company_id)
        #print % (whcfg.scratch_schema,self.imported_claim_file_id,whcfg.claims_master_schema,self.stage_claim_table, whcfg.master_schema, self.insurance_company_id)
        x_claims_queries = [{'query':u_network_id,
                             'description':'Update provider_network_id in x_dental_claims%s.' % self.imported_claim_file_id}]
        utils.execute_queries(self.conn, logger, x_claims_queries)
        
        dental_claims_update = """UPDATE {claims_master_schema}.dental_claims dc
                                  JOIN {x_claims_table} x ON dc.imported_claim_id = x.imported_claim_id
                                  SET dc.provider_network_id = x.provider_network_id
                                  where dc.imported_claim_file_id = {imported_claim_file_id}""".format(claims_master_schema=whcfg.claims_master_schema,
                                                                                                       x_claims_table=x_claims_table,
                                                                                                       imported_claim_file_id=self.imported_claim_file_id)
        cur.execute(dental_claims_update)
        cur.close()
        
    def __init_x_claims_metadata_generic(self, logger):
        pass
    
    def __refresh_x_claims_generic(self, logger):
        
        
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)
        field_mappings = self.normalization_rules.get('M')
        claims_insert_queries = []
        d_c_icf = """DROP TABLE IF EXISTS {x_claims_table}""".format(x_claims_table=x_claims_table)
        
        claims_insert_queries.append({'query':d_c_icf,
                                      'description':'Drop table if exists {x_claims_table}'.format(x_claims_table=x_claims_table),
                                      'warning_filter':'ignore'})
                                      
        c_c_icf = """CREATE TABLE %s LIKE %s.dental_claims""" % (x_claims_table,
                                                            whcfg.claims_master_schema)
        claims_insert_queries.append({'query':c_c_icf,
                                      'description':'Creating table %s like %s.dental_claims' % (x_claims_table,
                                                                                             whcfg.claims_master_schema)})
       
        l_c = """LOCK TABLES {x_claims_table} WRITE, {claims_schema}.{stage_table} cic WRITE""".format(x_claims_table = x_claims_table,  
                                                             claims_schema = whcfg.claims_master_schema,
                                                             stage_table = self.stage_claim_table)
        claims_insert_queries.append({'query':l_c,
                                      'description':'Acquire lock on %s' % x_claims_table}) 
        
        utils.execute_queries(self.conn, logger, claims_insert_queries)
        claims_insert_queries = []
        
        utils.drop_table_indexes(self.conn, '%s' % (x_claims_table))
        
        t_claims_icf = dbutils.Table(self.conn, '%s' % (x_claims_table))
                
        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        
        member_insert = self.__refresh_x_claim_field_mapping_generic(source_table=t_stage_claim_table,
                                                                       table_alias='cic',
                                                                       field_name_list=['member_first_name',
                                                                                        'member_last_name',
                                                                                        'member_dob',
                                                                                        'member_ssn',
                                                                                        'member_relationship',
                                                                                        'employee_first_name',
                                                                                        'employee_ssn'],
                                                                       derived_fields={'member_signature':['member_first_name',
                                                                                        'member_last_name',
                                                                                        'member_dob',
                                                                                        'member_ssn',
                                                                                        'member_relationship',
                                                                                        'employee_first_name',
                                                                                        'employee_ssn']},
                                                                       logger=logger)
        member_insert['member_sha1'] = 'UNHEX(SHA1(%s))' % member_insert.pop('member_signature')
        
        provider_insert = self.__refresh_x_claim_field_mapping_generic(source_table=t_stage_claim_table,
                                                                       table_alias='cic',
                                                                       field_name_list=['provider_pin',
                                                                                        'street_address',
                                                                                        'unit',
                                                                                        'city',
                                                                                        'state',
                                                                                        'zip'],
                                                                       derived_fields = None,
                                                                       logger=logger)
        procedure_insert = self.__refresh_x_claim_procedure_insert_generic(source_table=t_stage_claim_table, table_alias='cic', logger=logger)
        
        add_columns_map = {}
        add_columns_map.update(member_insert)
        add_columns_map.update(provider_insert)
        add_columns_map.update(procedure_insert)
        
        add_columns_map['address_sha1'] = """UNHEX(SHA1(CONCAT_WS(':',COALESCE({street_address},''),COALESCE({unit},''),COALESCE({city},''),COALESCE({state},''),COALESCE({zip},'')
                                            )))""".format(street_address=field_mappings.get('street_address') if field_mappings.get('street_address') else "''",
                                                          city=field_mappings.get('city') if field_mappings.get('city') else "''",
                                                          state=field_mappings.get('state') if field_mappings.get('state') else "''",
                                                          zip=field_mappings.get('zip') if field_mappings.get('zip') else "''",
                                                          unit=field_mappings.get('unit') if field_mappings.get('zip') else "''")
        address_fields = set(['street_address','unit','city','state','zip'])
        add_columns = set(provider_insert.keys()) | set(member_insert.keys()) | address_fields | set(procedure_insert.keys())
        add_columns = {x:'varchar(255)' for x in add_columns if x not in t_claims_icf.columns()}
        add_columns['address_sha1'] ='binary(20) NOT NULL'
        add_columns['member_sha1'] ='binary(20)'
        add_columns['procedure_code_sha1'] ='binary(20) NOT NULL'

        add_columns_list = ['ADD COLUMN {column_name} {column_definition}'.format(column_name=c, column_definition=d) for c,d in add_columns.iteritems()] 
        alter_claims_icf = 'ALTER TABLE {x_claims_table} {add_columns}'.format(x_claims_table = x_claims_table,
                                                                               add_columns=','.join(add_columns_list))
        claims_insert_queries.append({'query':alter_claims_icf,
                                      'description':'Altering table %s' % (x_claims_table)})
        
        create_address_sha1_index = 'CREATE INDEX ix_address_sha1 ON {x_claims_table}(address_sha1)'.format(x_claims_table = x_claims_table)
        create_member_sha1_index = 'CREATE INDEX ix_member_sha1 ON {x_claims_table}(member_sha1)'.format(x_claims_table = x_claims_table)
        create_procedure_code_sha1_index = 'CREATE INDEX ix_procedure_code_sha1 ON {x_claims_table}(procedure_code_sha1)'.format(x_claims_table = x_claims_table) 
       
        claims_insert_queries.append({'query':create_address_sha1_index,
                                      'description':'create index ix_address_sha1 on table {x_claims_table}'.format(x_claims_table = x_claims_table)})
        claims_insert_queries.append({'query':create_member_sha1_index,
                                      'description':'create index ix_member_sha1 on table {x_claims_table}'.format(x_claims_table = x_claims_table)})
        claims_insert_queries.append({'query':create_procedure_code_sha1_index,
                                      'description':'create index ix_procedure_code_sha1 on table {x_claims_table}'.format(x_claims_table = x_claims_table)})

       # allowed_amount_value = 'COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0)' % (field_mappings.get('paid_amount') if field_mappings.get('paid_amount') else '0',
#                                                                                                                               field_mappings.get('copay_amount') if field_mappings.get('copay_amount') else '0',
  #                                                                                                                             field_mappings.get('cob_amount') if field_mappings.get('cob_amount') else '0',
 #                                                                                                                              field_mappings.get('coinsurance_amount') if field_mappings.get('coinsurance_amount') else '0',
   #                                                                                                                            field_mappings.get('deductible_amount') if field_mappings.get('deductible_amount') else '0') if not field_mappings.get('allowed_amount') \
    #                        else field_mappings.get('allowed_amount')
        
        insert_map = {'imported_claim_file_id':str(self.imported_claim_file_id),
                      'imported_claim_id':'id',
                      'source_claim_number':field_mappings.get('source_claim_number'),
                      'source_claim_line_number':field_mappings.get('source_claim_line_number') if field_mappings.get('source_claim_line_number') else '001',
                      'employer_id':str(self.employer_id),
                      'insurance_company_id':str(self.insurance_company_id),
                      'provider_id':'-1',
                      'provider_name':"TRIM(%s)" % (field_mappings.get('provider_name')),
                      'provider_location_id':'-1',
                      'units_of_service':'%s' %('IF(%s=0,1,COALESCE(%s,1))' % (field_mappings.get('units_of_service'),field_mappings.get('units_of_service')) if field_mappings.get('units_of_service') else '1'),
                      'subscriber_patient_id':'-1',
                      'patient_id':'-1',
                      'internal_member_hash':field_mappings.get('internal_member_hash'),
                      'member_id':'SHA1(%s)' % field_mappings.get('member_id'),
                      'procedure_label_id':'-1',
                      'parse_status':'1',
                      'parse_comment':"''",
                      'service_date':"IF(%s < '1970-01-01', NULL, %s)" % (field_mappings.get('service_date'),
                                                                                field_mappings.get('service_date')),
                      'payment_date':"IF(%s < '1970-01-01', NULL, %s)" % (field_mappings.get('payment_date'),
                                                                                field_mappings.get('payment_date')),
                      'out_of_network':field_mappings.get('out_of_network') if field_mappings.get('out_of_network') else '1' ,
                      'cob_amount':'COALESCE(%s,0)' % field_mappings.get('cob_amount') if field_mappings.get('cob_amount') else '0',
                      'charged_amount':'COALESCE(%s,0)' % field_mappings.get('charged_amount') if field_mappings.get('charged_amount') else '0',
                      'approved_amount':'COALESCE(%s,0)' % field_mappings.get('approved_amount') if field_mappings.get('approved_amount') else '0',
                      'copay_amount':'COALESCE(%s,0)' % field_mappings.get('copay_amount') if field_mappings.get('copay_amount') else '0',
                      'deductible_amount':'COALESCE(%s,0)' % field_mappings.get('deductible_amount') if field_mappings.get('deductible_amount') else '0',
                      'paid_amount':'COALESCE(%s,0)' % field_mappings.get('paid_amount') if field_mappings.get('paid_amount') else '0',
                      'not_covered_amount':'COALESCE(%s,0)' % field_mappings.get('not_covered_amount') if field_mappings.get('not_covered_amount') else '0',
                      'allowed_amount':'COALESCE(%s,0)' % field_mappings.get('allowed_amount') if field_mappings.get('allowed_amount') else '0',
                      'savings_amount': 'COALESCE(%s,0)' % field_mappings.get('savings_amount') if field_mappings.get('savings_amount') else '0',
                      'imported_at':'NOW()',
                      'updated_at':'NOW()',                    
                      'service_place_id':'-1',
                      'provider_network_id': '-1',
                      'patient_paid_amount':'COALESCE(%s,0)' % field_mappings.get('patient_paid_amount') if field_mappings.get('patient_paid_amount') else '0',
                      'coinsurance_amount':'COALESCE(%s,0)' % field_mappings.get('coinsurance_amount') if field_mappings.get('coinsurance_amount') else '0',
                      'benefit_level_percentage':'COALESCE(%s,0)' % field_mappings.get('benefit_level_percentage') if field_mappings.get('benefit_level_percentage') else '0',
                      'payer_load_date':"IF(%s < '1970-01-01', NULL, %s)" % (field_mappings.get('payer_load_date'),field_mappings.get('payer_load_date'))
        }

       # import pdb
#	pdb.set_trace() 
        insert_map.update(add_columns_map)
        insert_cols = insert_map.keys()
        insert_vals = [insert_map.get(k) for k in insert_cols]
        q_insert_claims = """INSERT INTO %s (%s)
                            SELECT %s
                            FROM %s.%s cic
                            WHERE imported_claim_file_id = %s
                            AND duplicate_of_claim_id is null
                            %s""" % (x_claims_table,
                                     ',\n'.join(insert_cols),
                                     ',\n'.join(insert_vals),
                                     whcfg.claims_master_schema,
                                     self.stage_claim_table,
                                     self.imported_claim_file_id,
                                     LIMIT_CLAUSE)
        
        claims_insert_queries.append({'query':q_insert_claims,
                                      'description':'Insert new claims records into %s' % (x_claims_table),
                                      'warning_filter':'ignore'})        
       # import pdb
#	pdb.set_trace()
        utils.execute_queries(self.conn, logger, claims_insert_queries)
        claims_insert_queries = []
 #       import pdb
#	pdb.set_trace() 
#        ac_c_icf = """SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='x_claims%s_%s'""" % (whcfg.scratch_schema, self.claims_table_suffix, self.imported_claim_file_id)
#        ac_c_icf_r = Query(self.conn, ac_c_icf)
#        ac_claims_icf = ac_c_icf_r.next()['AUTO_INCREMENT']
#                        
#        a_c_ac = """ALTER TABLE %s.claims%s AUTO_INCREMENT = %d""" % (whcfg.claims_master_schema, self.claims_table_suffix, ac_claims_icf)
#        claims_insert_queries.append({'query':a_c_ac,
#                                      'description':'Setting AUTO_INCREMENT on table %s.claims%s' % (whcfg.claims_master_schema, 
#                                                                                                     self.claims_table_suffix)})
                    
        ul_c = """UNLOCK TABLES"""
        claims_insert_queries.append({'query':ul_c,
                                      'description':'Release lock on table %s' % (x_claims_table)})
        utils.execute_queries(self.conn, logger, claims_insert_queries)

    def __refresh_x_claim_locations_generic(self, logger):
        '''Normalize raw locations via address_utils.normalize_address().
           Insert them into stage_location and populate intermediate table address_sha1_match_key_unit_sha1.'''

        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)
        pq_sql_file_name = 'claims/model/ddl/stage_location.sql'
        fq_sql_file_name = os.path.join(whcfg.providerhome, pq_sql_file_name)
        placeholder_re_subs = [('STAGE_LOCATION','stage_location%s' % (self.imported_claim_file_id)), ('SCHEMA_NAME',whcfg.scratch_schema)]
        import_utils.execute_sql_from_file(self.conn, fq_sql_file_name, placeholder_re_subs=placeholder_re_subs)
        logutil.log(logger, logutil.INFO, "Created stage_location table in db %s using %s" % (whcfg.scratch_schema, fq_sql_file_name))

    
        pq_sql_file_name = 'claims/model/ddl/address_sha1_match_key_unit_sha1.sql'
        fq_sql_file_name = os.path.join(whcfg.providerhome, pq_sql_file_name)
        placeholder_re_subs = [('ADDRESS_SHA1_MATCH_KEY_UNIT_SHA1','address_sha1_match_key_unit_sha1%s' % (self.imported_claim_file_id)), ('SCHEMA_NAME',whcfg.scratch_schema)]
        import_utils.execute_sql_from_file(self.conn, fq_sql_file_name, placeholder_re_subs=placeholder_re_subs)
        logutil.log(logger, logutil.INFO, "Created address_sha1_match_key_unit_sha1 table in db %s using %s" % (whcfg.scratch_schema, fq_sql_file_name))


        logutil.log(logger, logutil.INFO, "Querying %s for locations to be normalized" % (x_claims_table))
        stage_cursor = self.conn.cursor()
        q_select_address = '''
                              SELECT address_sha1
                              ,      street_address
                              ,      unit
                              ,      city
                              ,      state
                              ,      zip
                              FROM %s
                              WHERE street_address is not null
                                OR city is not null
                                OR state is not null
                                OR zip is not null
                              GROUP BY address_sha1
                           ''' % (x_claims_table)
        stage_cursor.execute(q_select_address)
    
        # raw to stage mapping table
        q_insert_amu = '''
                          INSERT INTO %s.address_sha1_match_key_unit_sha1%s
                          (
                           address_sha1
                          ,match_key_unit_sha1
                          )
                          values (%s, %s)
                       ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s')
        qv_insert_amu = []
    
        # raw to stage insert
        q_insert_sl =  '''
                         INSERT INTO %s.stage_location%s
                         (
                           stage_location_id
                          ,source
                          ,match_key
                          ,query_address
                          ,building_name
                          ,street_address
                          ,unit
                          ,city
                          ,state_code
                          ,postal_code
                          ,country_code
                          ,master_location_id
                          ,match_key_unit_sha1
                          ,requested_action
                          ,performed_action
                         )
                         values (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, NULL)
                         ON DUPLICATE KEY UPDATE match_key_unit_sha1 = VALUES(match_key_unit_sha1)
                      ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
        qv_insert_sl = []
        
        logutil.log(logger, logutil.INFO, "Normalizing locations from %s" % x_claims_table)
        for row in stage_cursor.fetchall():
            (address_sha1, street, unit, city, state, zip, requested_action) = (row['address_sha1'], row['street_address'], row['unit'], row['city'], row['state'], row['zip'], 'upsert')
    
            dict_raw_address = {}
            dict_raw_address['street_address'] = street
            dict_raw_address['unit'] = unit
            dict_raw_address['city'] = city
            dict_raw_address['state'] = state
            dict_raw_address['zip'] = zip

            #normalize_address and cleanse_address come from $PROVIDERHOME/util/address_util.py
            cleansed_address = cleanse_address(dict_raw_address)
            if (cleansed_address['state'] is None or cleansed_address['city'] is None) and (cleansed_address['zip'] is None):
                continue

            normalized_address = normalize_address(cleansed_address)

            if normalized_address['state'] is None and normalized_address['zip'] is None:
                continue
            # we don't need to verify len(normalized_address['match_key']) <= 115 and len(normalized_address['unit']) <= 50 since inserting this data into stage_location will fail if these two conditions are not met
            match_key_unit_sha1 = hashlib.sha1(normalized_address['match_key']+':'+(normalized_address['unit'] or '')).digest()
            qv_insert_amu.append((address_sha1,match_key_unit_sha1))
            qv_insert_sl.append(('DENTAL_CLAIMS_%s' % self.imported_claim_file_id,normalized_address['match_key'],normalized_address['query_address'],normalized_address['building_name'],normalized_address['street_address'],normalized_address['unit'],normalized_address['city'],normalized_address['state'],normalized_address['zip'],normalized_address['country'],match_key_unit_sha1,requested_action))
    
        stage_cursor.executemany(q_insert_amu, qv_insert_amu)
        logutil.log(logger, logutil.INFO, "Inserted %s mapping row(s) in address_sha1_match_key_unit_sha1 table in db %s" % (import_utils.get_row_count(self.conn), whcfg.scratch_schema))
        try:
            stage_cursor.executemany(q_insert_sl, qv_insert_sl)
            logutil.log(logger, logutil.INFO, "Inserted %s cleansed row(s) in stage_location table in db %s" % (import_utils.get_row_count(self.conn), whcfg.scratch_schema))
        except:
            logutil.log(logger, logutil.INFO, "Inserted %s cleansed row(s) in stage_location table in db %s" % (import_utils.get_row_count(self.conn), whcfg.scratch_schema))
        # process stage_location rows
        # update sl records (for both upsert and resolve)
        # we make a conscious decision not to update locations.updated_at as nothing really changed
        pq_template_sql_file_name = 'claims/model/dml/sl_update_template.sql'
        fq_template_sql_file_name = os.path.join(whcfg.providerhome, pq_template_sql_file_name)
        placeholder_re_subs = [('MASTER_SCHEMA',whcfg.master_schema), ('STAGE_LOCATION','stage_location%s' % (self.imported_claim_file_id)), ('SCHEMA_NAME',whcfg.scratch_schema)]
        import_utils.execute_sql_from_file(self.conn, fq_template_sql_file_name, placeholder_re_subs=placeholder_re_subs)
        logutil.log(logger, logutil.INFO, "Updated %s row(s) in stage_location in db %s using %s" % (import_utils.get_row_count(self.conn), whcfg.scratch_schema, fq_template_sql_file_name))
    
        # insert l records and update sl records (for upsert only)
        pq_template_sql_file_name = 'claims/model/dml/l_insert_sl_update_template.sql'
        fq_template_sql_file_name = os.path.join(whcfg.providerhome, pq_template_sql_file_name)
        placeholder_re_subs = [('MASTER_SCHEMA',whcfg.master_schema), ('STAGE_LOCATION','stage_location%s' % (self.imported_claim_file_id)), ('SCHEMA_NAME',whcfg.scratch_schema)]
        import_utils.execute_sql_from_file(self.conn, fq_template_sql_file_name, placeholder_re_subs=placeholder_re_subs)
        logutil.log(logger, logutil.INFO, "Inserted %s row(s) into locations, updated row(s) in stage_location in dbs %s and %s, respectively, using %s" % (import_utils.get_row_count(self.conn), whcfg.master_schema, whcfg.scratch_schema, fq_template_sql_file_name))
        
        a_provider_location_id = """UPDATE %s.address_sha1_match_key_unit_sha1%s s
                                                JOIN %s.stage_location%s sl USING(match_key_unit_sha1)
                                                 SET s.provider_location_id=sl.master_location_id""" % (whcfg.scratch_schema,
                                                                                                        self.imported_claim_file_id,
                                                                                                        whcfg.scratch_schema,
                                                                                                        self.imported_claim_file_id)
        
        stage_cursor.execute(a_provider_location_id)
        x_claim_provider_location_update = """UPDATE %s x 
                                                JOIN %s.address_sha1_match_key_unit_sha1%s s  USING(address_sha1)
                                                 SET x.provider_location_id=s.provider_location_id""" % (x_claims_table,
                                                                                                        whcfg.scratch_schema,
                                                                                                        self.imported_claim_file_id)
        
        stage_cursor.execute(x_claim_provider_location_update)
        logutil.log(logger, logutil.INFO, "Updated %s with provider_location_id" % (x_claims_table))
        
        stage_cursor.close()
        
    def __refresh_x_claim_field_insert_generic(self, source_table, table_alias, field_name_list, derived_fields, logger):
        
        field_insert_list = ['{field_expression} as {field_name}\n'.format(field_expression=v, field_name=k) for k,v in self.__refresh_x_claim_field_mapping_generic(source_table, table_alias, field_name_list, logger).iteritems()]
#        
#        field_insert_list =  ['{field_expression} as {field_name}\n'.format(field_expression=claims_util.yaml_formula_insert(source_table, self.normalization_rules.get('M').get(field_name), table_alias), field_name=field_name) 
#                              for field_name in field_name_list if self.normalization_rules.get('M').get(field_name)]
#        
        return ','.join(field_insert_list)

    def __refresh_x_claim_field_mapping_generic(self, source_table, table_alias, field_name_list, derived_fields, logger):
        
        field_insert_map =  {field_name:self.normalization_rules.get('M').get(field_name) for field_name in field_name_list if self.normalization_rules.get('M').get(field_name)}
        derived_fields_insert_map = {}
        if derived_fields:
            derived_fields_insert_map = {k:"CONCAT_WS(':',%s)" % (','.join(["COALESCE(%s,'')" % field_insert_map.get(c) if c in field_insert_map.keys() else "''" for c in v])) for k,v in derived_fields.iteritems()}
        
        field_insert_map.update(derived_fields_insert_map)
        return field_insert_map

    def rehash_dental_claim_patients(self, logger,subscriber_patient_account_id = None, patient_account_id = None, rehash_unidentified = False):

        x_dental_claims_table = """%s.x_dental_claims_rehash%s_%s""" % (whcfg.scratch_schema, self.claims_table_suffix, self.imported_claim_file_id)
        claims_insert_queries = []
        filter = ""                    
        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        member_insert = self.__refresh_x_claim_field_mapping_generic(source_table=t_stage_claim_table,

                                                                       table_alias='cic',

                                                                       field_name_list=['member_first_name',

                                                                                        'member_last_name',

                                                                                        'member_dob',

                                                                                        'member_ssn',

                                                                                        'member_relationship',

                                                                                        'employee_first_name',

                                                                                        'employee_ssn'],

                                                                       derived_fields={'member_signature':['member_first_name',

                                                                                        'member_last_name',

                                                                                        'member_dob',

                                                                                        'member_ssn',

                                                                                        'member_relationship',

                                                                                        'employee_first_name',

                                                                                        'employee_ssn']},

                                                                       logger=logger)  

        if subscriber_patient_account_id != None and subscriber_patient_account_id != '':
            filter_query = """select p.first_name,p.last_name,p.date_of_birth,p.ssn from patients p join accounts a on a.patient_id = p.id
                               where a.id = {account_id} and a.is_active = 1""".format(account_id = subscriber_patient_account_id)
            filter_result = Query(self.conn, filter_query)

            filter_result = filter_result.next()

            filter += "%s %s" %(" and %s = '%s'" %(member_insert['employee_first_name'], filter_result['first_name']) if member_insert['employee_first_name'] != None else "",

                                " and %s = '%s'" %(member_insert['employee_ssn'],filter_result['ssn']) if member_insert['employee_ssn'] and filter_result['ssn'] else "")  if filter_result else " and False"

        

        if patient_account_id != None and patient_account_id != '':

            filter_query = """select p.first_name,p.last_name,p.date_of_birth,p.ssn from patients p 

                            join accounts a on a.patient_id = p.id

                            where a.id = {account_id} and a.is_active = 1""".format(account_id = patient_account_id)

            filter_result = Query(self.conn, filter_query)

            filter_result = filter_result.next()

            filter += "%s%s%s%s" %(" and %s = '%s'" %(member_insert['member_first_name'], filter_result['first_name']) if member_insert['member_first_name'] else "",

                                   " and %s = '%s'" %(member_insert['member_last_name'], filter_result['last_name'] ) if member_insert['member_last_name'] and filter_result['last_name'] else "",

                                   " and %s = '%s'" %(member_insert['member_dob'], filter_result['date_of_birth']) if member_insert['member_dob'] and filter_result['date_of_birth'] else "",

                                   " and %s = '%s'" %(member_insert['member_ssn'], filter_result['ssn']) if member_insert['member_ssn'] and filter_result['ssn'] else "")  if filter_result else " and False" 

      

        if rehash_unidentified:

            filter += """and c.patient_id = -1"""

        member_insert['member_sha1'] = 'UNHEX(SHA1(%s))' % member_insert.pop('member_signature')

        member_insert_keys = member_insert.keys()

        member_insert_vals = [member_insert.get(key) for key in member_insert_keys]

        d_c_icf = """DROP TABLE IF EXISTS {x_dental_claims_table}""".format(x_dental_claims_table=x_dental_claims_table)

        c_c_icf = """CREATE TABLE {x_dental_claims_table} 

                    (imported_claim_id INT(11),

                     {claim_patient_column_defs},

                     subscriber_patient_id INT(11),

                     patient_id INT(11),

                     INDEX ix_member_sha1(member_sha1))

                     """.format(x_dental_claims_table=x_dental_claims_table,

                                claim_patient_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(member_insert_keys)]))

        i_c_icf = """INSERT INTO {x_dental_claims_table}

                     SELECT cic.id,

                     {claim_patient_columns},

                     -1,

                     -1

                     FROM {claims_master_schema}.{stage_table} cic

                     JOIN dental_claims c on c.imported_claim_id = cic.id and cic.imported_claim_file_id = c.imported_claim_file_id

                     WHERE cic.imported_claim_file_id = {imported_claim_file_id}

                     {filter}

                     """.format(x_dental_claims_table=x_dental_claims_table,

                                claim_patient_columns=','.join([member_insert.get(key) for key in sorted(member_insert_keys)]),

                                claims_master_schema=whcfg.claims_master_schema,

                                stage_table=self.stage_claim_table,

                                imported_claim_file_id=self.imported_claim_file_id,

                                filter = "%s" % filter if filter else '')

        claims_insert_queries.extend([{'query':d_c_icf,

                                      'description':'Drop table if exists {x_dental_claims_table}'.format(x_dental_claims_table=x_dental_claims_table),

                                      'warning_filter':'ignore'},

                                     {'query':c_c_icf,

                                      'description':'Create table {x_dental_claims_table}'.format(x_dental_claims_table=x_dental_claims_table),

                                      'warning_filter':'ignore'},

                                     {'query':i_c_icf,

                                      'description':'Insert into {x_dental_claims_table}'.format(x_dental_claims_table=x_dental_claims_table),

                                      'warning_filter':'ignore'}]) 

        utils.execute_queries(self.conn, logger, claims_insert_queries) 

        drop_patients_icf = """DROP TABLE IF EXISTS {scratch_schema}.dental_claim_patients_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,

                                                                                                                        imported_claim_file_id=self.imported_claim_file_id)

        create_patients_icf = """CREATE TABLE {scratch_schema}.dental_claim_patients_{imported_claim_file_id} 

                                 ({claim_patient_column_defs},

                                 subscriber_patient_id INT(11),

                                 patient_id INT(11),

                                 INDEX ix_member_sha1(member_sha1))

                                 AS SELECT {claim_patient_columns},

                                 subscriber_patient_id,

                                 patient_id

                                 FROM {x_dental_claims_table}

                                 GROUP BY member_sha1

                                 """.format(scratch_schema=whcfg.scratch_schema,

                                            imported_claim_file_id=self.imported_claim_file_id,

                                            claim_patient_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(member_insert_keys)]),

                                            claim_patient_columns=','.join([key for key in sorted(member_insert_keys)]),

                                            x_dental_claims_table=x_dental_claims_table)

        

        claims_patients_queries = [{'query': drop_patients_icf,

                                    'description':'Drop {scratch_schema}.dental_claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,

                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id),

                                    'warning_filter':'ignore'},

                                   {'query':create_patients_icf,

                                    'description':'Create {scratch_schema}.dental_claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,

                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)}]

        utils.execute_queries(self.conn, logger, claims_patients_queries)

        t_dental_claim_patients = dbutils.Table(self.conn, '{scratch_schema}.dental_claim_patients_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id))

        t_dental_claim_patient_columns = t_dental_claim_patients.columns()

        is_relationship_available = 'member_relationship'  in t_dental_claim_patient_columns

        is_member_ssn_available = 'member_ssn' in t_dental_claim_patient_columns

        is_subscriber_first_name_available = 'employee_first_name' in t_dental_claim_patient_columns

        subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber')) if is_relationship_available else set([])

        subscriber_codes = set([x.lower() for x in subscriber_codes if x])

        q_update_member = '''UPDATE %s.dental_claim_patients_%s

                                SET subscriber_patient_id = %s,

                                    patient_id = %s

                             WHERE member_sha1 = %s

                       ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s')

                       

        qv_update_member = []             

        suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))

          

        for i, cp in enumerate(t_dental_claim_patients):

            pi = claims_util.PatientIdentifier()

            is_subscriber = True if (is_relationship_available and cp.get('member_relationship') and cp.get('member_relationship').lower() in subscriber_codes) else None

            patient_info = {'conn':self.conn,

                'subscriber_ssn':cp.get('employee_ssn'),

                'subscriber_first_name':cp.get('employee_first_name'),

                'subscriber_last_name':None,  

                'member_ssn':cp.get('member_ssn'), 

                'member_first_name':cp.get('member_first_name'), 

                'member_dob':cp.get('member_dob'),

                'member_last_name':cp.get('member_last_name'), 

                'is_relationship_available':is_relationship_available,

                'is_subscriber':is_subscriber,

                'is_member_ssn_available':is_member_ssn_available,

                'is_subscriber_first_name_available':is_subscriber_first_name_available,

                'insurance_company_id':self.insurance_company_id }

            if (not self.load_properties.get('field_column_mappings').get('patient_identifier_type')) or self.load_properties.get('field_column_mappings').get('patient_identifier_type') == 'ssn':

                return_dict = pi.resolve_generic_claim_patient(**patient_info)

            else:

                return_dict = pi.resolve_nonssn_claim_patient(conn=patient_info['conn'],

                                                           subscriber_identifier=patient_info['subscriber_ssn'],

                                                           subscriber_first_name=patient_info['subscriber_first_name'], 

                                                           subscriber_last_name=patient_info['subscriber_last_name'],                                                         

                                                           member_identifier=patient_info['member_ssn'], 

                                                           member_first_name=patient_info['member_first_name'], 

                                                           member_dob=patient_info['member_dob'],

                                                           member_last_name=patient_info['member_last_name'], 

                                                           is_relationship_available=patient_info['is_relationship_available'],

                                                           is_subscriber=patient_info['is_subscriber'],

                                                           is_member_identifier_available=patient_info['is_member_ssn_available'],

                                                           is_subscriber_first_name_available=patient_info['is_subscriber_first_name_available'],

                                                           insurance_company_id=self.insurance_company_id,

                                                           employer_id=self.employer_id,

                                                           identifier_type=self.load_properties.get('field_column_mappings').get('patient_identifier_type'))

                

            if return_dict.get('patient_id') == -1 and return_dict.get('subscriber_patient_id') > -1 and patient_info['member_first_name']:    

                fn_array = patient_info['member_first_name'].split()

                if len(fn_array) > 1 and len(fn_array[-1]) == 1:

                    fn_array.pop() ##remove trailing middle initial

                    patient_info['member_first_name'] = ' '.join(fn_array)

                    return_dict = pi.resolve_generic_claim_patient(**patient_info)

        

            if return_dict.get('subscriber_patient_id') > -1:

                if suppress_dependents:

                    if return_dict.get('subscriber_patient_id') == return_dict.get('patient_id'):

                        qv_update_member.append((return_dict.get('subscriber_patient_id'), return_dict.get('patient_id'), cp.get('member_sha1')))

                else:    

                    qv_update_member.append((return_dict.get('subscriber_patient_id'), return_dict.get('patient_id'), cp.get('member_sha1')))

        cur = self.conn.cursor()

        

        if qv_update_member:

            cur.executemany(q_update_member, qv_update_member) 

            

            x_dental_claims_update_patients = """UPDATE {x_dental_claims_table} x

                                            JOIN {scratch_schema}.dental_claim_patients_{imported_claim_file_id} cp USING (member_sha1)

                                             SET x.subscriber_patient_id=cp.subscriber_patient_id,

                                                 x.patient_id=cp.patient_id""".format(x_dental_claims_table=x_dental_claims_table,scratch_schema=whcfg.scratch_schema,

                                                                                      imported_claim_file_id=self.imported_claim_file_id)

            cur.execute(x_dental_claims_update_patients)

            

        claims_update_patients = """UPDATE {claims_master_schema}.dental_claims{claims_table_suffix} c

                                  JOIN {x_dental_claims_table} x ON c.imported_claim_id = x.imported_claim_id

                                  SET c.subscriber_patient_id = x.subscriber_patient_id,

                                  c.patient_id =  x.patient_id

                                  where c.imported_claim_file_id = {imported_claim_file_id}""".format(claims_master_schema=whcfg.claims_master_schema,

                                                                                                      claims_table_suffix=self.claims_table_suffix,

                                                                                                      x_dental_claims_table=x_dental_claims_table,

                                                                                                      imported_claim_file_id=self.imported_claim_file_id)

        cur.execute(claims_update_patients)

        cur.close()         
    def __refresh_x_claim_patients_generic(self, logger):

        claims_insert_queries = []
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)

        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        member_insert = self.__refresh_x_claim_field_mapping_generic(source_table=t_stage_claim_table,
                                                                       table_alias='cic',
                                                                       field_name_list=['member_first_name',
                                                                                        'member_last_name',
                                                                                        'member_dob',
                                                                                        'member_ssn',
                                                                                        'member_relationship',
                                                                                        'employee_first_name',
                                                                                        'employee_ssn'],
                                                                       derived_fields={'member_signature':['member_first_name',
                                                                                        'member_last_name',
                                                                                        'member_dob',
                                                                                        'member_ssn',
                                                                                        'member_relationship',
                                                                                        'employee_first_name',
                                                                                        'employee_ssn']},
                                                                       logger=logger)     
           
        member_insert['member_sha1'] = 'UNHEX(SHA1(%s))' % member_insert.pop('member_signature')

        member_insert_keys = member_insert.keys()
        member_insert_vals = [member_insert.get(key) for key in member_insert_keys]


        drop_patients_icf = """DROP TABLE IF EXISTS {scratch_schema}.dental_claim_patients_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                        imported_claim_file_id=self.imported_claim_file_id)
        create_patients_icf = """CREATE TABLE {scratch_schema}.dental_claim_patients_{imported_claim_file_id} 
                                 ({claim_patient_column_defs},
                                 subscriber_patient_id INT(11),
                                 patient_id INT(11),
                                 INDEX ix_member_sha1(member_sha1))
                                 AS SELECT {claim_patient_columns},
                                 subscriber_patient_id,
                                 patient_id
                                 FROM {x_claims_table}
                                 GROUP BY member_sha1
                                 """.format(scratch_schema=whcfg.scratch_schema,
                                            imported_claim_file_id=self.imported_claim_file_id,
                                            claim_patient_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(member_insert_keys)]),
                                            claim_patient_columns=','.join([key for key in sorted(member_insert_keys)]),
                                            x_claims_table=x_claims_table)

        claims_patients_queries = [{'query': drop_patients_icf,
                                    'description':'Drop {scratch_schema}.dental_claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id),
                                    'warning_filter':'ignore'},
                                   {'query':create_patients_icf,
                                    'description':'Create {scratch_schema}.dental_claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)}]
        utils.execute_queries(self.conn, logger, claims_patients_queries)

        t_claim_patients = dbutils.Table(self.conn, '{scratch_schema}.dental_claim_patients_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id))
        t_claim_patient_columns = t_claim_patients.columns()
        is_relationship_available = 'member_relationship'  in t_claim_patient_columns
        is_member_ssn_available = 'member_ssn' in t_claim_patient_columns
        is_subscriber_first_name_available = 'employee_first_name' in t_claim_patient_columns
        
        subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber')) if is_relationship_available else set([])
        subscriber_codes = set([x.lower() for x in subscriber_codes if x])

        q_update_member = '''UPDATE %s.dental_claim_patients_%s
                                SET subscriber_patient_id = %s,
                                patient_id = %s
                             WHERE member_sha1 = %s
                       ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s')

        qv_update_member = []

        

        for i, cp in enumerate(t_claim_patients):
            pi = claims_util.PatientIdentifier()
            is_subscriber = True if (is_relationship_available and cp.get('member_relationship') and cp.get('member_relationship').lower() in subscriber_codes) else None
            patient_info = {'conn':self.conn,
                'subscriber_ssn':cp.get('employee_ssn'),
                'subscriber_first_name':cp.get('employee_first_name'),
                'subscriber_last_name':None,  
                'member_ssn':cp.get('member_ssn'), 
                'member_first_name':cp.get('member_first_name'), 
                'member_dob':cp.get('member_dob'),
                'member_last_name':cp.get('member_last_name'), 
                'is_relationship_available':is_relationship_available,
                'is_subscriber':is_subscriber,
                'is_member_ssn_available':is_member_ssn_available,
                'is_subscriber_first_name_available':is_subscriber_first_name_available,
                'insurance_company_id':self.insurance_company_id }
        
            if (not self.load_properties.get('field_column_mappings').get('patient_identifier_type')) or self.load_properties.get('field_column_mappings').get('patient_identifier_type') == 'ssn':
                return_dict = pi.resolve_generic_claim_patient(**patient_info)
            else:
                return_dict = pi.resolve_nonssn_claim_patient(conn=patient_info['conn'],
                                                           subscriber_identifier=patient_info['subscriber_ssn'],
                                                           subscriber_first_name=patient_info['subscriber_first_name'], 
                                                           subscriber_last_name=patient_info['subscriber_last_name'],                                                         
                                                           member_identifier=patient_info['member_ssn'], 
                                                           member_first_name=patient_info['member_first_name'], 
                                                           member_dob=patient_info['member_dob'],
                                                           member_last_name=patient_info['member_last_name'], 
                                                           is_relationship_available=patient_info['is_relationship_available'],
                                                           is_subscriber=patient_info['is_subscriber'],
                                                           is_member_identifier_available=patient_info['is_member_ssn_available'],
                                                           is_subscriber_first_name_available=patient_info['is_subscriber_first_name_available'],
                                                           insurance_company_id=self.insurance_company_id,
                                                           employer_id=self.employer_id,
                                                           identifier_type=self.load_properties.get('field_column_mappings').get('patient_identifier_type')) 
                                            
            if return_dict.get('patient_id') == -1 and return_dict.get('subscriber_patient_id') > -1 and patient_info['member_first_name']:
                fn_array = patient_info['member_first_name'].split()
                if len(fn_array) > 1 and len(fn_array[-1]) == 1:
                    fn_array.pop() ##remove trailing middle initial
                    patient_info['member_first_name'] = ' '.join(fn_array)
                    return_dict = pi.resolve_generic_claim_patient(**patient_info)

            if return_dict.get('subscriber_patient_id') > -1:
                qv_update_member.append((return_dict.get('subscriber_patient_id'), return_dict.get('patient_id'), cp.get('member_sha1')))
        
        cur = self.conn.cursor()

        if qv_update_member:
            cur.executemany(q_update_member, qv_update_member)

            x_claims_update_patients = """UPDATE {x_claims_table} x
                                            JOIN {scratch_schema}.dental_claim_patients_{imported_claim_file_id} cp USING (member_sha1)
                                             SET x.subscriber_patient_id=cp.subscriber_patient_id,
                                                 x.patient_id=cp.patient_id""".format(x_claims_table=x_claims_table,scratch_schema=whcfg.scratch_schema,
                                                                                      imported_claim_file_id=self.imported_claim_file_id)
            cur.execute(x_claims_update_patients)

        cur.close()
        
    def __refresh_x_claim_procedure_insert_generic(self, source_table, table_alias, logger):
        
        icf_columns = source_table.columns()
        
        proc_insert = ''
        proc_insert_map = {}
        # make the prefix _secondary if we add secondary_procedure_labe_id to dental_claims
        prefix = ''
        
        procedure_code_type_column = self.normalization_rules['M'].get('{prefix}procedure_code_type'.format(prefix=prefix))
        
#        if not procedure_code_type_column or procedure_code_type_column.lower() not in icf_columns:
#            procedure_code_type_column = None
        
        procedure_code_type_mappings = {k:v for k,v in self.load_properties.get('procedure_code_types').iteritems() if v and v.strip() != ''}

        if (procedure_code_type_column and not procedure_code_type_mappings):
            logutil.log(logger, logutil.CRITICAL, "{prefix}Procedure Code Type column provided, but there are no Procedure Code Type mappings".format(prefix=prefix))
            
        if (not procedure_code_type_column and procedure_code_type_mappings):
            logutil.log(logger, logutil.CRITICAL, "Procedure Code Type mappings provided, but there is no {prefix}Procedure Code Type column".format(prefix=prefix))
                    
        if (procedure_code_type_column and procedure_code_type_mappings):
                        
            if_clause = '{else_clause}'
            if_clause_insert = """IF({procedure_code_type_column}='{external_code_type}','{internal_code_type}',{else_clause})""".format(procedure_code_type_column=procedure_code_type_column,
                                                                                                                                                        external_code_type='{external_code_type}',
                                                                                                                                                        internal_code_type='{internal_code_type}',
                                                                                                                                                        else_clause='{else_clause}')
            
            for internal_code_type, external_code_type in procedure_code_type_mappings.iteritems():
                if_clause = if_clause.format(else_clause=if_clause_insert.format(external_code_type=external_code_type,
                                                                                 internal_code_type=internal_code_type,
                                                                                 else_clause='{else_clause}'))
                
            if_clause = if_clause.format(else_clause='NULL')  
            
            proc_insert_map['{prefix}procedure_code_type'.format(prefix=prefix)] = if_clause  
        
        procedure_code_modifier_column = self.normalization_rules['M'].get('{prefix}procedure_code_modifier'.format(prefix=prefix))
        if (procedure_code_modifier_column):
            proc_insert_map['{prefix}procedure_code_modifier'.format(prefix=prefix)] = '{procedure_code_modifier}'.format(procedure_code_modifier=procedure_code_modifier_column)
        
        procedure_code_mapping = self.load_properties.get('field_column_mappings').get('{prefix}procedure_code'.format(prefix=prefix))
        proc_code_insert_list = []
        if procedure_code_mapping:
            if isinstance(procedure_code_mapping, dict):
                proc_insert_map.update({'{prefix}{procedure_code_type}_code'.format(prefix= prefix, procedure_code_type=k):'{ext_proc_code_column}'.format(ext_proc_code_column=v) for k,v in procedure_code_mapping.iteritems() if v and v.strip() != ''})
            else:
                proc_insert_map.update({'{prefix}procedure_code'.format(prefix=prefix):'{ext_proc_code_column}'.format(ext_proc_code_column=procedure_code_mapping)})
         
        if len(proc_insert_map) > 0:
            proc_insert_map['{prefix}procedure_code_sha1'.format(prefix=prefix)] = "UNHEX(SHA1(CONCAT_WS(':',%s)))" % (','.join(["COALESCE(%s,'')" % proc_insert_map.get(k) for k in sorted(proc_insert_map.keys())]))

        return proc_insert_map
    
    def __query_procedure_code_types__(self):
        if not self.procedure_code_types:
            self.procedure_code_types = {}
            self.procedure_code_type_values = {}
            fac_procedure_code_types = ModelFactory.get_instance(self.conn, 'procedure_code_types')
            for pct in fac_procedure_code_types.table:
                self.procedure_code_types[pct['name'].lower()] = pct['id']
                self.procedure_code_type_values[pct['id']] = pct['name'].lower()
        return self.procedure_code_types
    def __query_external_procedure_code_types__(self):
        if not self.external_procedure_code_types:
            pc_map = {}
            procedure_code_type_mappings = self.load_properties.get('procedure_code_types')
            if procedure_code_type_mappings:
                for k,v in procedure_code_type_mappings.iteritems():
                    if v: pc_map[str(v).lower()] = self.procedure_code_types.get(k)            
            self.external_procedure_code_types = {self.insurance_company_id:pc_map}

        return self.external_procedure_code_types
    def __query_procedure_code_modifiers__(self):
        if not self.procedure_code_modifiers:
            self.procedure_code_modifiers = {}
            fac_procedure_code_modifiers = ModelFactory.get_instance(self.conn, 'procedure_modifiers')
            for proc_modifier in fac_procedure_code_modifiers.table:
                self.procedure_code_modifiers[proc_modifier['code'].lower()] = proc_modifier['id']
    def __query_procedure_labels__(self):
        if not self.procedures:
            self.procedures = {}
            fac_procedures = ModelFactory.get_instance(self.conn, 'procedure_labels')
            for p in fac_procedures.table:
                if (not self.procedures.get(p['procedure_code_id'])):
                    self.procedures[p['procedure_code_id']] = {}

                modifier = p['procedure_modifier_id'] if p['procedure_modifier_id'] else -1
                if (not self.procedures.get(p['procedure_code_id']).get(modifier)):
                    self.procedures.get(p['procedure_code_id'])[modifier] = p['id']
                else:
                    print 'WARNING: Duplicate Procedure Labels (%s, %s)' % (p['procedure_code_id'], modifier)

        return self.procedures
  
    def __query_procedure_codes__(self):
        if not self.procedure_codes:
            self.procedure_codes = {}
            self.procedure_code_to_type_map = {}
            fac_procedure_codes = ModelFactory.get_instance(self.conn, 'procedure_codes')
            for pc in fac_procedure_codes.table:
                if (not self.procedure_codes.get(pc['procedure_code_type_id'])):
                    self.procedure_codes[pc['procedure_code_type_id']] = {}
                if (not self.procedure_code_to_type_map.get(pc['code'].lower())):
                    self.procedure_code_to_type_map[pc['code'].lower()] = set()

                self.procedure_code_to_type_map[pc['code'].lower()].add(pc['procedure_code_type_id'])

                if (not self.procedure_codes.get(pc['procedure_code_type_id']).get(pc['code'].lower())):
                    self.procedure_codes.get(pc['procedure_code_type_id'])[pc['code'].lower()] = pc['id']
                else:
                    print 'WARNING: Duplicate Procedure Code: (%s,%s)' % (pc['procedure_code_type_id'], pc['code'])
        return self.procedure_codes 
    def __refresh_x_claim_procedures_generic(self, logger):
    
        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        prefix = ''
        x_claims_table = """%s.x_dental_claims_%s""" % (whcfg.scratch_schema,  
                                                   self.imported_claim_file_id)
        
        procedure_insert = self.__refresh_x_claim_procedure_insert_generic(source_table=t_stage_claim_table, table_alias='cic', logger=logger)
        
        procedure_insert_keys = procedure_insert.keys()

        if len(procedure_insert_keys) == 0:
            return

        drop_procedure_labels_icf = """DROP TABLE IF EXISTS {scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id}""".format(prefix = prefix, scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)
        create_procedure_labels_icf = """CREATE TABLE {scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id} 
                                           ({procedure_column_defs},
                                            {prefix}procedure_code_id INT(11),
                                            {prefix}procedure_modifier_id INT(11),
                                            {prefix}procedure_code_type_id INT(11),
                                            {prefix}procedure_label_id INT(11),
                                            INDEX ix_procedure_code_sha1({prefix}procedure_code_sha1))
                                            AS SELECT {procedure_columns}
                                            FROM {x_claims_table}
                                            GROUP BY {prefix}procedure_code_sha1""".format(procedure_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(procedure_insert_keys)]),
                                                                                   procedure_columns=','.join(sorted(procedure_insert_keys)),
                                                                                  scratch_schema=whcfg.scratch_schema,
                                                                                  imported_claim_file_id=self.imported_claim_file_id,prefix = prefix,
                                                                                  x_claims_table=x_claims_table)

        claims_procedure_queries = [{'query': drop_procedure_labels_icf,
                                    'description':'Drop {scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                 imported_claim_file_id=self.imported_claim_file_id,
                                                                                                                                 prefix = prefix),
                                    'warning_filter':'ignore'},
                                   {'query':create_procedure_labels_icf,
                                    'description':'Create {scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                 imported_claim_file_id=self.imported_claim_file_id,
                                                                                                                                 prefix = prefix)
                                   }]
        utils.execute_queries(self.conn, logger, claims_procedure_queries)
        
        t_claim_pl = dbutils.Table(self.conn, '{scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema, 
                                                                                                                          imported_claim_file_id=self.imported_claim_file_id, 
                                                                                                                          prefix = prefix))
        
        t_claim_pl_columns = t_claim_pl.columns()

        is_single_pc_column = '{prefix}procedure_code'.format(prefix= prefix) in t_claim_pl_columns
        exists_pc_type_column = '{prefix}procedure_code_type'.format(prefix= prefix) in t_claim_pl_columns

        num_identified = 0
        total = 0
        qv_update_procedure = []  
        q_update_procedure = '''UPDATE %s.{prefix}claim_procedure_labels_%s
                    SET {prefix}procedure_code_id = %s,
                        {prefix}procedure_code_type_id = %s,
                        {prefix}procedure_modifier_id = %s,
                        {prefix}procedure_label_id = %s
                 WHERE {prefix}procedure_code_sha1 = %s
           '''.format(prefix= prefix) % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s', '%s', '%s')            
                
        for i, claim_pl_row in enumerate(t_claim_pl):
            epct = claim_pl_row.get('{prefix}procedure_code_type'.format(prefix= prefix)).strip().lower() if claim_pl_row.get('{prefix}procedure_code_type'.format(prefix= prefix)) else None 
            procedure_code_type = self.procedure_code_type_values.get(self.external_procedure_code_types[self.insurance_company_id].get(epct)) if exists_pc_type_column else None
            
            procedure_code = claim_pl_row.get('{prefix}procedure_code'.format(prefix= prefix)) if is_single_pc_column else None
                        
            procedure_modifier_code = claim_pl_row.get('{prefix}procedure_code_modifier'.format(prefix=prefix)).lower() if claim_pl_row.get('{prefix}procedure_code_modifier'.format(prefix=prefix)) else None
            procedure_modifier_id = -1
            if procedure_modifier_code:
                procedure_modifier_id = self.procedure_code_modifiers.get(procedure_modifier_code)
                if not procedure_modifier_id:
                    # TODO Log the creation of a new Modifier
                    fac_procedure_code_modifiers = ModelFactory.get_instance(self.conn, 'procedure_modifiers')
                    pm_entry = {'code':procedure_modifier_code}
                    pm_entry = fac_procedure_code_modifiers.create(pm_entry)
                    self.procedure_code_modifiers[procedure_modifier_code.lower()] = pm_entry['id']
                    procedure_modifier_id = pm_entry['id']
            
            procedure_code_type_id = self.procedure_code_types.get(procedure_code_type)
            if not procedure_code_type_id:
                procedure_code_type_id = -1
             
            procedure_code_length = self.procedure_code_type_lengths.get(str(procedure_code_type_id), 0)
            procedure_code = procedure_code.zfill(procedure_code_length).lower() if procedure_code else None
            procedure_code_id = self.procedure_codes.get(procedure_code_type_id).get(procedure_code) if self.procedure_codes.get(procedure_code_type_id) else None
            if not procedure_code_id:
                procedure_code_id = -1
               
            procedure_label_id = -1
            if procedure_code and procedure_code_type:
                total = total + 1
                if procedure_code_type_id > -1:
                    if procedure_code_id > -1:
                        num_identified = num_identified + 1
                        pass
                    else:
                        fac_procedure_codes = ModelFactory.get_instance(self.conn, 'procedure_codes')
                        p_code_entry = {'code':procedure_code,'procedure_code_type_id':procedure_code_type_id}
                        p_code_entry = fac_procedure_codes.create(p_code_entry)
                        procedure_code_id = p_code_entry['id']
                        
                        if (not self.procedure_codes.get(procedure_code_type_id)):
                            self.procedure_codes[procedure_code_type_id] = {}
                        if (not self.procedure_code_to_type_map.get(procedure_code)):
                            self.procedure_code_to_type_map[procedure_code] = set()
                        
                        self.procedure_codes.get(procedure_code_type_id)[procedure_code] = procedure_code_id
                        self.procedure_code_to_type_map[procedure_code].add(procedure_code_type_id)
                else:
                    pass
                
                if procedure_code_id > -1 and procedure_code_type_id > -1:
                    procedure_modifiers = self.procedures.get(procedure_code_id)

                    if (not procedure_modifiers):
                        procedure_modifiers = {}
                        self.procedures[procedure_code_id] = procedure_modifiers
                    
                    if procedure_modifiers.get(procedure_modifier_id):
                        procedure_label_id = procedure_modifiers.get(procedure_modifier_id)
                    else:
                        fac_procedure_labels = ModelFactory.get_instance(self.conn, 'procedure_labels')
                        p_entry = {'procedure_code_id':procedure_code_id,'procedure_code_type_id':procedure_code_type_id,'procedure_modifier_id':procedure_modifier_id}
                        p_entry = fac_procedure_labels.create(p_entry)
                        if (p_entry):
                            procedure_label_id = p_entry.get('id')
                            self.procedures[procedure_code_id][procedure_modifier_id] = p_entry.get('id')
                        else:
                            pprint.pprint('SEVERE ERROR: Creating procedure: %s' % p_entry)
                

            qv_update_procedure.append((procedure_code_id, procedure_code_type_id, procedure_modifier_id, procedure_label_id, claim_pl_row.get('{prefix}procedure_code_sha1'.format(prefix=prefix))))
                
        if qv_update_procedure:
            cur = self.conn.cursor()
            
            cur.executemany(q_update_procedure, qv_update_procedure) 
            x_claims_update_procedure = """UPDATE {x_claims_table} x
                                            JOIN {scratch_schema}.claim_procedure_labels_{imported_claim_file_id} cp USING (procedure_code_sha1)
                                             SET x.procedure_label_id=cp.procedure_label_id,
                                                 x.parse_status=IF(cp.procedure_label_id=-1,0,1),
                                                 x.parse_comment=IF(cp.procedure_label_id=-1,'Procedure Code not specified in claim.',NULL)
                                             WHERE cp.procedure_label_id is not null""".format(x_claims_table=x_claims_table,scratch_schema=whcfg.scratch_schema,
                                                                                  imported_claim_file_id=self.imported_claim_file_id,)
            
            cur.execute(x_claims_update_procedure)
            cur.close()

       

        
            
        
