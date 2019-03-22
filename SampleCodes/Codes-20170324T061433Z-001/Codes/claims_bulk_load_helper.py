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
st = Stats("claims_load_helper")

# TODO: Extend for stats and logging
""" Known Custom Factories """
helpers = {"generic": lambda conn, p_conn, icf, load_properties, dry_run: BaseBulkClaimsLoader(conn, p_conn, icf, load_properties, dry_run)
           }
LIMIT_CLAUSE = """ """

FALLBACK_DRG_TYPES = {'cigna':'CMS_DRG',
             'aetna':'MS_DRG',
             'bcbsma':'AP_DRG',
             'horizon':'AP_DRG',
             'bcbsnc':'AP_DRG',
             'premera':'AP_DRG'
             }

helper_instances = {}

def field_mappings():
    return claims_load_helper.FIELD_MAPPINGS

class _Callable:
    def __init__(self, anycallable):
        self.__call__ = anycallable

class ClaimsBulkLoaderFactory:

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

class BaseBulkClaimsLoader:

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
        if self.stage_claim_table[-4:] == '_bob':
            # hack for bob identification
            # TODO: There may be a need to have a separate method to 
            # identify the claims table suffix if this is extended beyond bob
            self.claims_table_suffix = '_bob'
            self.employer_id = -1
        
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

        self.load_properties = yaml.load(load_properties_text) if load_properties_text else {'field_column_mappings':field_mappings().get(self.insurance_company_name.lower()), 'member_relationships':field_mappings().get(self.insurance_company_name.lower()).get('member_relationships') if field_mappings().get(self.insurance_company_name.lower()).get('member_relationships') else None}
        self.static_entries = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml', 'r')) if self.insurance_company_id else None
        
#        self.normalization_rules = {'M':self.load_properties.get('field_column_mappings'),
#                                    'L':{
#                                        'source':'%s_CLAIM_%s' % (self.claim_file_source_name.upper(), self.imported_claim_file_id),
#                                        'employer_id':self.employer_id
#                                        }
#                                   }

        self.normalization_rules = {'M':{k:claims_util.yaml_formula_insert_simple(self.stage_claim_table, self.stage_claim_table_columns, v, self.stage_claims_table_alias) for k,v in self.load_properties.get('field_column_mappings').iteritems()},
                                    'L':{
                                        'source':'%s_CLAIM_%s' % (self.claim_file_source_name.upper(), self.imported_claim_file_id),
                                        'employer_id':self.employer_id
                                        }
                                   }

        self.__query_procedure_code_types__()
        self.__query_external_procedure_code_types__()
        self.__query_procedure_codes__()
        self.__query_procedure_code_modifiers__()
        self.__query_procedure_labels__()
        

        
#            if self.insurance_company_name.lower() != 'cigna' or self.stage_claim_table != 'cigna_imported_claims_bob':
#                # Bulk loader is only restricted to cigna bob claims for now
#                logutil.log(LOG, logutil.INFO, "Bulk loader only available for Cigna BOB claims. Exiting!" )
#                sys.exit()
                        

        
        self.external_specialties_map = {}
        specialties_entry = self.load_properties.get('specialties')
        self.external_specialty_source = specialties_entry.get('external_specialty_source') if specialties_entry else None
        self.external_specialty_source = self.external_specialty_source.lower() if self.external_specialty_source else None

#        if self.is_source_payer:
#            
#            # Create a single entry in payer_code_map and add a literal mapping in the norm_rules
#            self.payer_code_map = {str(self.insurance_company_id):(self.insurance_company_id, self.claim_file_source_name)}
#            self.normalization_rules['L']['payer_code'] = self.insurance_company_id
#            
#            # Remove any payer_code mapping from 'M' section of normalization rules
#            self.normalization_rules['M'].pop('payer_code', None)

    def __query_insurance_companies__(self):
        if not self.insurance_companies:
            self.insurance_companies = {}
            fac_insurance_companies = ModelFactory.get_instance(self.conn, '%s.insurance_companies' % whcfg.master_schema)
            for ic in fac_insurance_companies.table:
                self.insurance_companies[ic['name'].lower()] = ic['id']
        return self.insurance_companies
    
    def __query_procedure_code_modifiers__(self):
        if not self.procedure_code_modifiers:
            self.procedure_code_modifiers = {}
            fac_procedure_code_modifiers = ModelFactory.get_instance(self.conn, 'procedure_modifiers')
            for proc_modifier in fac_procedure_code_modifiers.table:
                self.procedure_code_modifiers[proc_modifier['code'].lower()] = proc_modifier['id']
                
    def __query_procedure_code_types__(self):
        if not self.procedure_code_types:
            self.procedure_code_types = {}
            self.procedure_code_type_values = {}
            fac_procedure_code_types = ModelFactory.get_instance(self.conn, 'procedure_code_types')
            for pct in fac_procedure_code_types.table:
                self.procedure_code_types[pct['name'].lower()] = pct['id']
                self.procedure_code_type_values[pct['id']] = pct['name'].lower()
        return self.procedure_code_types

    def __query_external_procedure_code_types_old__(self):
        if not self.external_procedure_code_types:
            self.external_procedure_code_types = {1:{},2:{}}
            fac_external_procedure_code_types = ModelFactory.get_instance(self.conn, 'external_procedure_code_types')
            for epct in fac_external_procedure_code_types.table:
                if (epct['procedure_code_type_id']):
                    if (not self.external_procedure_code_types.get(epct['insurance_company_id'])):
                        self.external_procedure_code_types[epct['insurance_company_id']] = {}
                    self.external_procedure_code_types[epct['insurance_company_id']][epct['name'].lower()] = epct['procedure_code_type_id']
        return self.external_procedure_code_types

    def __query_external_procedure_code_types__(self):
        if not self.external_procedure_code_types:
            pc_map = {}
            procedure_code_type_mappings = self.load_properties.get('procedure_code_types')
            if procedure_code_type_mappings:
                for k,v in procedure_code_type_mappings.iteritems():
                    if v: pc_map[str(v).lower()] = self.procedure_code_types.get(k)            
            self.external_procedure_code_types = {self.insurance_company_id:pc_map}

        return self.external_procedure_code_types

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
            self.__refresh_x_claim_locations_generic(LOG)
            self.__refresh_x_claim_procedures_generic(LOG)
            self.__refresh_x_claim_procedures_generic(LOG, False)
            self.__refresh_x_claim_patients_generic(LOG)
            
            self.insert_claims_generic(LOG)
            self.insert_claim_attributes_generic(LOG)
            self.insert_claim_specialties_generic(LOG)
            self.insert_claim_subscriber_identifiers(LOG)

            self.augment_claim_participations(LOG)
            self.claim_provider_exception()
            self.match_claim_providers() 
        else:
            sys.exit(1)
            
    def process_claims_generic_refresh_participation(self, st):
        self.__init_x_claims_metadata_generic(LOG)
        self.__refresh_x_claims_generic(LOG)
        self.__refresh_x_claim_locations_generic(LOG)
        
        self.update_claims_provider_location_generic(LOG)
        self.augment_claim_participations_provider_location(LOG)
        #self.claim_provider_exception()     
        
#        self.__refresh_x_claims_generic(LOG)

    def update_claims_provider_location_generic(self, st):   
        
        cur = self.conn.cursor()
        create_icf_ic_index = """CREATE INDEX ix_icf_ic ON {scratch_schema}.x_claims{claims_table_suffix}_{imported_claim_file_id}\
                                            (imported_claim_file_id, imported_claim_id)""".format(scratch_schema=whcfg.scratch_schema,
                                                                                        claims_table_suffix=self.claims_table_suffix,
                                                                                        imported_claim_file_id=self.imported_claim_file_id)
        cur.execute(create_icf_ic_index)
        
        update_stmt = """UPDATE {claims_master_schema}.claims{claims_table_suffix} c JOIN
                        {scratch_schema}.x_claims_{imported_claim_file_id} xc ON
                        c.imported_claim_file_id = xc.imported_claim_file_id and c.imported_claim_id = xc.imported_claim_id
                        set c.provider_location_id = xc.provider_location_id
                        """.format(claims_master_schema=whcfg.claims_master_schema,
                                                                                             claims_table_suffix=self.claims_table_suffix,
                                                                                             scratch_schema=whcfg.scratch_schema,
                                                                                             imported_claim_file_id=self.imported_claim_file_id)
        
        cur.execute(update_stmt)
        cur.close()

    def __validation_check(self, LOG):
        q_validation_check = """SELECT count(1) as num FROM claims WHERE imported_claim_file_id=%s""" % self.imported_claim_file_id 
        res = dbutils.Query(self.conn, q_validation_check)
        if res and res.next().get('num') > 0:
            logutil.log(LOG, logutil.CRITICAL, "Claims for imported_claim_file_id:%s have already been normalized. If the intent is to re-normalize, please re-run after performing a cleanup.")
            return False
        
        return True
    
    def insert_claims_generic(self, st):   
        t_claims = dbutils.Table(self.conn, '%s.claims%s' % (whcfg.claims_master_schema, self.claims_table_suffix))     
        claim_cols = t_claims.columns()
        
        insert_cols = ','.join([cc for cc in claim_cols if cc <> 'id'])
        insert_stmt = """INSERT INTO {claims_master_schema}.claims{claims_table_suffix} ({insert_cols}) 
                         SELECT {insert_cols}
                           FROM {scratch_schema}.x_claims_{imported_claim_file_id}""".format(claims_master_schema=whcfg.claims_master_schema,
                                                                                             claims_table_suffix=self.claims_table_suffix,
                                                                                             scratch_schema=whcfg.scratch_schema,
                                                                                             insert_cols=insert_cols,
                                                                                             imported_claim_file_id=self.imported_claim_file_id)
        cur = self.conn.cursor()
        cur.execute(insert_stmt)
        cur.close()

    def insert_claim_attributes_generic(self, logger):
        
        claim_attributes = self.load_properties.get('claim_attributes', {}) 

        q_insert_claim_attr_list = []
        
        q_insert_claim_attr = """INSERT INTO {claims_master_schema}.claim_attributes{claims_table_suffix} (claim_id, name, value, created_at, updated_at)
                                SELECT c.id, '{attribute_name}', {attribute_column_name}, NOW(), NOW()
                                  FROM {claims_master_schema}.{icf_table} aic
                                  JOIN {claims_master_schema}.claims{claims_table_suffix} c 
                                    ON aic.imported_claim_file_id=c.imported_claim_file_id 
                                   AND aic.id=c.imported_claim_id
                                 WHERE {attribute_column_name} <> '' AND {attribute_column_name} is not null
                                 AND   aic.imported_claim_file_id = {icf_id}
                                """
        if isinstance(claim_attributes, list):
            for attribute_name in  claim_attributes:
                q_insert = q_insert_claim_attr.format(claims_master_schema=whcfg.claims_master_schema,
                                                       claims_table_suffix=self.claims_table_suffix,
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
                                                       claims_table_suffix=self.claims_table_suffix,
                                                       attribute_name=mapped_attribute_name,
                                                       icf_table=self.stage_claim_table,                                                       
                                                       attribute_column_name=claims_util.yaml_formula_insert(dbutils.Table(self.conn, self.stage_claim_table), attribute_name, 'aic'),
                                                       icf_id=self.imported_claim_file_id)
                q_insert_claim_attr_list.append({'query':q_insert,
                                                 'description':'Inserting claims_attribute for %s.' % mapped_attribute_name,
                                                 'warning_filter':'ignore'})
        
        utils.execute_queries(self.conn, logger, q_insert_claim_attr_list)

    def insert_claim_specialties_generic(self, logger):
        
        q_insert_claim_spec_list = []
        
        q_insert_claim_spec = """INSERT INTO {claims_master_schema}.claim_specialties{claims_table_suffix} (claim_id, specialty_id)
                                SELECT c.id, sm.specialty_id
                                  FROM {claims_master_schema}.claims{claims_table_suffix} c
                                  JOIN {scratch_schema}.x_claims_{imported_claim_file_id} x ON c.imported_claim_file_id=x.imported_claim_file_id AND c.imported_claim_id=x.imported_claim_id
                                  JOIN {scratch_schema}.specialty_mapping_{imported_claim_file_id} sm
                                    ON x.provider_specialty_code=sm.external_code
                                """.format(claims_master_schema=whcfg.claims_master_schema, 
                                           claims_table_suffix=self.claims_table_suffix,
                                           scratch_schema=whcfg.scratch_schema,
                                           imported_claim_file_id=self.imported_claim_file_id)
                                
        q_insert_claim_spec_list.append({'query':q_insert_claim_spec,
                                         'description':'Inserting claim_specialties.'})

        utils.execute_queries(self.conn, logger, q_insert_claim_spec_list)

    def insert_claim_subscriber_identifiers(self, logger):
        
        q_insert_claim_subscriber_list = []
        
        q_insert_claim_subscribers = """INSERT INTO {claims_master_schema}.claim_subscriber_identifiers (claim_id, subscriber_identifier)
                                SELECT c.id, if(x.employee_ssn is null,'',x.employee_ssn)
                                  FROM {claims_master_schema}.claims{claims_table_suffix} c
                                  JOIN {scratch_schema}.x_claims_{imported_claim_file_id} x ON c.imported_claim_file_id=x.imported_claim_file_id AND c.imported_claim_id=x.imported_claim_id
                                """.format(claims_master_schema=whcfg.claims_master_schema, 
                                           claims_table_suffix=self.claims_table_suffix,
                                           scratch_schema=whcfg.scratch_schema,
                                           imported_claim_file_id=self.imported_claim_file_id)
                                
        q_insert_claim_subscriber_list.append({'query':q_insert_claim_subscribers,
                                         'description':'Inserting claim_subscribers.'})

        utils.execute_queries(self.conn, logger, q_insert_claim_subscriber_list)

    def process_claims_cigna_bob(self, st):
        
#        self.clear_claims(LOG)

        self.insert_claims(LOG)
        self.__refresh_x_claims(LOG)
        self.__refresh_x_claim_locations(LOG)
        self.__refresh_x_claim_providers(LOG)
        self.__refresh_x_claim_procedures(LOG)

#        self.__refresh_x_claim_provider_networks(LOG)
#        self.__refresh_x_claim_service_places(LOG)

        self.update_claims(LOG)
        
        self.insert_claim_attributes(LOG)
        self.insert_claim_specialties(LOG) 
                
    def process_claims(self, st):
        
        if self.stage_claim_table.lower() == 'cigna_imported_claims_bob':
            self.process_claims_cigna_bob(st)
        else:
            self.process_claims_generic(st)
        
        
    def update_claims(self, logger):

#CREATE TABLE pv_scratch.x_claims_bob_provider_networks (network_id INT(11), INDEX ix_pn(provider_network)) AS SELECT DISTINCT provider_network from x_claims_bob;
#CREATE TABLE pv_scratch.x_claims_bob_service_places (service_place_id INT(11), INDEX ix_sp(place_of_service)) AS SELECT DISTINCT place_of_service FROM x_claims_bob;
#UPDATE pv_scratch.x_claims_bob_provider_networks xn, 
#       pv_scratch.insurance_networks inet 
#   SET xn.network_id=inet.id
# WHERE xn.provider_network=inet.external_network_id
#   AND inet.insurance_company_id=2
#   AND inet.type is null;
#   
#UPDATE pv_scratch.x_claims_bob_provider_networks SET network_id=-1 where network_id is null;
#
#UPDATE pv_scratch.x_claims_bob_service_places xsp, 
#       pv_scratch.service_places sp
#   SET xsp.service_place_id=sp.id
# WHERE xsp.place_of_service=sp.code;

#CREATE TABLE pv_scratch.x_claims_bob_service_place_ids (id INT(11) PRIMARY KEY) AS
#SELECT xc.id, xp.service_place_id
#FROM pv_scratch.x_claims_bob xc,
#pv_scratch.x_claims_bob_service_places xp
#WHERE xc.place_of_service=xp.place_of_service;
#
#CREATE TABLE pv_scratch.x_claims_bob_provider_network_ids (id INT(11) PRIMARY KEY) AS
#SELECT xc.id, xp.network_id
#FROM pv_scratch.x_claims_bob xc,
#pv_scratch.x_claims_bob_provider_networks xp
#WHERE xc.provider_network=xp.provider_network;
#
#CREATE TABLE x_claims_bob_all_ids
#AS 
#SELECT xp.id, xp.provider_id, xl.provider_location_id, xpl.procedure_label_id, xpn.network_id, xsp.service_place_id
#FROM
#pv_scratch.x_claims_bob_provider_ids xp
#JOIN
#pv_scratch.x_claims_bob_location_ids xl ON xp.id=xl.id
#JOIN
#pv_scratch.x_claims_bob_procedure_label_ids xpl ON xl.id=xpl.id
#JOIN
#pv_scratch.x_claims_bob_provider_network_ids xpn ON xpl.id=xpn.id
#JOIN
#pv_scratch.x_claims_bob_service_place_ids xsp ON xpn.id=xsp.id;
        
        # Update provider_id
        u_provider_id = """UPDATE %s.x_claims%s x, 
                                   %s.x_claims%s_providers sp
                               SET x.provider_id=sp.provider_id
                             WHERE x.cigna_pin=sp.cigna_pin""" % (whcfg.scratch_schema, self.claims_table_suffix, 
                                                                  whcfg.scratch_schema, self.claims_table_suffix)

        # Update provider_location_id
        u_location_id = """UPDATE %s.x_claims%s x, 
                                   %s.x_claims%s_locations sp
                               SET x.provider_location_id=sp.location_id
                             WHERE x.raw_address=sp.raw_address""" % (whcfg.scratch_schema, self.claims_table_suffix, 
                                                                      whcfg.scratch_schema, self.claims_table_suffix)

        # Update procedure_label_id
        u_proc_lbl_id = """UPDATE %s.x_claims%s x, 
                                   %s.x_claims%s_procedures sp
                               SET x.procedure_label_id=sp.procedure_label_id
                             WHERE x.procedure_label=sp.procedure_label""" % (whcfg.scratch_schema, self.claims_table_suffix, 
                                                                              whcfg.scratch_schema, self.claims_table_suffix)
        # Update service_place_id
        
        u_svc_place_id = """UPDATE %s.x_claims%s x, 
                                   %s.service_places sp
                               SET x.service_place_id=sp.id
                             WHERE x.place_of_service=sp.code""" % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.claims_master_schema)

        # Update insurance_network_id
        
        u_network_id = """UPDATE %s.x_claims%s x, 
                                   %s.insurance_networks sp
                               SET x.insurance_network_id=sp.id
                             WHERE x.provider_network=sp.external_network_id
                               AND sp.insurance_company_id=%s
                               AND sp.type is null""" % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.master_schema, self.insurance_company_id)

        ix_icfid_id = """CREATE INDEX ix_icfid_id ON %s.x_claims%s(imported_claim_file_id, id)""" % (whcfg.scratch_schema, self.claims_table_suffix)
        
        i_claims = """INSERT INTO %s.claims%s SELECT * FROM %s.claims%s_%s""" % (whcfg.claims_master_schema, self.claims_table_suffix,
                                                                                 whcfg.scratch_schema, self.claims_table_suffix, 
                                                                                 self.imported_claim_file_id)
        u_claims = """UPDATE %s.claims%s c,
                             %s.x_claims%s x
                        SET c.provider_id=x.provider_id,
                            c.provider_location_id=x.provider_location_id,
                            c.procedure_label_id=x.procedure_label_id,
                            c.service_place_id=x.service_place_id,
                            c.insurance_network_id=x.insurance_network_id
                        WHERE c.id=x.id""" % (whcfg.claims_master_schema, self.claims_table_suffix,
                                              whcfg.scratch_schema, self.claims_table_suffix)
                        
        x_claims_queries = [{'query':u_provider_id,
                             'description':'Update provider_id in x_claims%s.' % self.claims_table_suffix},
                            {'query':u_location_id,
                             'description':'Update location_id in x_claims%s.' % self.claims_table_suffix},
                            {'query':u_proc_lbl_id,
                             'description':'Update procedure_label_id in x_claims%s.' % self.claims_table_suffix},
                            {'query':u_svc_place_id,
                             'description':'Update service_place_id in x_claims%s.' % self.claims_table_suffix},
                            {'query':u_network_id,
                             'description':'Update insurance_network_id in x_claims%s.' % self.claims_table_suffix},
                            {'query':ix_icfid_id,
                             'description':'Create index on x_claims%s.' % self.claims_table_suffix},
                            {'query':i_claims,
                             'description':'Insert claims%s.' % self.claims_table_suffix},
                            {'query':u_claims,
                             'description':'Update claims%s.' % self.claims_table_suffix}]
        
        utils.execute_queries(self.conn, logger, x_claims_queries)

    def insert_claim_attributes(self, logger):
        insert_rmk = """INSERT INTO %s.claim_attributes%s(claim_id, name, value, created_at, updated_at)
                        SELECT x.id, 'RMK_CD', x.RMK_CD, NOW(), NOW()
                        FROM %s.x_claims%s x
                        WHERE x.RMK_CD is not null AND TRIM(x.RMK_CD) <> ''"""  % (whcfg.claims_master_schema, self.claims_table_suffix,
                                                  whcfg.scratch_schema, self.claims_table_suffix)
        insert_rnc = """INSERT INTO %s.claim_attributes%s(claim_id, name, value, created_at, updated_at)
                        SELECT x.id, 'RSN_NOT_COVRD_CD', x.RSN_NOT_COVRD_CD, NOW(), NOW()
                        FROM %s.x_claims%s x
                        WHERE x.RSN_NOT_COVRD_CD is not null AND TRIM(x.RSN_NOT_COVRD_CD) <> ''"""  % (whcfg.claims_master_schema, self.claims_table_suffix,
                                                  whcfg.scratch_schema, self.claims_table_suffix)

        x_claims_queries = [{'query':insert_rmk,
                             'description':'Insert claim_attributes%s for RMK_CD.' % self.claims_table_suffix},
                            {'query':insert_rnc,
                             'description':'Insert claim_attributes%s for RSN_NOT_COVRD_CD.' % self.claims_table_suffix}]   
        
        utils.execute_queries(self.conn, logger, x_claims_queries)     
        return
    
    def insert_claim_specialties(self, logger):
        # TODO: Definitely needs to be made generic for other payers
        insert_spec_pr = """INSERT INTO %s.claim_specialties%s (claim_id, specialty_id)
                                SELECT DISTINCT c.id, esm.specialty_id
                                FROM %s.x_claims%s c,
                                     %s.external_specialties_map esm
                               WHERE c.provider_type = 'PR' 
                                 AND (CONCAT('Physician:',c.provider_specialty_code)=esm.external_specialty_code OR CONCAT('CBH Physician:',c.provider_specialty_code)=esm.external_specialty_code)
                                 AND esm.source='%s'""" % (whcfg.claims_master_schema, self.claims_table_suffix,
                                                           whcfg.scratch_schema, self.claims_table_suffix,
                                                           whcfg.master_schema, self.insurance_company_name)

        insert_spec_fac = """INSERT IGNORE INTO %s.claim_specialties%s (claim_id, specialty_id)
                                SELECT DISTINCT c.id, esm.specialty_id
                                FROM %s.x_claims%s c,
                                     %s.external_specialties_map esm
                               WHERE c.provider_type <> 'PR' 
                                 AND (CONCAT('Hospital:',c.provider_specialty_code)=esm.external_specialty_code OR CONCAT('Other Services:',c.provider_specialty_code)=esm.external_specialty_code)
                                 AND esm.source='%s'""" % (whcfg.claims_master_schema, self.claims_table_suffix,
                                                           whcfg.scratch_schema, self.claims_table_suffix,
                                                           whcfg.master_schema, self.insurance_company_name)
        
        x_claims_queries = [{'query':insert_spec_pr,
                             'description':'Insert Physician claim_specialties%s.' % self.claims_table_suffix},
                            {'query':insert_spec_fac,
                             'description':'Insert Facility claim_specialties%s.' % self.claims_table_suffix}]          
        
        utils.execute_queries(self.conn, logger, x_claims_queries)
        return
    
    def insert_claims(self, logger):
        
#        Inserting directly into claims_bob requires a bunch of updates at the very end.
#        This is significalntly slow for large tables, as is claims_bob. 
#        
#        We will therefore do the following:
#        1. Create a table claims_bob_<icf_id> that looks like claims_bob
#        2. Acquire a lock on claims_bob
#        3. Set the auto increment id on claims_bob_<icf_id> to what claims_bob has
#        4. Insert into claims_bob_<icf_id>
#        5. Set the auto increment id on claims_bob to what claims_bob_<icf_id> has
#        6. Release lock on claims_bob 
        claims_insert_queries = []
        d_c_icf = """DROP TABLE IF EXISTS %s.claims%s_%s""" % (whcfg.scratch_schema, 
                                                                self.claims_table_suffix, 
                                                                self.imported_claim_file_id)
        claims_insert_queries.append({'query':d_c_icf,
                                      'description':'Drop table if exists %s.claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                        self.claims_table_suffix, 
                                                                                                        self.imported_claim_file_id),
                                      'warning_filter':'ignore'})
                                      
        c_c_icf = """CREATE TABLE %s.claims%s_%s LIKE %s.claims%s""" % (whcfg.scratch_schema, 
                                                                        self.claims_table_suffix, 
                                                                        self.imported_claim_file_id,
                                                                        whcfg.claims_master_schema, 
                                                                        self.claims_table_suffix)
        claims_insert_queries.append({'query':c_c_icf,
                                      'description':'Creating table %s.claims%s_%s like %s.claims%s' % (whcfg.scratch_schema, 
                                                                                                     self.claims_table_suffix, 
                                                                                                     self.imported_claim_file_id,
                                                                                                     whcfg.claims_master_schema, 
                                                                                                     self.claims_table_suffix)})
        
        l_c = """LOCK TABLES %s.claims%s WRITE, %s.claims%s_%s WRITE, %s.%s cic READ, wellpoint_imported_claim_members READ, bcbsal_imported_claim_providers READ, wellpoint_xref_providers READ""" % (whcfg.claims_master_schema, 
                                                                           self.claims_table_suffix,
                                                                           whcfg.scratch_schema, 
                                                                           self.claims_table_suffix, 
                                                                           self.imported_claim_file_id,
                                                                           whcfg.claims_master_schema,
                                                                           self.stage_claim_table)
        claims_insert_queries.append({'query':l_c,
                                  'description':'Acquire lock on claims%s' % self.claims_table_suffix}) 
        
        ac_c = """SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='claims%s'""" % (whcfg.claims_master_schema, self.claims_table_suffix)
        ac_c_r = Query(self.conn, ac_c)
        ac_claims = ac_c_r.next()['AUTO_INCREMENT']
        

        
        a_c_icf = """ALTER TABLE  %s.claims%s_%s AUTO_INCREMENT = %d""" % (whcfg.scratch_schema, 
                                                                           self.claims_table_suffix, 
                                                                           self.imported_claim_file_id,
                                                                           ac_claims)
        
        claims_insert_queries.append({'query':a_c_icf,
                                      'description':'Setting AUTO_INCREMENT on table %s.claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                         self.claims_table_suffix, 
                                                                                                         self.imported_claim_file_id)})
        
        # TODO: Update to make this more generic to support missing fields or additional fields e.g. diagnosis_code_4
        field_mappings = self.normalization_rules.get('M')
        drg_type_mapping = None
        drg_type_mapping = "'" + FALLBACK_DRG_TYPES.get(self.insurance_company_name.lower(), 'UNKNOWN') + "'"
        if self.load_properties.get('field_column_mappings').get('drg_type') and not isinstance(self.load_properties.get('field_column_mappings').get('drg_type'), dict) \
        and re.match(r"^'.*'$",self.load_properties.get('field_column_mappings').get('drg_type')):
            drg_type_mapping = self.load_properties.get('field_column_mappings').get('drg_type')
        elif field_mappings.get('drg_type'):
            drg_type_mapping = field_mappings.get('drg_type')
            
        insert_map = {'imported_claim_id':'id',
                      'imported_claim_file_id':str(self.imported_claim_file_id),
                      'patient_id':'-1',
                      'user_id':'-1', 
                      'insurance_company_id':str(self.insurance_company_id), 
                      'employer_id':str(self.employer_id),
                      'imported_at':'NOW()',
                      'updated_at':'NOW()',
                      'parse_status':'1',
                      'access_privileges':'1',
                      'member_id':'SHA1(%s)' % field_mappings.get('member_id'),
                      'out_of_network':"IF(%s = 'i' or TRIM(%s) = '' or %s is null, 0, 1)" % (field_mappings.get('out_of_network_indicator'),
                                                                                              field_mappings.get('out_of_network_indicator'),
                                                                                              field_mappings.get('out_of_network_indicator')),
                      'units_of_service':'IF(%s=0,1,COALESCE(%s,1))' % (field_mappings.get('units_of_service'),field_mappings.get('units_of_service')),
                      'service_begin_date':"IF(%s < '1970-01-01', NULL, %s)" % (field_mappings.get('service_begin_date'),
                                                                                field_mappings.get('service_begin_date')),
                      'service_end_date':"IF(%s < '1970-01-01',IF(%s < '1970-01-01', NULL, %s),%s)" % (field_mappings.get('service_end_date'),
                                                                                                       field_mappings.get('service_begin_date'),
                                                                                                       field_mappings.get('service_begin_date'),
                                                                                                       field_mappings.get('service_end_date')),
                      'payment_date':field_mappings.get('payment_date'),
                      'charged_amount':'COALESCE(%s,0)' % field_mappings.get('charged_amount'),
                      'allowed_amount':'COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0)' % (field_mappings.get('paid_amount'),
                                                                                                                               field_mappings.get('copay_amount'),
                                                                                                                               field_mappings.get('cob_amount'),
                                                                                                                               field_mappings.get('coinsurance_amount'),
                                                                                                                               field_mappings.get('deductible_amount')),
                      'savings_amount':'COALESCE(%s,0)' % field_mappings.get('savings_amount'),
                      'cob_amount':'COALESCE(%s,0)' % field_mappings.get('cob_amount'),
                      'coinsurance_amount':'COALESCE(%s,0)' % field_mappings.get('coinsurance_amount'),
                      'deductible_amount':'COALESCE(%s,0)' % field_mappings.get('deductible_amount'),
                      'paid_amount':'COALESCE(%s,0)' % field_mappings.get('paid_amount'),
                      'copay_amount':'COALESCE(%s,0)' % field_mappings.get('copay_amount'),
                      'not_covered_amount':'COALESCE(%s,0)' % field_mappings.get('not_covered_amount'),
                      'diagnosis_code_1':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1')),
                      'diagnosis_code_2':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2')),
                      'diagnosis_code_3':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3')),
                      'drg':field_mappings.get('diagnosis_related_group'),
                      'drg_type':"IF(%s is not null, %s, NULL)" % (field_mappings.get('diagnosis_related_group'),\
                                                                     drg_type_mapping),
                      'source_claim_number':field_mappings.get('source_claim_id'),
                      'source_claim_line_number':field_mappings.get('source_claim_line_number'),
                      'provider_name':'%s.title_case(%s)' % (whcfg.master_schema, field_mappings.get('provider_name')),
                      'procedure_label_id':'-1',
                      'provider_location_id':'-1',
                      'insurance_network_id':'-1',
                      'inpatient':'0',
                      'hra_amount':'0',
                      'service_place_id':'-1',
                      'service_type_id':'-1',
                      'parse_comment':"''"
                      }
        
        insert_cols = insert_map.keys()
        insert_vals = [insert_map.get(k) for k in insert_cols]
        
        q_insert_claims = """INSERT INTO %s.claims%s_%s (%s)
                                              SELECT %s
                                               FROM %s.%s cic
                                              WHERE imported_claim_file_id = %s
                                              AND duplicate_of_claim_id is null
                                              %s""" % (whcfg.scratch_schema,
                                                       self.claims_table_suffix, 
                                                       self.imported_claim_file_id,
                                                       ',\n'.join(insert_cols),
                                                       ',\n'.join(insert_vals),
                                                       whcfg.claims_master_schema,
                                                       self.stage_claim_table,
                                                       self.imported_claim_file_id,
                                                       LIMIT_CLAUSE)

        claims_insert_queries.append({'query':q_insert_claims,
                                      'description':'Insert new claims records into %s.claims%s_%s' % (whcfg.scratch_schema,
                                                                                                       self.claims_table_suffix,
                                                                                                       self.imported_claim_file_id)})        
        
        utils.execute_queries(self.conn, logger, claims_insert_queries)
        
        claims_insert_queries = []
        ac_c_icf = """SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='claims%s_%s'""" % (whcfg.scratch_schema, self.claims_table_suffix, self.imported_claim_file_id)
        ac_c_icf_r = Query(self.conn, ac_c_icf)
        ac_claims_icf = ac_c_icf_r.next()['AUTO_INCREMENT']
                        
        a_c_ac = """ALTER TABLE %s.claims%s AUTO_INCREMENT = %d""" % (whcfg.claims_master_schema, self.claims_table_suffix, ac_claims_icf)
        claims_insert_queries.append({'query':a_c_ac,
                                      'description':'Setting AUTO_INCREMENT on table %s.claims%s' % (whcfg.claims_master_schema, 
                                                                                                     self.claims_table_suffix)})
                    
        ul_c = """UNLOCK TABLES"""
        claims_insert_queries.append({'query':ul_c,
                                      'description':'Release lock on table %s.claims%s' % (whcfg.claims_master_schema, 
                                                                                           self.claims_table_suffix)})
        utils.execute_queries(self.conn, logger, claims_insert_queries)

    def __init_x_claims_metadata_generic(self, logger):
        
        #Type of Service
        service_types = self.load_properties.get('type_of_service')
        claims_metadata_queries = []
        
        ts_drop_table = """DROP TABLE IF EXISTS {scratch_schema}.service_type_mapping_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                      imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ts_drop_table,
                                      'description':'Dropping table {scratch_schema}.service_type_mapping_{imported_claim_file_id} if it exists'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id),
                                      'warning_filter':'ignore'})
                
        ts_create_table = """CREATE TABLE {scratch_schema}.service_type_mapping_{imported_claim_file_id} 
                            (external_code VARCHAR(20), 
                             service_type_id INT(11) DEFAULT -1,
                             INDEX ix_ec(external_code))""".format(scratch_schema=whcfg.scratch_schema,
                                                                           imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ts_create_table,
                                      'description':'Creating table {scratch_schema}.service_type_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})
        
        ts_insert_table = """INSERT INTO {scratch_schema}.service_type_mapping_{imported_claim_file_id} 
                            (external_code, service_type_id) 
                          VALUES {service_type_mappings}""".format(scratch_schema=whcfg.scratch_schema,
                                                                   imported_claim_file_id=self.imported_claim_file_id,
                                                                   service_type_mappings=','.join(["('%s',%s)" % (v,k) for k,v in service_types.iteritems()])) if service_types \
                     else """INSERT INTO {scratch_schema}.service_type_mapping_{imported_claim_file_id} 
                            (external_code, service_type_id)
                            SELECT code, id FROM {claims_master_schema}.service_types""".format(scratch_schema=whcfg.scratch_schema,
                                                                   imported_claim_file_id=self.imported_claim_file_id,
                                                                   claims_master_schema=whcfg.claims_master_schema)
        
        claims_metadata_queries.append({'query':ts_insert_table,
                                      'description':'Inserting data into table {scratch_schema}.service_type_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})
        
        ts_drop_function = """DROP FUNCTION IF EXISTS {scratch_schema}.resolve_service_type_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                            imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ts_drop_function,
                                        'description':'Dropping function {scratch_schema}.resolve_service_type_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                imported_claim_file_id=self.imported_claim_file_id),
                                      'warning_filter':'ignore'})        
        ts_create_function = """CREATE FUNCTION {scratch_schema}.resolve_service_type_{imported_claim_file_id} (est CHAR(255))
RETURNS INT(11) DETERMINISTIC
BEGIN
DECLARE stid INT;
DECLARE dummystid INT;
SELECT service_type_id INTO stid
  FROM {scratch_schema}.service_type_mapping_{imported_claim_file_id}
  WHERE external_code=est;
SELECT service_type_id INTO dummystid FROM {scratch_schema}.service_type_mapping_{imported_claim_file_id} LIMIT 1; 
RETURN COALESCE(stid, -1);
END
""".format(scratch_schema=whcfg.scratch_schema,
           imported_claim_file_id=self.imported_claim_file_id)
        
        claims_metadata_queries.append({'query':ts_create_function,
                                        'description':'Creating function {scratch_schema}.resolve_service_type_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                imported_claim_file_id=self.imported_claim_file_id)})
           
        # Place of Service
        service_places = self.load_properties.get('place_of_service')
        
        ps_drop_table = """DROP TABLE IF EXISTS {scratch_schema}.service_place_mapping_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                      imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ps_drop_table,
                                      'description':'Dropping table {scratch_schema}.service_place_mapping_{imported_claim_file_id} if it exists'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id),
                                      'warning_filter':'ignore'})
                
        ps_create_table = """CREATE TABLE {scratch_schema}.service_place_mapping_{imported_claim_file_id} 
                            (external_code VARCHAR(20), 
                             service_place_id INT(11) DEFAULT -1,
                             INDEX ix_ec(external_code))""".format(scratch_schema=whcfg.scratch_schema,
                                                                           imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ps_create_table,
                                      'description':'Creating table {scratch_schema}.service_place_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})
        
        ps_insert_table = """INSERT INTO {scratch_schema}.service_place_mapping_{imported_claim_file_id} 
                            (external_code, service_place_id) 
                          VALUES {service_place_mappings}""".format(scratch_schema=whcfg.scratch_schema,
                                                                   imported_claim_file_id=self.imported_claim_file_id,
                                                                   service_place_mappings=','.join(["('%s',%s)" % (v,k) for k,v in service_places.iteritems()])) if service_places \
                     else """INSERT INTO {scratch_schema}.service_place_mapping_{imported_claim_file_id} 
                            (external_code, service_place_id)
                            SELECT code, id FROM {claims_master_schema}.service_places""".format(scratch_schema=whcfg.scratch_schema,
                                                                   imported_claim_file_id=self.imported_claim_file_id,
                                                                   claims_master_schema=whcfg.claims_master_schema)
        
        claims_metadata_queries.append({'query':ps_insert_table,
                                      'description':'Inserting data into table {scratch_schema}.service_place_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})
        
        ps_drop_function = """DROP FUNCTION IF EXISTS {scratch_schema}.resolve_service_place_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                            imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ps_drop_function,
                                        'description':'Dropping function {scratch_schema}.resolve_service_place_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                imported_claim_file_id=self.imported_claim_file_id),
                                      'warning_filter':'ignore'})        
        ps_create_function = """CREATE FUNCTION {scratch_schema}.resolve_service_place_{imported_claim_file_id} (esp CHAR(255))
RETURNS INT(11) DETERMINISTIC
BEGIN
DECLARE spid INT;
DECLARE dummyspid INT;
SELECT service_place_id INTO spid
  FROM {scratch_schema}.service_place_mapping_{imported_claim_file_id}
  WHERE external_code=esp;
SELECT service_place_id INTO dummyspid FROM {scratch_schema}.service_place_mapping_{imported_claim_file_id} LIMIT 1; 
RETURN COALESCE(spid, -1);
END
""".format(scratch_schema=whcfg.scratch_schema,
           imported_claim_file_id=self.imported_claim_file_id)
         
        claims_metadata_queries.append({'query':ps_create_function,
                                        'description':'Creating function {scratch_schema}.resolve_service_place_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                imported_claim_file_id=self.imported_claim_file_id)})









        # Specialties
       
        tab_external_specialties_map = Table(self.prod_conn, 'external_specialties_map')
        tab_external_specialties_map.search("source='%s'" % self.external_specialty_source)

        for ext_spec_entry in tab_external_specialties_map:
            if not self.external_specialties_map.get(ext_spec_entry['external_specialty_code']):
                self.external_specialties_map[str(ext_spec_entry['external_specialty_code']).lower()] = set([ext_spec_entry['specialty_id']])
            else:
                self.external_specialties_map[str(ext_spec_entry['external_specialty_code']).lower()].update(set([ext_spec_entry['specialty_id']]))

        # Superimpose manual specialty mappings from load properties file
        load_properties_specialty_mappings = self.load_properties.get('specialties',{}).get('specialty_mappings', {})
        if not load_properties_specialty_mappings:
            load_properties_specialty_mappings = {}
        
        for specialty_code_int, mapped_specialty_ids in load_properties_specialty_mappings.iteritems():
            if not mapped_specialty_ids:
                continue
            specialty_code = str(specialty_code_int)
            if not self.external_specialties_map.get(specialty_code.lower()):
                # Specialty Mappings may be specified either as a single value or as a list of values. 
                # Cast to list so that it works in either case 
                self.external_specialties_map[specialty_code.lower()] = set(mapped_specialty_ids) if isinstance(mapped_specialty_ids, list) else set([mapped_specialty_ids])
            else:
                self.external_specialties_map[specialty_code.lower()].update(set(mapped_specialty_ids)) if isinstance(mapped_specialty_ids, list) else self.external_specialties_map[specialty_code].update(set([mapped_specialty_ids]))
    
        ps_drop_table = """DROP TABLE IF EXISTS {scratch_schema}.specialty_mapping_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                      imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ps_drop_table,
                                      'description':'Dropping table {scratch_schema}.specialty_mapping_{imported_claim_file_id} if it exists'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id),
                                      'warning_filter':'ignore'})
                
        ps_create_table = """CREATE TABLE {scratch_schema}.specialty_mapping_{imported_claim_file_id} 
                            (external_code VARCHAR(255), 
                             specialty_id INT(11) DEFAULT -1,
                             INDEX ix_ec(external_code))""".format(scratch_schema=whcfg.scratch_schema,
                                                                           imported_claim_file_id=self.imported_claim_file_id)
        claims_metadata_queries.append({'query':ps_create_table,
                                      'description':'Creating table {scratch_schema}.specialty_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})
        
        ps_insert_list = []
        for ext_code, specialties in self.external_specialties_map.iteritems():
            for specialty_id in specialties:
                ps_insert_list.append((ext_code, specialty_id))

        ps_insert_table = """INSERT INTO {scratch_schema}.specialty_mapping_{imported_claim_file_id} 
                            (external_code, specialty_id) 
                          VALUES {specialty_mappings}""".format(scratch_schema=whcfg.scratch_schema,
                                                                   imported_claim_file_id=self.imported_claim_file_id,
                                                                   specialty_mappings=','.join(["(%s,%s)" % ('%s',s[1]) for s in ps_insert_list]))
        bind_values = [s[0] for s in ps_insert_list]
        claims_metadata_queries.append({'query':ps_insert_table,
                                        'bind_values':bind_values,
                                      'description':'Inserting data into table {scratch_schema}.specialty_mapping_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                           imported_claim_file_id=self.imported_claim_file_id)})


        utils.execute_queries(self.conn, logger, claims_metadata_queries)
        
            
                   
    def __refresh_x_claims_generic(self, logger):
        
#        Inserting directly into claims_bob requires a bunch of updates at the very end.
#        This is significalntly slow for large tables, as is claims_bob. 
#        
#        We will therefore do the following:
#        1. Create a table claims_bob_<icf_id> that looks like claims_bob
#        2. Acquire a lock on claims_bob
#        3. Set the auto increment id on claims_bob_<icf_id> to what claims_bob has
#        4. Insert into claims_bob_<icf_id>
#        5. Set the auto increment id on claims_bob to what claims_bob_<icf_id> has
#        6. Release lock on claims_bob
        SKIP_SPECIALTY_CODE = ['thp','nhp']
        x_claims_table = """%s.x_claims%s_%s""" % (whcfg.scratch_schema, 
                                                   self.claims_table_suffix, 
                                                   self.imported_claim_file_id)
        claims_insert_queries = []
        d_c_icf = """DROP TABLE IF EXISTS {x_claims_table}""".format(x_claims_table=x_claims_table)
        
        claims_insert_queries.append({'query':d_c_icf,
                                      'description':'Drop table if exists {x_claims_table}'.format(x_claims_table=x_claims_table),
                                      'warning_filter':'ignore'})
                                      
        c_c_icf = """CREATE TABLE %s LIKE %s.claims%s""" % (x_claims_table,
                                                            whcfg.claims_master_schema, 
                                                            self.claims_table_suffix)
        claims_insert_queries.append({'query':c_c_icf,
                                      'description':'Creating table %s.x_claims%s_%s like %s.claims%s' % (whcfg.scratch_schema, 
                                                                                                     self.claims_table_suffix, 
                                                                                                     self.imported_claim_file_id,
                                                                                                     whcfg.claims_master_schema, 
                                                                                                     self.claims_table_suffix)})
        
        l_c = """LOCK TABLES %s.x_claims%s_%s WRITE, %s.%s cic WRITE, %s.service_type_mapping_%s WRITE, %s.service_place_mapping_%s WRITE, wellpoint_imported_claim_members READ, bcbsal_imported_claim_providers READ, wellpoint_xref_providers READ""" % (whcfg.scratch_schema, 
                                                                           self.claims_table_suffix, 
                                                                           self.imported_claim_file_id,
                                                                           whcfg.claims_master_schema,
                                                                           self.stage_claim_table,
                                                                           whcfg.scratch_schema,
                                                                           self.imported_claim_file_id,
                                                                           whcfg.scratch_schema,
                                                                           self.imported_claim_file_id)
        claims_insert_queries.append({'query':l_c,
                                      'description':'Acquire lock on claims%s' % self.claims_table_suffix}) 
        
#        ac_c = """SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='claims%s'""" % (whcfg.claims_master_schema, self.claims_table_suffix)
#        ac_c_r = Query(self.conn, ac_c)
#        ac_claims = ac_c_r.next()['AUTO_INCREMENT']
#        
#        a_c_icf = """ALTER TABLE  %s.x_claims%s_%s AUTO_INCREMENT = %d""" % (whcfg.scratch_schema, 
#                                                                           self.claims_table_suffix, 
#                                                                           self.imported_claim_file_id,
#                                                                           ac_claims)
#        
#        claims_insert_queries.append({'query':a_c_icf,
#                                      'description':'Setting AUTO_INCREMENT on table %s.x_claims%s_%s' % (whcfg.scratch_schema, 
#                                                                                                         self.claims_table_suffix, 
#                                                                                                         self.imported_claim_file_id)})

        
        utils.execute_queries(self.conn, logger, claims_insert_queries)
        claims_insert_queries = []
        
        utils.drop_table_indexes(self.conn, '%s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                self.claims_table_suffix, 
                                                                self.imported_claim_file_id))
        
        t_claims_icf = dbutils.Table(self.conn, '%s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                self.claims_table_suffix, 
                                                                self.imported_claim_file_id))
                
        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        
        provider_insert = self.__refresh_x_claim_field_mapping_generic(source_table=t_stage_claim_table,
                                                                       table_alias='cic',
                                                                       field_name_list=['provider_pin',
                                                                                        'provider_tax_id',
                                                                                        'provider_type',
                                                                                        'street_address',
                                                                                        'unit',
                                                                                        'city',
                                                                                        'state',
                                                                                        'zip',
                                                                                        'place_of_service',
                                                                                        'type_of_service',
                                                                                        'provider_specialty_code',
                                                                                        'out_of_network_indicator',
                                                                                        'provider_network_id'],
                                                                       derived_fields = None,
                                                                       logger=logger)
        
        procedure_insert = self.__refresh_x_claim_procedure_insert_generic(source_table=t_stage_claim_table, table_alias='cic', logger=logger)

        secondary_procedure_insert = self.__refresh_x_claim_procedure_insert_generic(source_table=t_stage_claim_table, table_alias='cic', logger=logger, is_primary = False)

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

        if self.insurance_company_name.lower() in SKIP_SPECIALTY_CODE and 'provider_specialty_code' not in provider_insert.keys():
            provider_insert['provider_specialty_code'] = 'NULL' 
        add_columns_map = {}
        add_columns_map.update(provider_insert)
        add_columns_map.update(procedure_insert)
        add_columns_map.update(member_insert)
        add_columns_map.update(secondary_procedure_insert)
        
        address_fields = set(['street_address','unit','city','state','zip'])
        add_columns = set(provider_insert.keys()) | set(procedure_insert.keys()) | set(member_insert.keys()) | address_fields | set(secondary_procedure_insert.keys())
        
        add_columns = {x:'varchar(255)' for x in add_columns if x not in t_claims_icf.columns()}
        add_columns['address_sha1'] ='binary(20) NOT NULL'
        add_columns['procedure_code_sha1'] ='binary(20) NOT NULL'
        add_columns['member_sha1'] ='binary(20)'
        add_columns['member_dob'] = 'date'

        if 'secondary_procedure_code_sha1' in add_columns:
           add_columns['secondary_procedure_code_sha1'] ='binary(20) NOT NULL'
        
        
        add_columns_list = ['ADD COLUMN {column_name} {column_definition}'.format(column_name=c, column_definition=d) for c,d in add_columns.iteritems()] 
        alter_claims_icf = 'ALTER TABLE {scratch_schema}.x_claims{claims_table_suffix}_{imported_claim_file_id} {add_columns}'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                        claims_table_suffix=self.claims_table_suffix,
                                                                                                                        imported_claim_file_id=self.imported_claim_file_id,
                                                                                                                        add_columns=','.join(add_columns_list))
        claims_insert_queries.append({'query':alter_claims_icf,
                                      'description':'Altering table %s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                         self.claims_table_suffix, 
                                                                                                         self.imported_claim_file_id)})

        create_address_sha1_index = 'CREATE INDEX ix_address_sha1 ON {scratch_schema}.x_claims{claims_table_suffix}_{imported_claim_file_id}(address_sha1)'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                            claims_table_suffix=self.claims_table_suffix,
                                                                                                                                                            imported_claim_file_id=self.imported_claim_file_id)
        create_member_sha1_index = 'CREATE INDEX ix_member_sha1 ON {scratch_schema}.x_claims{claims_table_suffix}_{imported_claim_file_id}(member_sha1)'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                            claims_table_suffix=self.claims_table_suffix,
                                                                                                                                                            imported_claim_file_id=self.imported_claim_file_id)
        create_procedure_code_sha1_index = 'CREATE INDEX ix_procedure_code_sha1 ON {scratch_schema}.x_claims{claims_table_suffix}_{imported_claim_file_id}(procedure_code_sha1)'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                                            claims_table_suffix=self.claims_table_suffix,
                                                                                                                                                            imported_claim_file_id=self.imported_claim_file_id)
        claims_insert_queries.append({'query':create_address_sha1_index,
                                      'description':'create index ix_address_sha1 on table %s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                         self.claims_table_suffix, 
                                                                                                         self.imported_claim_file_id)})
        claims_insert_queries.append({'query':create_member_sha1_index,
                                      'description':'create index ix_member_sha1 on table %s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                         self.claims_table_suffix, 
                                                                                                         self.imported_claim_file_id)})
        claims_insert_queries.append({'query':create_procedure_code_sha1_index,
                                      'description':'create index ix_procedure_code_sha1 on table %s.x_claims%s_%s' % (whcfg.scratch_schema, 
                                                                                                         self.claims_table_suffix, 
                                                                                                         self.imported_claim_file_id)})
        
        # TODO: Update to make this more generic to support missing fields or additional fields e.g. diagnosis_code_4
        field_mappings = self.normalization_rules.get('M')
        
        out_of_network_mappings = self.load_properties.get('out_of_network_indicator')
        inn_codes = set([str(x).lower() for x in out_of_network_mappings.get('n')]) if isinstance(out_of_network_mappings.get('n'), list) else set([str(out_of_network_mappings.get('n')).lower()]) if out_of_network_mappings else None
        inn_codes_insert = "('%s')" % "','".join(inn_codes)
        
        add_columns_map['address_sha1'] = """UNHEX(SHA1(CONCAT_WS(':',COALESCE({street_address},''),COALESCE({unit},''),COALESCE({city},''),COALESCE({state},''),COALESCE({zip},'')
                                            )))""".format(street_address=field_mappings.get('street_address') if field_mappings.get('street_address') else "''",
                                                          unit=field_mappings.get('unit') if field_mappings.get('unit') else "''",
                                                          city=field_mappings.get('city') if field_mappings.get('city') else "''",
                                                          state=field_mappings.get('state') if field_mappings.get('state') else "''",
                                                          zip=field_mappings.get('zip') if field_mappings.get('zip') else "''")
        
        allowed_amount_value = 'COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0) + COALESCE(%s,0)' % (field_mappings.get('paid_amount') if field_mappings.get('paid_amount') else '0',
                                                                                                                               field_mappings.get('copay_amount') if field_mappings.get('copay_amount') else '0',
                                                                                                                               field_mappings.get('cob_amount') if field_mappings.get('cob_amount') else '0',
                                                                                                                               field_mappings.get('coinsurance_amount') if field_mappings.get('coinsurance_amount') else '0',
                                                                                                                               field_mappings.get('deductible_amount') if field_mappings.get('deductible_amount') else '0') if not field_mappings.get('allowed_amount') \
                            else field_mappings.get('allowed_amount')
                      
                      
        drg_type_mapping = None
        drg_type_mapping = "'" + FALLBACK_DRG_TYPES.get(self.insurance_company_name.lower(), 'UNKNOWN') + "'"
        if self.load_properties.get('field_column_mappings').get('drg_type') and not isinstance(self.load_properties.get('field_column_mappings').get('drg_type'), dict) \
        and re.match(r"^'.*'$",self.load_properties.get('field_column_mappings').get('drg_type')):
            drg_type_mapping = self.load_properties.get('field_column_mappings').get('drg_type')
        elif field_mappings.get('drg_type'):
            drg_type_mapping = field_mappings.get('drg_type')
        
        insert_map = {'imported_claim_id':'id',
                      'imported_claim_file_id':str(self.imported_claim_file_id),
                      'subscriber_patient_id':'-1',
                      'patient_id':'-1',
                      'user_id':'-1', 
                      'insurance_company_id':str(self.insurance_company_id), 
                      'employer_id':str(self.employer_id),
                      'imported_at':'NOW()',
                      'updated_at':'NOW()',
                      'parse_status':'1',
                      'access_privileges':'1',
                      'member_id':'SHA1(%s)' % field_mappings.get('member_id'),
                      'out_of_network':"IF(TRIM(%s) IN %s, 0, 1)" % (field_mappings.get('out_of_network_indicator'),
                                                                                                    inn_codes_insert),
                      'units_of_service':'IF(%s=0,1,COALESCE(%s,1))' % (field_mappings.get('units_of_service'),field_mappings.get('units_of_service')) if field_mappings.get('units_of_service') else '0',
                      'service_begin_date':"IF(%s < '1970-01-01', NULL, %s)" % (field_mappings.get('service_begin_date'),
                                                                                field_mappings.get('service_begin_date')),
                      'service_end_date':"IF(%s < '1970-01-01',IF(%s < '1970-01-01', NULL, %s),%s)" % (field_mappings.get('service_end_date'),
                                                                                                       field_mappings.get('service_begin_date'),
                                                                                                       field_mappings.get('service_begin_date'),
                                                                                                       field_mappings.get('service_end_date')),
                      'payment_date':field_mappings.get('payment_date'),
                      'charged_amount':'COALESCE(%s,0)' % field_mappings.get('charged_amount') if field_mappings.get('charged_amount') else '0',
                      'allowed_amount': allowed_amount_value,
                      'savings_amount':'COALESCE(%s,0)' % field_mappings.get('savings_amount') if field_mappings.get('savings_amount') else '0',
                      'cob_amount':'COALESCE(%s,0)' % field_mappings.get('cob_amount') if field_mappings.get('cob_amount') else '0',
                      'coinsurance_amount':'COALESCE(%s,0)' % field_mappings.get('coinsurance_amount') if field_mappings.get('coinsurance_amount') else '0',
                      'deductible_amount':'COALESCE(%s,0)' % field_mappings.get('deductible_amount') if field_mappings.get('deductible_amount') else '0',
                      'paid_amount':'COALESCE(%s,0)' % field_mappings.get('paid_amount') if field_mappings.get('paid_amount') else '0',
                      'copay_amount':'COALESCE(%s,0)' % field_mappings.get('copay_amount') if field_mappings.get('copay_amount') else '0',
                      'not_covered_amount':'COALESCE(%s,0)' % field_mappings.get('not_covered_amount') if field_mappings.get('not_covered_amount') else '0',
                      'diagnosis_code_1':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_1')) if field_mappings.get('diagnosis_code_1') else 'NULL',
                      'diagnosis_code_2':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_2')) if field_mappings.get('diagnosis_code_2') else 'NULL',
                      'diagnosis_code_3':"IF(%s is NULL, NULL, IF(LOCATE('.',%s) > 0, %s, CONCAT_WS('.',LEFT(%s, 3), IF(CHAR_LENGTH(TRIM(%s)) > 3, SUBSTRING(%s,4), NULL))))" % (field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3'),
                                                                                                                                                                                 field_mappings.get('diagnosis_code_3')) if field_mappings.get('diagnosis_code_3') else 'NULL',
                      'drg':field_mappings.get('diagnosis_related_group') if field_mappings.get('diagnosis_related_group') else 'NULL',
                      'drg_type':"IF(%s is not null, %s, NULL)" % (field_mappings.get('diagnosis_related_group') if field_mappings.get('diagnosis_related_group') else 'null', drg_type_mapping),
                      'source_claim_number':field_mappings.get('source_claim_id') if field_mappings.get('source_claim_id') else 'NULL',
                      'source_claim_line_number': field_mappings.get('source_claim_line_number') if field_mappings.get('source_claim_line_number') else 'NULL',
                      'provider_name':'%s.title_case(%s)' % (whcfg.master_schema, field_mappings.get('provider_name')) if field_mappings.get('provider_name') else 'NULL',
                      'procedure_label_id':'-1',
                      'provider_location_id':'-1',
                      'insurance_network_id':'-1',
                      'inpatient':'%s' % field_mappings.get('inpatient') if field_mappings.get('inpatient') != None \
                                        else 'IF(%s OR %s,1,0)' % (field_mappings.get('diagnosis_related_group') if field_mappings.get('diagnosis_related_group') else '0',
                                        "(%s.resolve_service_place_%s(%s) = 21)" % (whcfg.scratch_schema, self.imported_claim_file_id, field_mappings.get('place_of_service')) if field_mappings.get('place_of_service') else '0'),
#                      'inpatient':'-1',
                      'hra_amount':'COALESCE(%s,0)' % field_mappings.get('hra_amount') if field_mappings.get('hra_amount') else '0',
                      'service_place_id':"%s.resolve_service_place_%s(%s)" % (whcfg.scratch_schema, self.imported_claim_file_id, field_mappings.get('place_of_service')),
                      'service_type_id':"%s.resolve_service_type_%s(%s)" % (whcfg.scratch_schema, self.imported_claim_file_id, field_mappings.get('type_of_service')) if field_mappings.get('type_of_service') else 'NULL',
#                      'service_place_id':'-1',
#                      'service_type_id':"-1",
                      'parse_comment':"''",
                      'provider_id':'-1',
                      'provider_specialty_code':field_mappings.get('provider_specialty_code')
                      }
       
#        inpatient_expression = 'IF(%s OR %s,1,0)' % (field_mappings.get('diagnosis_related_group') if field_mappings.get('diagnosis_related_group') else '0',
#                                                     "(%s.resolve_service_place_%s(%s) = 21)" % (whcfg.scratch_schema, self.imported_claim_file_id, field_mappings.get('place_of_service')) if field_mappings.get('place_of_service') else '0')
#        normalized_claim.get('diagnosis_related_group') or int(place_of_service_id) == 21
        
        insert_map.update(add_columns_map)
        
        if self.insurance_company_name.lower() in SKIP_SPECIALTY_CODE and insert_map.get('provider_specialty_code') == None:
            insert_map['provider_specialty_code'] = 'NULL'
        insert_cols = insert_map.keys()
        insert_vals = [insert_map.get(k) for k in insert_cols]
        q_insert_claims = """INSERT INTO %s.x_claims%s_%s (%s)
                                              SELECT %s
                                               FROM %s.%s cic
                                              WHERE imported_claim_file_id = %s
                                              AND duplicate_of_claim_id is null
                                              %s""" % (whcfg.scratch_schema,
                                                       self.claims_table_suffix, 
                                                       self.imported_claim_file_id,
                                                       ',\n'.join(insert_cols),
                                                       ',\n'.join(insert_vals),
                                                       whcfg.claims_master_schema,
                                                       self.stage_claim_table,
                                                       self.imported_claim_file_id,
                                                       LIMIT_CLAUSE)
        
        claims_insert_queries.append({'query':q_insert_claims,
                                      'description':'Insert new claims records into %s.x_claims%s_%s' % (whcfg.scratch_schema,
                                                                                                       self.claims_table_suffix,
                                                                                                       self.imported_claim_file_id),
                                      'warning_filter':'ignore'})        
        utils.execute_queries(self.conn, logger, claims_insert_queries)
        claims_insert_queries = []
        
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
                                      'description':'Release lock on table %s.claims%s' % (whcfg.claims_master_schema, 
                                                                                           self.claims_table_suffix)})
        utils.execute_queries(self.conn, logger, claims_insert_queries)

    def __refresh_x_claim_locations_generic(self, logger):
        '''Normalize raw locations via address_utils.normalize_address().
           Insert them into stage_location and populate intermediate table address_sha1_match_key_unit_sha1.'''

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


        logutil.log(logger, logutil.INFO, "Querying %s.x_claims%s for locations to be normalized" % (whcfg.scratch_schema, self.imported_claim_file_id))
        stage_cursor = self.conn.cursor()
        q_select_address = '''
                              SELECT address_sha1
                              ,      street_address
                              ,      unit
                              ,      city
                              ,      state
                              ,      zip
                              FROM %s.x_claims_%s
                              WHERE street_address is not null
                                OR city is not null
                                OR state is not null
                                OR zip is not null
                              GROUP BY address_sha1
                           ''' % (whcfg.scratch_schema, self.imported_claim_file_id)
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
        q_insert_sl = '''
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
        
        logutil.log(logger, logutil.INFO, "Normalizing locations from %s.x_claims%s" % (whcfg.scratch_schema, self.imported_claim_file_id))
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
            qv_insert_sl.append(('CLAIMS_%s' % self.imported_claim_file_id,normalized_address['match_key'],normalized_address['query_address'],normalized_address['building_name'],normalized_address['street_address'],normalized_address['unit'],normalized_address['city'],normalized_address['state'],normalized_address['zip'],normalized_address['country'],match_key_unit_sha1,requested_action))
    
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
        x_claim_provider_location_update = """UPDATE %s.x_claims_%s x 
                                                JOIN %s.address_sha1_match_key_unit_sha1%s s  USING(address_sha1)
                                                 SET x.provider_location_id=s.provider_location_id""" % (whcfg.scratch_schema,
                                                                                                        self.imported_claim_file_id,
                                                                                                        whcfg.scratch_schema,
                                                                                                        self.imported_claim_file_id)
        
        stage_cursor.execute(x_claim_provider_location_update)
        logutil.log(logger, logutil.INFO, "Updated %s.x_claims_%s with provider_location_id" % (whcfg.scratch_schema, self.imported_claim_file_id))
        
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
    
    def __refresh_x_claim_procedures_generic(self, logger, is_primary = True):
        
        t_stage_claim_table = dbutils.Table(self.conn, self.stage_claim_table)
        prefix = 'secondary_' if (is_primary == False) else ''
        
        procedure_insert = self.__refresh_x_claim_procedure_insert_generic(source_table=t_stage_claim_table, table_alias='cic', logger=logger, is_primary=is_primary)
        
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
                                            FROM {scratch_schema}.x_claims_{imported_claim_file_id}
                                            GROUP BY {prefix}procedure_code_sha1""".format(procedure_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(procedure_insert_keys)]),
                                                                                   procedure_columns=','.join(sorted(procedure_insert_keys)),
                                                                                  scratch_schema=whcfg.scratch_schema,
                                                                                  imported_claim_file_id=self.imported_claim_file_id,prefix = prefix)

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
            condition = 0
            if procedure_code and procedure_code_type:
                condition = 1
                pass
            elif procedure_code:
                condition = 2
                #procedure_code_length = self.procedure_code_type_lengths.get(str(procedure_code_type_id), 0)
               # procedure_code = procedure_code.zfill(procedure_code_length)
        
                # Resolve procedure_code_type
                if self.procedure_codes.get(self.procedure_code_types['cpt']).get(procedure_code.zfill(self.procedure_code_type_lengths.get(str(self.procedure_code_types['cpt']), 0)).lower()):
                    procedure_code_type = 'cpt'
                elif self.procedure_codes.get(self.procedure_code_types['hcpc']).get(procedure_code.zfill(self.procedure_code_type_lengths.get(str(self.procedure_code_types['hcpc']), 0)).lower()):
                    procedure_code_type = 'hcpc'
                elif self.procedure_codes.get(self.procedure_code_types['revenue']).get(procedure_code.zfill(self.procedure_code_type_lengths.get(str(self.procedure_code_types['revenue']), 0)).lower()):
                    procedure_code_type = 'revenue'
                elif self.procedure_codes.get(self.procedure_code_types['icd9']).get(procedure_code.zfill(self.procedure_code_type_lengths.get(str(self.procedure_code_types['icd9']), 0)).lower()):
                    procedure_code_type = 'icd9'
                
            elif procedure_code_type:
                condition = 3
                # Resolve internal procedure_code_type
                procedure_code = claim_pl_row.get('{prefix}%s_code'.format(prefix=prefix) % procedure_code_type)
                
            else:
                condition = 4
                procedure_code = claim_pl_row.get('{prefix}cpt_code'.format(prefix=prefix))
                procedure_code_type = 'cpt'
                if not procedure_code:
                    procedure_code = claim_pl_row.get('{prefix}hcpc_code'.format(prefix=prefix))
                    procedure_code_type = 'hcpc'
                if not procedure_code:
                    procedure_code = claim_pl_row.get('{prefix}revenue_code'.format(prefix=prefix))
                    procedure_code_type = 'revenue'
                if not procedure_code:
                    procedure_code = claim_pl_row.get('{prefix}icd9_code'.format(prefix=prefix))
                    procedure_code_type = 'icd9'
                
            if procedure_code and (procedure_code_type == 'cpt' or procedure_code_type == 'hcpc'):
                hcpc_matcher = re.compile('^[a-z]{1}[0-9]{4}', flags=re.IGNORECASE)
                if hcpc_matcher.match(procedure_code):
                    procedure_code_type = 'hcpc'
                else:
                    procedure_code_type = 'cpt'
            
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
                        # Create procedure_code
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
                    # Unknown procedure_code_type
                    # procedure_code_id is -1 too
                    # Only create Exception
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
                
#            if i < 5:
#                print condition, procedure_code_type, procedure_code
            qv_update_procedure.append((procedure_code_id, procedure_code_type_id, procedure_modifier_id, procedure_label_id, claim_pl_row.get('{prefix}procedure_code_sha1'.format(prefix=prefix))))
#            if i < 5:
#                print claim_pl_row
                
        if qv_update_procedure:
            cur = self.conn.cursor()
            
            cur.executemany(q_update_procedure, qv_update_procedure)
            if is_primary == True: 
                x_claims_update_procedure = """UPDATE {scratch_schema}.x_claims_{imported_claim_file_id} x
                                            JOIN {scratch_schema}.claim_procedure_labels_{imported_claim_file_id} cp USING (procedure_code_sha1)
                                             SET x.procedure_label_id=cp.procedure_label_id,
                                                 x.parse_status=IF(cp.procedure_label_id=-1,0,1),
                                                 x.parse_comment=IF(cp.procedure_label_id=-1,'Procedure Code not specified in claim.',NULL)
                                             WHERE cp.procedure_label_id is not null""".format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id)
            else:
                x_claims_update_procedure = """UPDATE {scratch_schema}.x_claims_{imported_claim_file_id} x
                                            JOIN {scratch_schema}.{prefix}claim_procedure_labels_{imported_claim_file_id} cp USING ({prefix}procedure_code_sha1)
                                             SET x.{prefix}procedure_label_id=cp.{prefix}procedure_label_id
                                             WHERE cp.{prefix}procedure_label_id is not null""".format(\
                                             scratch_schema=whcfg.scratch_schema, \
                                             imported_claim_file_id=self.imported_claim_file_id, prefix = prefix)
            
            cur.execute(x_claims_update_procedure)
            cur.close()
            
    def rehash_claim_patients(self, logger,subscriber_patient_account_id = None, patient_account_id = None, rehash_unidentified = False):
        
        x_claims_table = """%s.x_claims_rehash%s_%s""" % (whcfg.scratch_schema, 
                                                   self.claims_table_suffix, 
                                                   self.imported_claim_file_id)
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
            filter_query = """select p.first_name,p.last_name,p.date_of_birth,p.ssn from patients p 
                            join accounts a on a.patient_id = p.id
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
        
        d_c_icf = """DROP TABLE IF EXISTS {x_claims_table}""".format(x_claims_table=x_claims_table)
        c_c_icf = """CREATE TABLE {x_claims_table} 
                    (imported_claim_id INT(11),
                     {claim_patient_column_defs},
                     subscriber_patient_id INT(11),
                     patient_id INT(11),
                     INDEX ix_member_sha1(member_sha1))
                     """.format(x_claims_table=x_claims_table,
                                claim_patient_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(member_insert_keys)]))
        i_c_icf = """INSERT INTO {x_claims_table}
                     SELECT cic.id,
                     {claim_patient_columns},
                     -1,
                     -1
                     FROM {claims_master_schema}.{stage_table} cic
                     JOIN claims c on c.imported_claim_id = cic.id and cic.imported_claim_file_id = c.imported_claim_file_id
                     WHERE cic.imported_claim_file_id = {imported_claim_file_id}
                     {filter}
                     """.format(x_claims_table=x_claims_table,
                                claim_patient_columns=','.join([member_insert.get(key) for key in sorted(member_insert_keys)]),
                                claims_master_schema=whcfg.claims_master_schema,
                                stage_table=self.stage_claim_table,
                                imported_claim_file_id=self.imported_claim_file_id,
                                filter = "%s" % filter if filter else '')
                     
        claims_insert_queries.extend([{'query':d_c_icf,
                                      'description':'Drop table if exists {x_claims_table}'.format(x_claims_table=x_claims_table),
                                      'warning_filter':'ignore'},
                                     {'query':c_c_icf,
                                      'description':'Create table {x_claims_table}'.format(x_claims_table=x_claims_table),
                                      'warning_filter':'ignore'},
                                     {'query':i_c_icf,
                                      'description':'Insert into {x_claims_table}'.format(x_claims_table=x_claims_table),
                                      'warning_filter':'ignore'}]) 
        utils.execute_queries(self.conn, logger, claims_insert_queries) 
        drop_patients_icf = """DROP TABLE IF EXISTS {scratch_schema}.claim_patients_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                        imported_claim_file_id=self.imported_claim_file_id)
        create_patients_icf = """CREATE TABLE {scratch_schema}.claim_patients_{imported_claim_file_id} 
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
                                    'description':'Drop {scratch_schema}.claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id),
                                    'warning_filter':'ignore'},
                                   {'query':create_patients_icf,
                                    'description':'Create {scratch_schema}.claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)}]
        utils.execute_queries(self.conn, logger, claims_patients_queries)
        
        t_claim_patients = dbutils.Table(self.conn, '{scratch_schema}.claim_patients_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id))
        
        t_claim_patient_columns = t_claim_patients.columns()
        is_relationship_available = 'member_relationship'  in t_claim_patient_columns
        is_member_ssn_available = 'member_ssn' in t_claim_patient_columns
        is_subscriber_first_name_available = 'employee_first_name' in t_claim_patient_columns
        
        if isinstance(self.load_properties.get('member_relationships').get('subscriber'), str):
            subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber').split(','))
        else:
            subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber')) if is_relationship_available else set([])
        
        subscriber_codes = set([x.lower() for x in subscriber_codes if x])
        
        q_update_member = '''UPDATE %s.claim_patients_%s
                                SET subscriber_patient_id = %s,
                                    patient_id = %s
                             WHERE member_sha1 = %s
                       ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s')
                       
        qv_update_member = []             
        
        suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))
          
        for i, cp in enumerate(t_claim_patients):
            pi = claims_util.PatientIdentifier()
            if not cp.get('member_relationship'):
                  is_relationship_available = False
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
                'insurance_company_id':self.insurance_company_id,
                'employer_id':self.employer_id }
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
            
            x_claims_update_patients = """UPDATE {x_claims_table} x
                                            JOIN {scratch_schema}.claim_patients_{imported_claim_file_id} cp USING (member_sha1)
                                             SET x.subscriber_patient_id=cp.subscriber_patient_id,
                                                 x.patient_id=cp.patient_id""".format(x_claims_table=x_claims_table,scratch_schema=whcfg.scratch_schema,
                                                                                      imported_claim_file_id=self.imported_claim_file_id)
            cur.execute(x_claims_update_patients)
            
        claims_update_patients = """UPDATE {claims_master_schema}.claims{claims_table_suffix} c
                                  JOIN {x_claims_table} x ON c.imported_claim_id = x.imported_claim_id
                                  SET c.subscriber_patient_id = x.subscriber_patient_id,
                                  c.patient_id =  x.patient_id
                                  where c.imported_claim_file_id = {imported_claim_file_id}""".format(claims_master_schema=whcfg.claims_master_schema,
                                                                                                      claims_table_suffix=self.claims_table_suffix,
                                                                                                      x_claims_table=x_claims_table,
                                                                                                      imported_claim_file_id=self.imported_claim_file_id)
        cur.execute(claims_update_patients)
        cur.close()
        
        
    def __refresh_x_claim_patients_generic(self, logger):

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
        drop_patients_icf = """DROP TABLE IF EXISTS {scratch_schema}.claim_patients_{imported_claim_file_id}""".format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)
        create_patients_icf = """CREATE TABLE {scratch_schema}.claim_patients_{imported_claim_file_id} 
                                           ({claim_patient_column_defs},
                                            user_id INT(11),
                                            subscriber_patient_id INT(11),
                                            patient_id INT(11),
                                            INDEX ix_member_sha1(member_sha1))
                                            AS SELECT {claim_patient_columns},
                                            user_id, subscriber_patient_id, patient_id
                                            FROM {scratch_schema}.x_claims_{imported_claim_file_id}
                                            GROUP BY member_sha1""".format(claim_patient_column_defs=','.join(['%s VARCHAR(255)' % k for k in sorted(member_insert_keys)]),
                                                                                   claim_patient_columns=','.join(sorted(member_insert_keys)),
                                                                                  scratch_schema=whcfg.scratch_schema,
                                                                                  imported_claim_file_id=self.imported_claim_file_id)
        
        
        claims_patients_queries = [{'query': drop_patients_icf,
                                    'description':'Drop {scratch_schema}.claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id),
                                    'warning_filter':'ignore'},
                                   {'query':create_patients_icf,
                                    'description':'Create {scratch_schema}.claim_patients_{imported_claim_file_id} table if it exists.'.format(scratch_schema=whcfg.scratch_schema,
                                                                                                                                   imported_claim_file_id=self.imported_claim_file_id)}]
        utils.execute_queries(self.conn, logger, claims_patients_queries)
        
        t_claim_patients = dbutils.Table(self.conn, '{scratch_schema}.claim_patients_{imported_claim_file_id}'.format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id))
        
        t_claim_patient_columns = t_claim_patients.columns()
        is_relationship_available = 'member_relationship'  in t_claim_patient_columns
        is_member_ssn_available = 'member_ssn' in t_claim_patient_columns
        is_subscriber_first_name_available = 'employee_first_name' in t_claim_patient_columns
        
        if isinstance(self.load_properties.get('member_relationships').get('subscriber'), str):
            subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber').split(','))
        else:
            subscriber_codes = set(self.load_properties.get('member_relationships').get('subscriber')) if is_relationship_available else set([])
        
        subscriber_codes = set([x.lower() for x in subscriber_codes if x])
        
        q_update_member = '''UPDATE %s.claim_patients_%s
                                SET subscriber_patient_id = %s,
                                    patient_id = %s
                             WHERE member_sha1 = %s
                       ''' % (whcfg.scratch_schema, self.imported_claim_file_id, '%s', '%s', '%s')
                       
        qv_update_member = []             
        
        suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))
          
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
			    'insurance_company_id':self.insurance_company_id,
                            'employer_id':self.employer_id }
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
        
        if qv_update_member:
            cur = self.conn.cursor()
            
            cur.executemany(q_update_member, qv_update_member) 
            
            x_claims_update_patients = """UPDATE {scratch_schema}.x_claims_{imported_claim_file_id} x
                                            JOIN {scratch_schema}.claim_patients_{imported_claim_file_id} cp USING (member_sha1)
                                             SET x.subscriber_patient_id=cp.subscriber_patient_id,
                                                 x.patient_id=cp.patient_id""".format(scratch_schema=whcfg.scratch_schema, imported_claim_file_id=self.imported_claim_file_id)
            
            cur.execute(x_claims_update_patients)
            cur.close()

       

        
    def __refresh_x_claim_procedure_insert_generic(self, source_table, table_alias, logger, is_primary = True):
        #Procedure Codes and Types:
        #
        #   * Single Procedure Code column
        #   * Revenue code alone in a separate column 
        #   * Multiple Procedure Code columns
        #
        #In each of the above cases, the Procedure Code Type may or may not be provided in an additional column.
        
        # TODO: Need to modify to support formula
        
        icf_columns = source_table.columns()
        
        proc_insert = ''
        proc_insert_map = {}

        prefix = 'secondary_' if (is_primary == False) else ''
        
#        procedure_code_type_column = self.load_properties.get('field_column_mappings').get('procedure_code_type')
        
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
#                proc_code_insert_list = ["\n{ext_proc_code_column} as {procedure_code_type}_code".format(ext_proc_code_column=v, procedure_code_type=k) for k,v in procedure_code_mapping.iteritems() if v and v.strip() != '']
                proc_insert_map.update({'{prefix}{procedure_code_type}_code'.format(prefix= prefix, procedure_code_type=k):'{ext_proc_code_column}'.format(ext_proc_code_column=v) for k,v in procedure_code_mapping.iteritems() if v and v.strip() != ''})
            else:
#                proc_code_insert_list = ["\n{ext_proc_code_column} as procedure_code".format(ext_proc_code_column=procedure_code_mapping)]
                proc_insert_map.update({'{prefix}procedure_code'.format(prefix=prefix):'{ext_proc_code_column}'.format(ext_proc_code_column=procedure_code_mapping)})
         
        if len(proc_insert_map) > 0:
            proc_insert_map['{prefix}procedure_code_sha1'.format(prefix=prefix)] = "UNHEX(SHA1(CONCAT_WS(':',%s)))" % (','.join(["COALESCE(%s,'')" % proc_insert_map.get(k) for k in sorted(proc_insert_map.keys())]))
#        proc_code_insert = ','.join(proc_code_insert_list)
        
#        proc_insert = proc_insert + proc_code_insert + ','
        return proc_insert_map
        
    def __refresh_x_claims(self, logger):
        # TODO: Update to make this more generic on the lines of insert_claims
        d_x_claims = """DROP TABLE IF EXISTS %s.x_claims%s""" % (whcfg.scratch_schema, self.claims_table_suffix)
        c_x_claims = """CREATE TABLE %s.x_claims%s
                        (raw_address VARCHAR(500), 
                         provider_id INT(11), 
                         provider_location_id INT(11),
                         procedure_label_id INT(11),
                         service_place_id INT(11),
                         insurance_network_id INT(11),
                        INDEX ix_cigna_pin(cigna_pin))
                        AS
                        SELECT
                        id,
                        imported_claim_file_id,
                        PROV_NM as provider_name,
                        PROV_TY as provider_type,
                        TRIM(LEADING '0' FROM CPF_ID) as cigna_pin,
                        IF(LOCATE('#',PROV_ADDR) = 1, NULL, IF(LOCATE('#',PROV_ADDR2) = 1, NULL, PROV_ADDR2)) as building_name,
                        IF(LOCATE('#',PROV_ADDR) = 1,
                           PROV_ADDR2, IF(LOCATE('#',PROV_ADDR) > 1,
                           SUBSTRING(PROV_ADDR, 1, LOCATE('#',PROV_ADDR)-1),PROV_ADDR)) as street_address,
                        REPLACE(IF(LOCATE('#',PROV_ADDR2) = 1, 
                           PROV_ADDR2, 
                           IF(LOCATE('#',PROV_ADDR) = 1,
                              PROV_ADDR,
                              SUBSTRING(PROV_ADDR, LOCATE('#',PROV_ADDR)))),'#','Suite') as unit,
                        PROV_CITY as city,
                        PROV_ST as state,
                        PROV_ZIP as zip,
                        IF(cic.PROC_TY='RV', cic.CLM_LN_PROC_TY, cic.PROC_TY) as procedure_type,
                        IF(cic.PROC_TY='RV', cic.CLM_LN_PROC_CD, cic.PROC_CD) as procedure_code,
                        IF(cic.PROC_TY='RV', cic.CLM_LN_PROC_MDFR_CD, cic.MODIFIER) as procedure_code_modifier,
                        IF(cic.PROC_TY='RV',
                          CONCAT_WS('|', COALESCE(cic.CLM_LN_PROC_TY,''), COALESCE(cic.CLM_LN_PROC_CD,''), COALESCE(cic.CLM_LN_PROC_MDFR_CD,'')),
                          CONCAT_WS('|', COALESCE(cic.PROC_TY,''), COALESCE(cic.PROC_CD,''), COALESCE(cic.MODIFIER,''))) as procedure_label,
                        LOC_CD as place_of_service,
                        TRIM(LEADING '0' FROM TRIM(SVC_PROV_NT_ID)) as provider_network,
                        -1 as insurance_network_id,
                        -1 as provider_location_id,
                        -1 as provider_id,
                        -1 as procedure_label_id,
                        -1 as service_place_id,
                        PROVSPEC as provider_specialty_code,
                        RMK_CD,
                        RSN_NOT_COVRD_CD
                        FROM
                        %s.%s cic
                        WHERE duplicate_of_claim_id is NULL
                        AND imported_claim_file_id = %s
                        %s""" % (whcfg.scratch_schema, 
                                 self.claims_table_suffix, 
                                 whcfg.claims_master_schema, 
                                 self.stage_claim_table, 
                                 self.imported_claim_file_id,
                                 LIMIT_CLAUSE)
                        
        u_x_claims_bnsa = """UPDATE %s.x_claims%s
                                SET street_address=building_name, building_name=street_address
                              WHERE building_name REGEXP ' RD| BLDG| LN| DR| AVE| PKY| ST| HWY'""" % (whcfg.scratch_schema, self.claims_table_suffix)

        u_x_claims_ra = """UPDATE %s.x_claims%s 
                              SET raw_address = CONCAT_WS('|',COALESCE(building_name,''),COALESCE(street_address,''),COALESCE(unit,''),COALESCE(city,''),COALESCE(state,''),COALESCE(zip,''))
                              """ % (whcfg.scratch_schema, self.claims_table_suffix)

        ix_x_claims_ra = """CREATE INDEX ix_raw_address ON %s.x_claims%s (raw_address)""" % (whcfg.scratch_schema, self.claims_table_suffix)
        
        x_claims_queries = [{'query':d_x_claims,
                             'description':'Drop x_claims%s table if it exists.' % self.claims_table_suffix,
                             'warning_filter':'ignore'},
                            {'query':c_x_claims,
                             'description':'Create x_claims%s table.' % self.claims_table_suffix},
                            {'query':u_x_claims_bnsa,
                             'description':'Fixing switched building_name and street_address columns.'},
                            {'query':u_x_claims_ra,
                             'description':'Populating raw_Address in x_claims%s table.' % self.claims_table_suffix},
                            {'query':ix_x_claims_ra,
                             'description':'Creating index on x_claims%s(raw_address).' % self.claims_table_suffix}]
        utils.execute_queries(self.conn, logger, x_claims_queries)
    
    def __refresh_x_claim_locations(self, logger):
        
        d_x_claim_locations = """DROP TABLE IF EXISTS %s.x_claims%s_locations""" % (whcfg.scratch_schema, self.claims_table_suffix)

        c_x_claim_locations = """CREATE TABLE %s.x_claims%s_locations (id INT(11) AUTO_INCREMENT PRIMARY KEY, location_id INT(11), match_key VARCHAR(500), query_address VARCHAR(500))
                                           AS 
                                       SELECT raw_address, building_name, street_address, IF(TRIM(unit)='',NULL,unit) as unit, city, state, zip, -1 as location_id
                                         FROM %s.x_claims%s WHERE raw_address <> '||||||'
                                        GROUP BY raw_address""" % (whcfg.scratch_schema, self.claims_table_suffix,
                                                                   whcfg.scratch_schema, self.claims_table_suffix)
        x_claims_loc_queries = [{'query':d_x_claim_locations,
                                 'description':'Drop x_claims%s_locations table if it exists.' % self.claims_table_suffix,
                                 'warning_filter':'ignore'},
                                {'query':c_x_claim_locations,
                                 'description':'Create x_claims%s_locations table.' % self.claims_table_suffix}]
        
        utils.execute_queries(self.conn, logger, x_claims_loc_queries)
        
        location_utils.normalize_addresses(self.conn, '%s.x_claims%s_locations' % (whcfg.scratch_schema, self.claims_table_suffix), {'location_id': 'location_id',
                                                                                                                                     'building_name': 'building_name',
                                                                                                                                     'street_address': 'street_address',
                                                                                                                                     'unit': 'unit',
                                                                                                                                     'city': 'city',
                                                                                                                                     'state': 'state',
                                                                                                                                     'zip': 'zip',
                                                                                                                                     'match_key': 'match_key',
                                                                                                                                     'query_address': 'query_address'}
                                           ,'MASTER', self.prod_conn, None, False
                                           )
        
#        ix_x_claim_locations = """CREATE INDEX ix_mk_u ON %s.x_claims%s_locations (match_key, unit)""" % (whcfg.scratch_schema, self.claims_table_suffix)
#        
#        u_x_claim_locations_1 = """UPDATE %s.x_claims%s_locations xl,
#                                        %s.locations l
#                                    SET xl.location_id=l.master_location_id
#                                  WHERE xl.match_key=l.match_key
#                                  AND xl.unit=l.unit
#                                  AND l.source='MASTER'"""  % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.master_schema)
#
#        u_x_claim_locations_2 = """UPDATE %s.x_claims%s_locations xl,
#                                        %s.locations l
#                                    SET xl.location_id=l.master_location_id
#                                  WHERE xl.match_key=l.match_key
#                                  AND xl.unit is null
#                                  AND l.unit is null
#                                  AND l.source='MASTER'"""  % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.master_schema)
#                                                                    
#        x_claims_loc_queries = [{'query':ix_x_claim_locations,
#                                 'description':'Create index on x_claims%s_locations table.' % self.claims_table_suffix},
#                                {'query':u_x_claim_locations_1,
#                                 'description':'Updating location_id in x_claims%s_locations for non-null unit.' % self.claims_table_suffix},
#                                {'query':u_x_claim_locations_2,
#                                 'description':'Updating location_id in x_claims%s_locations for null unit.' % self.claims_table_suffix}]
                
#        utils.execute_queries(self.conn, logger, x_claims_loc_queries)
        
        return
    
    def __refresh_x_claim_providers(self, logger):
        
        d_x_claim_providers = """DROP TABLE IF EXISTS %s.x_claims%s_providers""" % (whcfg.scratch_schema, self.claims_table_suffix)
        
        c_x_claim_providers = """CREATE TABLE %s.x_claims%s_providers (provider_id INT(11) NOT NULL DEFAULT -1, INDEX ix_cigna_pin(cigna_pin))
                                           AS 
                                       SELECT provider_name, cigna_pin, provider_type
                                         FROM %s.x_claims%s
                                        WHERE cigna_pin <> '' AND cigna_pin is not null
                                        GROUP BY cigna_pin""" % (whcfg.scratch_schema, self.claims_table_suffix,
                                                                 whcfg.scratch_schema, self.claims_table_suffix)

        u_x_claim_providers = """UPDATE %s.x_claims%s_providers xp,
                                        %s.provider_external_ids pei
                                   SET xp.provider_id=pei.provider_id
                                WHERE xp.cigna_pin=pei.external_id
                                  AND pei.external_id_type='CIGNA'""" % (whcfg.scratch_schema, self.claims_table_suffix,
                                                                         whcfg.master_schema)

        x_claims_prov_queries = [{'query':d_x_claim_providers,
                                  'description':'Drop table x_claims%s_providers if exists.' % self.claims_table_suffix,
                                  'warning_filter':'ignore'},
                                 {'query':c_x_claim_providers,
                                  'description':'Create table x_claims%s_providers.' % self.claims_table_suffix},
                                 {'query':u_x_claim_providers,
                                  'description':'Updating provider_id in x_claims%s_providers.' % self.claims_table_suffix}]
                
        utils.execute_queries(self.conn, logger, x_claims_prov_queries)
        
        return

    def __refresh_x_claim_procedures(self, logger):
        
        d_x_claim_procedures = """DROP TABLE IF EXISTS %s.x_claims%s_procedures""" % (whcfg.scratch_schema, self.claims_table_suffix)
        c_x_claim_procedures = """CREATE TABLE %s.x_claims%s_procedures (id INT(11) AUTO_INCREMENT PRIMARY KEY, procedure_code_id INT(11), procedure_modifier_id INT(11), procedure_code_type_id INT(11), procedure_label_id INT(11), INDEX ix_procedure_label(procedure_label))
                                            AS 
                                        SELECT procedure_type, procedure_code, procedure_code_modifier, procedure_label
                                          FROM %s.x_claims%s 
                                         GROUP BY procedure_label""" % (whcfg.scratch_schema, self.claims_table_suffix,
                                                                        whcfg.scratch_schema, self.claims_table_suffix)

        u_x_claim_procedures_1 = """UPDATE %s.x_claims%s_procedures
                                      SET procedure_code_type_id=IF(procedure_type='CP',1,4), procedure_modifier_id=-1""" % (whcfg.scratch_schema, self.claims_table_suffix)

        u_x_claim_procedures_2 = """UPDATE %s.x_claims%s_procedures cbp,
                                           %s.procedure_codes pc
                                    SET cbp.procedure_code_id=pc.id
                                    WHERE cbp.procedure_code_type_id=pc.procedure_code_type_id
                                      AND cbp.procedure_code=pc.code""" % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.claims_master_schema)

        u_x_claim_procedures_3 = """UPDATE %s.x_claims%s_procedures cbp,
                                           %s.procedure_modifiers pm
                                    SET cbp.procedure_modifier_id=pm.id
                                    WHERE cbp.procedure_code_modifier=pm.code""" % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.claims_master_schema)

        u_x_claim_procedures_4 = """UPDATE %s.x_claims%s_procedures cbp,
                                           %s.procedure_labels pl
                                    SET cbp.procedure_label_id=pl.id
                                    WHERE cbp.procedure_code_id=pl.procedure_code_id
                                    AND cbp.procedure_modifier_id=pl.procedure_modifier_id""" % (whcfg.scratch_schema, self.claims_table_suffix, whcfg.claims_master_schema)

        x_claims_proc_queries = [{'query':d_x_claim_procedures,
                                  'description':'Drop table x_claims%s_procedures if exists.' % self.claims_table_suffix,
                                  'warning_filter':'ignore'},
                                 {'query':c_x_claim_procedures,
                                  'description':'Create table x_claims%s_procedures.' % self.claims_table_suffix},
                                 {'query':u_x_claim_procedures_1,
                                  'description':'Updating procedure_code_type_id in x_claims%s_procedures.' % self.claims_table_suffix},
                                 {'query':u_x_claim_procedures_2,
                                  'description':'Updating procedure_code_id in x_claims%s_procedures.' % self.claims_table_suffix},
                                 {'query':u_x_claim_procedures_3,
                                  'description':'Updating procedure_modifier_id in x_claims%s_procedures.' % self.claims_table_suffix},
                                 {'query':u_x_claim_procedures_4,
                                  'description':'Updating procedure_label_id in x_claims%s_procedures.' % self.claims_table_suffix}]
                
        utils.execute_queries(self.conn, logger, x_claims_proc_queries)
        return
        
    def insert_claim_provider_exceptions(self,map_load_properties):
        field_column_mappings = map_load_properties.get('field_column_mappings')
        t_table_name = Table(self.conn, self.stage_claim_table)
        cur = self.conn.cursor()
        ## Preparing insert columns names
        str_insert =['claim_id' ,'imported_claim_file_id' ,'source_claim_number' , 'source_claim_line_number' , 'employer_id' , 'insurance_company_id' ]
        ## Preparint select columns names from the join
        str_select =['c.id','c.imported_claim_file_id' ,'c.source_claim_number','c.source_claim_line_number','c.employer_id','c.insurance_company_id']
        ## Logic to check if the column mappping exists in the mapping text passed. 
        ## If it exist added to the list of the insert and select and latter create comma separated string to create query.
        if (field_column_mappings.get('provider_tax_id')):
             str_insert.append('provider_tax_id')
             str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('provider_tax_id'), 'ic'))
        if (field_column_mappings.get('provider_name')):
             str_insert.append('provider_name')
             str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('provider_name'), 'ic'))
        if (field_column_mappings.get('street_address')):
            str_insert.append('street_address')
            str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('street_address'), 'ic'))
        if (field_column_mappings.get('unit')):
            str_insert.append('unit')
            str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('unit'), 'ic'))
        if ( field_column_mappings.get('city')):
            str_insert.append('city')
            str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('city'), 'ic'))
        if (field_column_mappings.get('state')):
           str_insert.append('state')
           str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('state'), 'ic'))
        if (field_column_mappings.get('zip')):
           str_insert.append('zip')
           str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('zip'), 'ic'))
        #if (field_column_mappings.get('employee_state')):
        #   str_insert.append('subscriber_state')
        #   str_select.append("ic." + field_column_mappings.get('employee_state'))
        #if ( field_column_mappings.get('employee_zip_code')):
        #   str_insert.append('subscriber_zip')   
        #   str_select.append("ic." + field_column_mappings.get('employee_zip_code'))
        ## Query to Select the data after joining with staged claims with master claims table and insert into provider_exception_table.
        insert_claim_prv_excep = """insert ignore into claim_provider_exceptions%s( %s )
                                     select %s
                                        from claims%s c,
                                             %s ic
                                        where c.imported_claim_file_id = ic.imported_claim_file_id
                                          and c.imported_claim_id = ic.id
                                          and c.imported_claim_file_id = %s
                                          and c.provider_id = -1 """   %(self.claims_table_suffix, ','.join(str_insert), ','.join(str_select), self.claims_table_suffix, self.stage_claim_table,self.imported_claim_file_id)
        cur.execute(insert_claim_prv_excep) 
        cur.close()
        ##For Testing print and in production uncomment
        ##print insert_claim_prv_excep

    def clear_claims(self, logger = None):
        if (logger):
            logutil.log(logger, logutil.INFO, "Clearing claim_attributes, claim_specialties, claims for imported_claim_file_id: %s" % self.imported_claim_file_id)
#        cur = self.conn.cursor()
        
        clear_ca = """DELETE ca.* 
                            FROM %s.claims%s c, 
                                 %s.claim_attributes%s ca
                           WHERE c.imported_claim_file_id = %s
                             AND c.id = ca.claim_id""" % (whcfg.claims_master_schema,
                                                          self.claims_table_suffix,
                                                          whcfg.claims_master_schema,
                                                          self.claims_table_suffix,
                                                          self.imported_claim_file_id)
#        cur.execute(clear_ca)
 
        clear_cs = """DELETE cs.* 
                            FROM %s.claims%s c, 
                                 %s.claim_specialties%s cs
                           WHERE c.imported_claim_file_id = %s
                             AND c.id = cs.claim_id""" % (whcfg.claims_master_schema,
                                                          self.claims_table_suffix,
                                                          whcfg.claims_master_schema, 
                                                          self.claims_table_suffix,
                                                          self.imported_claim_file_id)
#        cur.execute(clear_cs)
        
        clear_claims = """DELETE c.* 
                            FROM %s.claims%s c
                           WHERE c.imported_claim_file_id = %s""" % (whcfg.claims_master_schema,
                                                          self.claims_table_suffix,
                                                          self.imported_claim_file_id)
#        cur.execute(clear_claims)
        
        update_icf = """UPDATE %s.imported_claim_files 
                           SET normalized = 0,
                               has_eligibility = 0
                         WHERE id = %s""" % (whcfg.claims_master_schema,
                                             self.imported_claim_file_id)
#        cur.execute(update_icf)
        
        utils.execute_queries(self.conn, logger, [{'query':clear_ca,
                                                   'description':'Clearing claim_attributes%s for imported_claim_file_id: %s.' % (self.claims_table_suffix, self.imported_claim_file_id),
                                                   'warning_filter':'error'},
                                                  {'query':clear_cs,
                                                   'description':'Clearing claim_specialties%s for imported_claim_file_id: %s.' % (self.claims_table_suffix, self.imported_claim_file_id),
                                                   'warning_filter':'error'},
                                                  {'query':clear_claims,
                                                   'description':'Clearing claims%s for imported_claim_file_id: %s.' % (self.claims_table_suffix, self.imported_claim_file_id),
                                                   'warning_filter':'error'},
                                                  {'query':update_icf,
                                                   'description':'Updating imported_claim_files entry for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                   'warning_filter':'error'}])

    def augment_claim_participations(self, logger = None):
        #if self.load_properties and self.load_properties.get('external_id_type','').lower() == 'npi': 
        if self.load_properties and self.load_properties.get('external_id_type'):
            logutil.log(LOG if not logger else logger, logutil.INFO, 'Augmenting Provider Participations with addresses from claims.')
            claims_util.refresh_claim_participations_generic(whcfg.claims_master_schema, [str(self.imported_claim_file_id)], LOG if not logger else logger, False, self.load_properties.get('external_id_type'))
            
    def augment_claim_participations_provider_location(self, logger = None):
        if self.load_properties and self.load_properties.get('external_id_type'):
            logutil.log(LOG if not logger else logger, logutil.INFO, 'Augmenting Provider Participations with addresses from claims.')
            claims_util.refresh_claim_participations_provider_location(whcfg.claims_master_schema, [str(self.imported_claim_file_id)], LOG if not logger else logger, False, self.load_properties.get('external_id_type'))
        

    def claim_provider_exception(self):
         map_load_properties = self.load_properties
         self.insert_claim_provider_exceptions(map_load_properties)
         return None

    def match_claim_providers(self, logger = None):
        logutil.log(LOG if not logger else logger, logutil.INFO, 'Calling matcher for unresolved providers')
        claims_util.match_claim_providers(whcfg.claims_master_schema, [str(self.imported_claim_file_id)], LOG if not logger else logger)
