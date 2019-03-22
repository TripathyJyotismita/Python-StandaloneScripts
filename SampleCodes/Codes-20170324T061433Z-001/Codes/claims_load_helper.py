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
import name_utils

LOG = logutil.initlog('claims')
st = Stats("claims_load_helper")

# TODO: Extend for stats and logging
""" Known Custom Factories """
helpers = {"aetna": lambda conn, p_conn, icf, bm, load_properties, user_ids, dry_run: AetnaClaimsLoader(conn, p_conn, icf, bm, load_properties, user_ids, dry_run),
           "cigna": lambda conn, p_conn, icf, bm, load_properties, user_ids, dry_run: CignaClaimsLoader(conn, p_conn, icf, bm, load_properties, user_ids, dry_run),
           "bcbsma-presales": lambda conn, p_conn, icf, bm, load_properties, user_ids, dry_run: BcbsmaClaimsLoader(conn, p_conn, icf, bm, load_properties, user_ids, dry_run),
           "bcbsal-presales": lambda conn, p_conn, icf, bm, load_properties, user_ids, dry_run: BcbsalClaimsLoader(conn, p_conn, icf, bm, load_properties, user_ids, dry_run),
           "generic": lambda conn, p_conn, icf, bm, load_properties, user_ids, dry_run: GenericClaimsLoader(conn, p_conn, icf, bm, load_properties, user_ids, dry_run)
           }

FALLBACK_DRG_TYPES = {'cigna':'CMS_DRG',
             'aetna':'MS_DRG',
             'bcbsma':'AP_DRG',
             'horizon':'AP_DRG',
             'bcbsnc':'AP_DRG',
             'premera':'AP_DRG'
             }

FIELD_MAPPINGS = {"aetna":{
                            'provider_pin':'servicing_provider_pin',
                            'provider_name':'servicing_provider_name_last_or_full',
                            'provider_type':'servicing_provider_type',
                            'procedure_code':'line_level_procedure_code',
                            'procedure_code_type':'line_level_procedure_code_type',
                            'procedure_code_modifier':'line_level_procedure_code_modifier',
                            'ub92_revenue_center':'ub92_revenue_center',
                            'street_address':'servicing_provider_street_address_1',
                            'unit':'servicing_provider_street_address_2',
                            'city':'servicing_provider_city',
                            'state':'servicing_provider_state',
                            'zip':'servicing_provider_zip_code',
                            'provider_network_id':'servicing_provider_network_id',
                            'imported_claim_id':'id',
                            'imported_claim_file_id':'imported_claim_file_id',
                            'type_of_service':'type_of_service',
                            'place_of_service':'place_of_service',
                            'units_of_service':'numberunits_of_service',
                            'service_begin_date':'date_service_started',
                            'service_end_date':'date_service_stopped',
                            'payment_date':'date_processed_all',
                            'charged_amount':'net_submitted_expense',
                            'allowed_amount':'allowed_amount',
                            'savings_amount':'savings_negotiated_fee',
                            'cob_amount':'cob_paid_amount',
                            'coinsurance_amount':'coinsurance',
                            'deductible_amount':'deductible_amount',
                            'paid_amount':'paid_amount',
                            'copay_amount':'copayment_amount',
                            'out_of_network_indicator':'preferred_vs_non_preferred_benefit_level',
                            'national_drug_code':'national_drug_code',
                            'member_first_name':'member_first_name',
                            'member_last_name':'member_last_name',
                            'member_ssn':{'formula': "RIGHT(member_ssn, 9)"},
                            'member_dob':'member_date_of_birth',
                            'member_gender':'member_gender',
                            'member_id':'member_cumbid',
                            'member_relationship':'member_relationship_to_employee',
                            'employee_first_name':'employee_first_name_or_initial',
                            'employee_last_name':'employee_last_name',
                            'employee_ssn':{'formula': "RIGHT(employee_ssn, 9)"},
                            'employee_state':'employee_state',     ##WHSE-600
                            'employee_zip_code':'employee_zip_code',
                            'provider_specialty_code':'servicing_provider_specialty_code',
                            'not_covered_amount':'not_covered_amount_1',
                            'hra_amount':'aetna_health_fund_payable_amount',
                            'provider_tax_id':'servicing_provider_tax_id_number_tin',
                            'diagnosis_related_group':'diagnosis_related_group_drg',
                            'diagnosis_code_1':'diagnosis_code_1',
                            'diagnosis_code_2':'diagnosis_code_2',
                            'diagnosis_code_3':'diagnosis_code_3',
                            'diagnosis_code_4':'diagnosis_code_4',
                            'source_claim_id':'traditional_claim_id',
                            'source_claim_line_number':'expensepay_line_number',               
                            #'member_relationships':{'subscriber': ['E','M']},             
                            },
                  "cigna":{'member_id':'MBR_NUM',
                            'member_first_name':'FRST_NM',
                            'member_last_name':'LAST_NM',
                            'member_dob':'BRTH_DT',
                            'member_gender':'SEX_CD',
                            'employee_zip_code':'EMP_ZIP',
                            'employee_state':'EMP_ST',
                            'employee_ssn':{'formula': "LEFT(MBR_NUM, 9)"},
                            'member_age':'AGE_AMT',
                            'member_relationship':'PAT_REL',
                            'provider_tax_id':'PROV_ID',
                            'provider_npi':'NPI',
                            'provider_name':'PROV_NM',
                            'street_address':'PROV_ADDR',
                            'unit':'PROV_ADDR2',
                            'city':'PROV_CITY',
                            'state':'PROV_ST',
                            'zip':'PROV_ZIP',
                            'provider_type':'PROV_TY',
                            'provider_specialty_code':'PROVSPEC',
                            'payment_date':'PD_DT',
                            'service_begin_date':'SVC_DT',
                            'service_end_date':'END_DT',
                            'charged_amount':'CHRG_AMT',
                            'not_covered_amount':'NOT_COV',
                            'savings_amount':'SAV_AMT',
                            'allowed_amount':'COV_AMT',
                            'deductible_amount':'DED_AMT',
                            'copay_amount':'COPAY',
                            'coinsurance_amount':'COIN_AMT',
                            'cob_amount':'COB_AMT',
                            'paid_amount':'PAY_AMT',
                            'hra_amount':'HRA_AMT',
                            'procedure_code_type':'PROC_TY',
                            'procedure_code':'PROC_CD',
                            'secondary_procedure_code_type':'PROC_TY_CLP',
                            'secondary_procedure_code':'PROC_CD_CLP',                            
                            'revenue_code':'REVENUE_CD',
                            'out_of_network_indicator':'NT_IND',
                            'units_of_service':'ADJUDICATED_SERVICE_COUNT',
                            'procedure_code_modifier':'MODIFIER',
                            'secondary_procedure_code_modifier':'PROC_MDFR_CD_CLP',
                            'provider_network_id':'SVC_PROV_NT_ID',
                            'type_of_service':'SVC_TY',
                            'place_of_service':'LOC_CD',
                            'national_drug_code':'DRG_CD',
                            'provider_pin':'CPF_ID',
                            'imported_claim_id':'id',
                            'imported_claim_file_id':'imported_claim_file_id',
                            'diagnosis_related_group':'DRG_CD',                            
                            'diagnosis_code_1':'DIAG_CD1',
                            'diagnosis_code_2':'DIAG_CD2',
                            'diagnosis_code_3':'DIAG_CD3',
                            'source_claim_id':'CLAIM_ID',
                            'source_claim_line_number':'LN_NUM',
                            #'member_relationships':{'subscriber': ['E']},
                            },
                  "bcbsma":{
                            'provider_pin':'source_provider_id',
                            'provider_name':'provider_name',
                            'provider_type':'provider_type',
                            'procedure_code':'procedure_code',
                            'procedure_code_type':'procedure_type',
                            'city':'provider_city_name',
                            'state':'provider_state',
                            'zip':'provider_zip_code',
                            'imported_claim_id':'id',
                            'imported_claim_file_id':'imported_claim_file_id',
                            'units_of_service':'number_of_services_count',
                            'service_begin_date':'incurred_date',
                            'payment_date':'paid_date',
                            'allowed_amount':'eligible_expense_sum',
                            'coinsurance_amount':'coinsurance_amount_sum',
                            'deductible_amount':'deductible_amount_sum',
                            'paid_amount':'benefits_paid_sum',
                            'copay_amount':'copayment_amount_sum',
                            'out_of_network_indicator':'network_indicator',
                            'member_age':'age',
                            'member_gender':'gender',
                            'member_id':'claimant_id',
                            'member_relationship':'claimant_relationship_code',
                            'employee_id':'subscriber_id',
                            'employee_zip_code':'person_zip_code',
                            'provider_specialty_code':'provider_specialty'
                            },
                  }

helper_instances = {}

def field_mappings():
    return FIELD_MAPPINGS

def update_imported_claim_file_status(conn, imported_claim_file_ids = None, claims_table_suffix = None):
    
    q_insert1 = ''
    q_insert2 = ''
    if imported_claim_file_ids and len(imported_claim_file_ids) > 0:
        str_imported_claim_file_ids = [str(v) for v in imported_claim_file_ids]
        q_insert1 = "icf.id IN (%s) AND " % (','.join(str_imported_claim_file_ids))
        q_insert2 = "AND imported_claim_file_id IN (%s)" % (','.join(str_imported_claim_file_ids))

    query_norm = """UPDATE imported_claim_files icf 
                       SET icf.normalized=1 
                     WHERE %s EXISTS (SELECT 1 FROM claims%s c WHERE icf.id=c.imported_claim_file_id)""" % (q_insert1, claims_table_suffix)
                     
    c = conn.cursor()                      
    c.execute(query_norm)
            
    query_elig_rate = """SELECT imported_claim_file_id, 
                                IF(100*sum(IF(user_id <> -1, 1, 0))/count(1) < 10, 0, 1) as has_eligibility
                           FROM claims%s, imported_claim_files icf 
                          WHERE imported_claim_file_id=icf.id %s 
                          GROUP BY imported_claim_file_id""" % (claims_table_suffix, q_insert2)
    results = c.execute(query_elig_rate)
    imported_claim_file_ids_update = [] 
    while (True):
        result = c.fetchone()
        if result == None:
            c.close()
            break
        update_query = """UPDATE imported_claim_files SET has_eligibility=%s WHERE id=%s""" % (result['has_eligibility'],result['imported_claim_file_id'])
        conn.cursor().execute(update_query)
    
    c.close()

class _Callable:
    def __init__(self, anycallable):
        self.__call__ = anycallable

class ClaimsLoaderFactory:

    def get_instance(claims_master_conn, provider_master_conn, imported_claim_file_id, batch_mode = True, user_ids = None, dry_run = False):

        if (claims_master_conn is not None):

            if not claims_master_conn in helper_instances:
                helper_instances[claims_master_conn] = {}
            if imported_claim_file_id in helper_instances[claims_master_conn]:
                return helper_instances[claims_master_conn][imported_claim_file_id]
            else:
                fac_imported_claim_files = ModelFactory.get_instance(claims_master_conn, 'imported_claim_files')
#                fac_imported_claim_files.table.select('id, insurance_company_id, employer_id, table_name, (SELECT name from %s.insurance_companies ic where ic.id = insurance_company_id) as insurance_company_name' % whcfg.prod_schema)
                fac_imported_claim_files.table.select('id, claim_file_source_name, claim_file_source_type, employer_id, table_name, load_properties')
                icf_entry = {'id':imported_claim_file_id}
                icf_entry = fac_imported_claim_files.find(icf_entry)
                load_properties_text = None
                if icf_entry:
                    claim_file_source_type = icf_entry['claim_file_source_type'].upper() 
                    icf_entry['is_payer'] = True if claim_file_source_type == 'PAYER' else False
                    load_properties_text = icf_entry['load_properties']
                    
                insurance_company_name = icf_entry['claim_file_source_name'].lower() if icf_entry and helpers.get(icf_entry['claim_file_source_name'].lower()) else 'generic'
                fac = helpers[insurance_company_name](claims_master_conn, provider_master_conn, icf_entry, batch_mode, load_properties_text, user_ids, dry_run)
                helper_instances[claims_master_conn][imported_claim_file_id] = fac
                return fac
#                else:
##                    Throw Exception
#                    return None

    get_instance = _Callable(get_instance)

class BaseClaimsLoader:

    def __init__(self, claims_master_conn, provider_master_conn, imported_claim_file_details, batch_mode = False, load_properties_text = None, user_ids = None, dry_run = False):

        self.dry_run = dry_run 
        self.user_ids_to_log = set() if not user_ids else user_ids
        
        self.batch_mode = batch_mode
        self.imported_claim_ids = []
        self.batched_claims = []
        self.batched_claim_attributes = []
        self.batched_claim_specialties = []
        self.batched_claim_subscriber_identifiers = []
        
        self.imported_claim_file_id = imported_claim_file_details['id']
        self.employer_id = imported_claim_file_details['employer_id']
        self.stage_claim_table = imported_claim_file_details['table_name']
        self.claims_table_suffix = ''
        
        self.conn = claims_master_conn
        self.prod_conn = provider_master_conn
        self.normalization_rules = {}
        self.external_specialty_sources = None
        self.external_specialties_map = {}
        self.provider_type_map = {}
        self.providers = {}

#        TODO:Check refereces to insurance_company_id and insurance_company_name
#        self.insurance_company_id = imported_claim_file_details['insurance_company_id']
#        self.insurance_company_name = imported_claim_file_details['claim_file_source_name']
        
        self.insurance_company_id = None
        self.insurance_company_name = None
        
        self.claim_file_source_name = imported_claim_file_details['claim_file_source_name']
        self.claim_file_source_type = imported_claim_file_details['claim_file_source_type']
        self.is_source_payer = imported_claim_file_details['is_payer']
        
#        load_properties_stream = None
#        
#        if load_properties:
#            load_properties_stream = open(load_properties, 'r')
                    
        self.load_properties = yaml.load(load_properties_text) if load_properties_text else None
        
        # A Single (key, value) per type cache that helps with faster processing
        # For e.g., since Claims are all grouped by provider_pin, we can avoid multiple
        # finds on the same provider while processing claims
        self.lru = {}

        self.types_of_service = None
        self.external_types_of_service = None

        self.places_of_service = None
        self.external_places_of_service = None        
        self.member_relationships = None
        
        self.external_procedure_code_types = None
        self.procedure_code_types = None
        self.procedure_code_type_values = None
        self.procedure_code_type_lengths  = {'1':5,'2':6,'3':4}
        self.procedure_codes = None
        self.procedure_code_to_type_map = None
        self.procedures = None
        self.procedure_code_modifiers = None
        self.insurance_networks = None
        
        self.access_privileges = None
        self.anti_transparency_providers = None
        
        if self.is_source_payer:
            # Expect to find insurance_company_id in imported_claim_file_insurance_companies
            query = """SELECT insurance_company_id 
                         FROM imported_claim_files_insurance_companies
                        WHERE imported_claim_file_id=%s""" % self.imported_claim_file_id
            results = Query(claims_master_conn, query)
            insurance_company_id = results.next().get('insurance_company_id') if results else None
            if not insurance_company_id:
                logutil.log(LOG, logutil.INFO, "Invalid imported_claim_file_id: %s passed. Exiting!" % self.imported_claim_file_id)
                sys.exit()
            else:
                self.insurance_company_id = insurance_company_id
        
        self.static_entries = yaml.load(open(whcfg.providerhome + '/import/common/static_provider_master_entries.yml', 'r')) if self.insurance_company_id else None
        bucket_details = self.__query_bucket_details()
        self.bucket_privileges = bucket_details['access_privileges_bitmask']
        self.bucket_id = bucket_details['id']
                
        self.__initialize__()
        
        self.users = {}
        self.patients = {}
        self.subscriber_patients = {}

        self.__query_procedure_code_types__()
        self.__query_external_procedure_code_types__()
        self.__query_procedure_codes__()
        self.__query_procedure_code_modifiers__()
        self.__query_procedure_labels__()
        self.__query_insurance_networks__()

        self.__query_types_of_service__()
        
        self.__query_places_of_service__()
        self.__query_access_privileges()
        
        self.payer_code_map = {} 
            
        # This section is to handle claims coming from sources other than an insurance_company.
        # We make an assumption that when the source of the claim file is the insurance_company itself, 
        # all claims belong only to this one Payer. If this assumption is not true or we run into a real
        # usecase where this assumption is broken, it can easily be changed here.
        if self.is_source_payer:
            
            # Create a single entry in payer_code_map and add a literal mapping in the norm_rules
            self.payer_code_map = {str(self.insurance_company_id):(self.insurance_company_id, self.claim_file_source_name)}
            self.normalization_rules['L']['payer_code'] = self.insurance_company_id
            
            # Remove any payer_code mapping from 'M' section of normalization rules
            self.normalization_rules['M'].pop('payer_code', None)
            
            # For backwards compatibility of Aetna and Cigna loaders until they are ported to the new design
            self.insurance_company_name = self.claim_file_source_name
            self.__query_anti_transparency_providers__()
            self.__query_external_types_of_service__()
            self.__query_external_places_of_service__()
        else:
            # Read Payer Map if specified in load_properties
            payer_map_properties = self.load_properties.get('payer_map',{}) if self.load_properties else {}
            
            for payer_name, payer_codes in payer_map_properties.iteritems():
                fac_insurance_companies = ModelFactory.get_instance(provider_master_conn, "insurance_companies")
                ic_entry = {'name':payer_name}
                ic_entry = fac_insurance_companies.find(ic_entry)
                if not ic_entry:
                    logutil.log(LOG, logutil.INFO, "Invalid Insurance Company Name: '%s' in payer_map section. Exiting!" % payer_name)
                if not isinstance(payer_codes, list):
                    payer_codes = [payer_codes] 
                for payer_code in payer_codes:
                    self.payer_code_map[str(payer_code).upper()] = (ic_entry['id'],ic_entry['name'])
            
            mapped_payer_code_set = set([str(key).strip().lstrip('0').upper() for key in self.payer_code_map.keys()]) 
            
            # Check for distinct payer_code values from xyz_imported_claims table
            payer_code_column_name = self.normalization_rules.get('M',{}).get('payer_code', None)
            if not payer_code_column_name:
                logutil.log(LOG, logutil.INFO, "payer_code column mapping not specified in load properties file. Exiting!")
                sys.exit()
            query = """SELECT DISTINCT %s as payer_code 
                         FROM %s 
                        WHERE imported_claim_file_id=%s""" % (payer_code_column_name, self.stage_claim_table, self.imported_claim_file_id)
            results = Query(claims_master_conn, query)
            claims_payer_code_set = set([result['payer_code'].strip().lstrip('0').upper() for result in results]) if results else set([])
            
            if not claims_payer_code_set.issubset(mapped_payer_code_set):
                unmapped_payer_codes = claims_payer_code_set - mapped_payer_code_set
                logutil.log(LOG, logutil.INFO, "Load properties file does not have mappings for the following codes: %s. Exiting!" % unmapped_payer_codes)
                sys.exit()

    def __load_claim_providers__(self):
        self.claim_providers = {}

    def __query_access_privileges(self):
        if self.access_privileges is None:
            ap_map = {}
            t_ap = Table(self.conn, '%s.access_privileges' % whcfg.master_schema)
            for ap in t_ap:
                ap_map[ap['name']] = ap['value']
            self.access_privileges = ap_map
        return self.access_privileges
    
    def __query_bucket_details(self): 
        if self.insurance_company_id and self.employer_id:
            bucket_details_q = """SELECT b.* FROM %s.buckets b, %s.bucket_mappings bm
                                       WHERE b.id=bm.bucket_id
                                         AND bm.insurance_company_id=%d
                                         AND bm.employer_id=%d
                                         AND bm.active_flag='ACTIVE'""" % (whcfg.master_schema, whcfg.master_schema, self.insurance_company_id, self.employer_id)
            bd_r = Query(self.conn, bucket_details_q)
            bucket_details = {'access_privileges_bitmask':1,
                              'id':-1}
            if bd_r:
                for bd in bd_r:
                    bucket_details = bd
            
        return bucket_details

    def __query_anti_transparency_providers__(self):
        if self.anti_transparency_providers is None:
            atp_q = """SELECT atp.* FROM anti_transparency_list atp, insurance_company_data_files icdf
                        WHERE icdf.insurance_company_id=%d
                          AND icdf.active_flag='ACTIVE'
                          AND atp.insurance_company_data_file_id=icdf.id
                          AND atp.bucket_id=%d""" % (self.insurance_company_id, self.bucket_id)
            atp_r = Query(self.conn, atp_q)
            
            atp_list = {'provider_tax_id':set(),
                        'provider_pin':set(),
                        'provider_network_id':set()
                        }
            for atp in atp_r:
                if atp.get('tax_id'):
                    atp_list.get('provider_tax_id').update(set([atp['tax_id']]))
                if atp.get('identifier') and atp.get('identifier_type') and atp.get('identifier_type').upper() == self.insurance_company_name.upper():
                    atp_list.get('provider_pin').update(set([atp['identifier']]))
                if atp.get('identifier') and atp.get('identifier_type') and atp.get('identifier_type').upper() == 'NETWORK':
                    atp_list.get('provider_network_id').update(set([atp['identifier']]))
                
            self.anti_transparency_providers = atp_list

    def __query_procedure_code_modifiers__(self):
        if not self.procedure_code_modifiers:
            self.procedure_code_modifiers = {}
            fac_procedure_code_modifiers = ModelFactory.get_instance(self.conn, 'procedure_modifiers')
            for proc_modifier in fac_procedure_code_modifiers.table:
                self.procedure_code_modifiers[proc_modifier['code'].lower()] = proc_modifier['id']

    def __query_types_of_service__(self):
        if not self.types_of_service:
            self.types_of_service = {}
            fac_type_of_service = ModelFactory.get_instance(self.conn, 'service_types')
            for tos in fac_type_of_service.table:
                self.types_of_service[tos['code'].lower()] = tos['id']
        return self.types_of_service

    def __query_external_types_of_service__(self):
        if not self.external_types_of_service:
            self.external_types_of_service = {}
            fac_external_type_of_service = ModelFactory.get_instance(self.conn, 'external_service_types')
            for etos in fac_external_type_of_service.table:
                if (not self.external_types_of_service.get(self.insurance_company_id)):
                    self.external_types_of_service[self.insurance_company_id] = {}
                self.external_types_of_service[self.insurance_company_id][etos['code']] = etos['service_type_id']
        return self.external_types_of_service

    def __query_places_of_service__(self):
        if not self.places_of_service:
            self.places_of_service = {}
            fac_place_of_service = ModelFactory.get_instance(self.conn, 'service_places')
            for tos in fac_place_of_service.table:
                self.places_of_service[tos['code'].lower()] = tos['id']
        return self.places_of_service

    def __query_insurance_companies__(self):
        if not self.insurance_companies:
            self.insurance_companies = {}
            fac_insurance_companies = ModelFactory.get_instance(self.conn, '%s.insurance_companies' % whcfg.master_schema)
            for ic in fac_insurance_companies.table:
                self.insurance_companies[ic['name'].lower()] = ic['id']
        return self.insurance_companies

    def __query_external_places_of_service__(self):
        if not self.external_places_of_service:
            self.external_places_of_service = {}
            fac_external_place_of_service = ModelFactory.get_instance(self.conn, 'external_service_places')
            for epos in fac_external_place_of_service.table:
                if epos['insurance_company_id'] != self.insurance_company_id: continue
                if (not self.external_places_of_service.get(self.insurance_company_id)):
                    self.external_places_of_service[self.insurance_company_id] = {}
                self.external_places_of_service[self.insurance_company_id][epos['code'].lower()] = epos['service_place_id']
        return self.external_places_of_service

    #Builds a map as follows:
    #{'cpt':1,''}
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
            self.external_procedure_code_types = {1:{},2:{}}
            fac_external_procedure_code_types = ModelFactory.get_instance(self.conn, 'external_procedure_code_types')
            for epct in fac_external_procedure_code_types.table:
                if (epct['procedure_code_type_id']):
                    if (not self.external_procedure_code_types.get(epct['insurance_company_id'])):
                        self.external_procedure_code_types[epct['insurance_company_id']] = {}
                    self.external_procedure_code_types[epct['insurance_company_id']][epct['name'].lower()] = epct['procedure_code_type_id']
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

    def __query_insurance_networks__(self):
        tab_insurance_networks = Table(self.prod_conn, "insurance_networks")

        self.insurance_networks = {1:{},2:{}}
        for ir in tab_insurance_networks:
            i = tab_insurance_networks.row_to_dict(ir)
            if (i['active_flag'].lower() == 'active'):
                if not self.insurance_networks.get(i['insurance_company_id']): self.insurance_networks[i['insurance_company_id']] = {}
                if i['external_network_id']:
                    self.insurance_networks[i['insurance_company_id']][i['external_network_id']] = str(i['id'])
                else:
                    self.insurance_networks[i['insurance_company_id']][i['name']] = str(i['id'])
        return self.insurance_networks

    def __query_external_specialties_map__(self):
        return self.external_specialties_map

    def __initialize__(self):
        return None

    def __get_lru__(self, id, type, default_value = None):

        if self.lru.get(type):
            return self.lru.get(type).get(id) if self.lru.get(type).get(id) else default_value

    def __put_lru__(self, id, type, value):

        if not (self.lru.get(type) and self.lru.get(type).get(id)):
            self.lru[type] = {id:value}


    def __normalize_claim__(self, claim, filter = None):
        if claim.get('normalized'):
            return claim
        else:
            normalized_claim = {'normalized':True, 'parse_status':1, 'parse_comment':'', 'access_privileges':self.access_privileges['GENERAL']}
        if not filter:
            filter = self.normalization_rules.get('M').keys()
        for key in filter:
            map_key = self.normalization_rules.get('M').get(key)
            if (isinstance(map_key, dict)):
                norm_value = {}
                for map_key_key, map_key_map_key in map_key.items():
                    if map_key_key == 'formula':
                        formula_query = """SELECT %s 
                                        FROM %s 
                                        WHERE imported_claim_file_id = %s
                                        AND id = %s""" % (map_key_map_key,self.stage_claim_table,self.imported_claim_file_id,claim.get('id'))
                        formula_result = None
                        try:
                            formula_result = Query(self.conn, formula_query)
                        except Warning as e:
                            print "WARNING: MySQL Warning during formula select: %s" % e
                            norm_value = None
                        if formula_result:
			    formula_result_value = formula_result.next()
                            norm_value = formula_result_value.get(map_key_map_key) 
                    else:                    
                        norm_value[map_key_key] = claim.get(map_key_map_key)
                normalized_claim[key] = norm_value
            else:
                if map_key and re.match(r"^'(.)*'$",map_key):
                    normalized_claim[key] = map_key.strip("'")
                else:
                    normalized_claim[key] = claim.get(map_key)
                if key == 'provider_name':
                    normalized_claim[key] = utils.format_provider_name(normalized_claim[key])
            
            # Initialize amount fields to 0 if None
            if key[-6:].lower() == 'amount' and normalized_claim[key] is None:
                normalized_claim[key] = 0
        
        normalized_claim['source_claim_number'] = normalized_claim.get('source_claim_id')        

        for key, value in self.normalization_rules.get('L').items():
            normalized_claim[key] = value
        
        if normalized_claim.get('diagnosis_code_1'):
            dc = normalized_claim['diagnosis_code_1'].strip()
            normalized_claim['diagnosis_code_1'] = dc if (dc.find('.') > 0 or len(dc) <= 3) else '%s.%s' % (dc[0:3],dc[3:])
        if normalized_claim.get('diagnosis_code_2'):
            dc = normalized_claim['diagnosis_code_2'].strip()
            normalized_claim['diagnosis_code_2'] = dc if (dc.find('.') > 0 or len(dc) <= 3) else '%s.%s' % (dc[0:3],dc[3:])
        if normalized_claim.get('diagnosis_code_3'):
            dc = normalized_claim['diagnosis_code_3'].strip()
            normalized_claim['diagnosis_code_3'] = dc if (dc.find('.') > 0 or len(dc) <= 3) else '%s.%s' % (dc[0:3],dc[3:])
        if normalized_claim.get('diagnosis_code_4'):
            dc = normalized_claim['diagnosis_code_4'].strip()
            normalized_claim['diagnosis_code_4'] = dc if (dc.find('.') > 0 or len(dc) <= 3) else '%s.%s' % (dc[0:3],dc[3:])
        normalized_claim['insurance_company_id'] = self.payer_code_map[str(normalized_claim['payer_code']).strip().lstrip('0').upper()][0]
        normalized_claim['external_id_type'] = self.payer_code_map[str(normalized_claim['payer_code']).strip().lstrip('0').upper()][1]
        
        self.__custom_normalization__(normalized_claim, claim, filter)
        
        if 'zip' in filter and normalized_claim['zip'] and len(normalized_claim['zip']) == 4:
            normalized_claim['zip'] = normalized_claim['zip'].zfill(5)
        return normalized_claim

    def __update_normalized_claim_status__(self, normalized_claim, status, comment):
        if (status == normalized_claim['parse_status']):
            normalized_claim['parse_comment'] += "|%s|" % comment
            normalized_claim['parse_comment'] = normalized_claim['parse_comment'].strip('|')
        else:
            normalized_claim['parse_status'] = status
            normalized_claim['parse_comment'] = comment

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
    
    def __custom_normalization__(self, normalized_claim, claim, filter):
        return True

#    def __lookup_npi_address__(self, claim):
#        # Look up based on npi and raw_claim_zip    
#        normalized_claim = self.__normalize_claim__(claim)
#        zip = normalized_claim.get('zip')
#        npi = normalized_claim.get('provider_pin')
#        
#        addr_lru_key = '|'.join([npi,zip])
#        npi_location_id = self.__get_lru__(addr_lru_key, 'location', -1) if len(addr_lru_key) > 0 else -1
#        if not npi_location_id:
#            cid = stat.start('Lookup NPI location', '%s' % time.time())
#            q_npi_location_id = """SELECT l.id
#                                   FROM %s.provider_external_ids pei
#                                   JOIN %s.providers_locations_networks pln USING (provider_id)
#                                   JOIN %s.locations l WHERE pln.location_id=l.id
#                                  WHERE pei.external_id='%s'
#                                    AND pei.external_id_type='NPI'
#                                    AND pln.network_id=-8888
#                                    AND pei.active_flag='ACTIVE'
#                                    AND pln.active_flag='ACTIVE'
#                                    AND l.zip='%s'""" % (whcfg.master_schema,
#                                                         whcfg.master_schema,
#                                                         whcfg.master_schema,
#                                                         npi,
#                                                         zip)
#            r_npi_location_id = Query(self.conn, q_npi_location_id)
#            if r_npi_location_id:
#                npi_location = r_npi_location_id.next()
#                npi_location_id = npi_location['id']
#            
#            stat.end(cid)
#
#        if npi_location_id:
#            if len(addr_lru_key) > 0: self.__put_lru__(addr_lru_key, 'location', npi_location_id)
#
#        return npi_location_id
#    
#    def __lookup_npi_address_old__(self, claim):
#        # Look up based on npi and raw_claim_zip    
#        normalized_claim = self.__normalize_claim__(claim)
#        zip = normalized_claim.get('zip')
#        npi = normalized_claim.get('provider_pin')
#        npi_address = None
#        if zip and npi:
#            npi_lookup = """SELECT street_address, street_address_2 as unit, city, state, zip
#                                FROM %s.npi_provider_locations
#                               WHERE npi=%s
#                                 AND zip='%s'""" % (whcfg.npi_schema, normalized_claim['provider_pin'], normalized_claim['zip'])
#            q_npi_lookup = Query(self.conn, npi_lookup)
#            if q_npi_lookup:
#                npi_address = q_npi_lookup.next()
#                npi_address['source'] = 'NPI'
#        
#        return npi_address

    def is_anti_transparency(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        if self.anti_transparency_providers:
            for k in self.anti_transparency_providers.keys():
                if normalized_claim.get(k) and normalized_claim.get(k) in self.anti_transparency_providers.get(k):
                    return True
        return False

    def __requires_host_plan_privileges(self, claim):
        # TODO: Revisit this to see if there is a better way to do it
        normalized_claim = self.__normalize_claim__(claim)
        if self.bucket_privileges & self.access_privileges.get('HOST_PLAN') == 0:
            ic_se = self.static_entries.get('insurance_companies').get('entries').get(self.insurance_company_id)
            if ic_se:
                state = ic_se.get('home_state')
                if state and normalized_claim.get('state') and state.strip().lower() != normalized_claim.get('state').strip().lower():
                    return True
        return False
                    
            
    def resolve_access_privilege(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        is_at_claim = self.is_anti_transparency(normalized_claim)
        requires_host_priv = self.__requires_host_plan_privileges(normalized_claim)
        access_privileges = self.access_privileges['GENERAL']
        if is_at_claim:
            access_privileges = access_privileges | self.access_privileges['ANTI_TRANSPARENCY']
        if requires_host_priv:
            access_privileges = access_privileges | self.access_privileges['HOST_PLAN']
        return access_privileges
    
    def resolve_out_of_network(self, claim):

        normalized_claim = self.__normalize_claim__(claim)
        return 1 if normalized_claim['out_of_network_indicator'] and (normalized_claim['out_of_network_indicator'].lower() == 'y' or normalized_claim['out_of_network_indicator'].lower() == 'o') else 0;
        
    def resolve_provider(self, claim):

        normalized_claim = self.__normalize_claim__(claim)
        
        if normalized_claim['external_id_type'] == 'npi':
            # In the case when npi is the external_id_type,
            # we'll resolve provider in a post-processing step
            # where new providers and participations may be created.
            return -1 
        
        if not normalized_claim['provider_pin']: return -1
        provider_pin = normalized_claim['provider_pin'].lstrip('0')
        if not provider_pin or len(provider_pin) == 0:
            return -1
        
        provider_id = self.providers.get(provider_pin, {}).get('provider_id', -1)
#        provider_id = self.__get_lru__('provider', provider_pin, -1)
        if (provider_id > 0):
            return provider_id

        p_ext_id_entry = {'external_id':provider_pin, 'external_id_type':normalized_claim['external_id_type']}
        if (normalized_claim.get('external_sub_id')):
            p_ext_id_entry['external_sub_id'] = normalized_claim.get('external_sub_id')
        fac_provider_external_ids = ModelFactory.get_instance(self.prod_conn, 'provider_external_ids')
        cid = st.start('provider_external_ids.find')
        p_ext_id_entry = fac_provider_external_ids.find(p_ext_id_entry)
        st.end(cid)

        if (p_ext_id_entry):
            provider_id = p_ext_id_entry['provider_id']
        else:
            fac_to_use = None
#                print 'WARNING: Unknown provider pin: %s' % (claim['servicing_provider_pin'])
#            parsed_provider = self.__parse_provider__(normalized_claim)
            parsed_provider = self.__parse_provider__(claim)
            provider_type = parsed_provider['provider_type']

            provider_entry = {'raw_name':parsed_provider['provider_name'],
                              'external_id':provider_pin,
                              'external_id_type':normalized_claim['external_id_type'],
                              'source':normalized_claim['source'],
                              'provider_type':provider_type
                              }
            if (normalized_claim.get('external_sub_id')):
                provider_entry['external_sub_id'] = normalized_claim.get('external_sub_id')

            if (provider_type == 'practitioner'):

                provider_entry.update({'first_name':name_utils.correctcase(parsed_provider.get('first_name')) if parsed_provider.get('first_name') else '',
                                          'last_name':name_utils.correctcase(parsed_provider.get('last_name')) if parsed_provider.get('last_name') else '',
                                          'middle_name':parsed_provider.get('initial'),
                                          'suffix':parsed_provider.get('suffix'),
                                          'medical_degrees':parsed_provider.get('medical_degree')
                                      })
                fac_to_use = ModelFactory.get_instance(self.prod_conn, "practitioners")

            elif (provider_type == 'facility'):

                provider_entry.update({'facility_name':name_utils.correctcase(parsed_provider['provider_name'])})
                fac_to_use = ModelFactory.get_instance(self.prod_conn, "facilities")

            elif (provider_type == 'group'):

                provider_entry.update({'group_name':name_utils.correctcase(parsed_provider['provider_name'])})
                fac_to_use = ModelFactory.get_instance(self.prod_conn, "groups")

#                print 'Creating %s with provider pin: %s' % (provider_entry['provider_type'], claim['servicing_provider_pin'])

            cid = st.start('fac_to_use.create')
            provider_entry = fac_to_use.create(provider_entry)
            st.end(cid)

            provider_id = provider_entry['provider_id']
#            self.__put_lru__(normalized_claim['provider_pin'], 'provider', provider_id)
            self.providers[provider_pin] = {'provider_id':provider_id}

            if normalized_claim['provider_specialty_code']: 
                fac_provider_specialties = ModelFactory.get_instance(self.prod_conn, 'providers_specialties')
                fac_provider_external_specialties = ModelFactory.get_instance(self.prod_conn, 'provider_external_specialties')
                timevalue = datetime.datetime.now()
                now = timevalue.isoformat(' ').split('.')[0]
                p_ext_s = {'provider_id':provider_id,
                           'external_specialty_code':normalized_claim['provider_specialty_code'],
                           'source':normalized_claim['source'],
                           'created_at':now,
                           'updated_at':now
                           }

                fac_provider_external_specialties.create(p_ext_s)

                specialty_ids = self.external_specialties_map.get(normalized_claim['provider_specialty_code'].lower())
                p_s_entries = []
                if specialty_ids:
                    for specialty_id in specialty_ids:
                        p_s = {'provider_id':provider_id,
                               'specialty_id':specialty_id,
                               'created_at':now,
                               'updated_at':now}
                        p_s_entries.append(p_s)

                    fac_provider_specialties.table.insert_multiple(p_s_entries)
        return provider_id

    def resolve_procedure_labels(self, claim):
        return {'procedure_label_id':self.resolve_procedure_label(claim),
                'secondary_procedure_label_id':self.resolve_procedure_label(claim, False)} 

    def resolve_procedure_label(self, claim, is_primary = True):

        normalized_claim = self.__normalize_claim__(claim)

#        procedure_code_id = self.__parse_procedure_code__(normalized_claim)
        external_procedure_code_info = self.__extract_procedure_code__(normalized_claim, is_primary)
        if not external_procedure_code_info or not external_procedure_code_info.get('external_procedure_code'):
            if is_primary:
                self.__update_normalized_claim_status__(normalized_claim, 0, "Procedure Code not specified in claim.")
            return -1
        
        procedure_code = external_procedure_code_info['external_procedure_code']
#        if (is_primary and not procedure_code):
#            self.__update_normalized_claim_status__(normalized_claim, 0, "Procedure Code not specified in claim.")
#            return -1
#            pprint.pprint(claim)

        external_procedure_code_type = external_procedure_code_info['external_procedure_code_type']
        procedure_code_type_id = None
        
        if not external_procedure_code_type:
            # if external_procedure_code_type is not set at this point, we should attempt to resolve it
            # based on the codes that already exist in the claims master
            valid_types = self.procedure_code_to_type_map.get(procedure_code.lower())
            if valid_types:
                if self.procedure_code_types['cpt'] in valid_types:
                    procedure_code_type_id = self.procedure_code_types['cpt']
                elif self.procedure_code_types['hcpc'] in valid_types:
                    procedure_code_type_id = self.procedure_code_types['hcpc']
                elif self.procedure_code_types['revenue'] in valid_types:
                    procedure_code_type_id = self.procedure_code_types['revenue']
                else:
                    # Pick randomly from set of valid types for this code if its neither cpt/hcpc/revenue
                    if len(valid_types) > 1:
                        # If there is more than one valid type make sure the unknown type is not considered
                        valid_types.discard('unknown')
                    procedure_code_type_id = valid_types.pop()        
            else:
                procedure_code_type_id = self.procedure_code_types['unknown']
#            self.__update_normalized_claim_status__(normalized_claim, 0, "Procedure Code Type not specified for external procedure code: %s" % procedure_code)
#            return -1
        else:
            procedure_code_type_id = self.external_procedure_code_types[self.insurance_company_id].get(external_procedure_code_type)
        
        procedure_code_length = self.procedure_code_type_lengths.get(str(procedure_code_type_id), 0)
        procedure_code = procedure_code.zfill(procedure_code_length)
        
        if (not procedure_code_type_id):
            if is_primary:
                self.__update_normalized_claim_status__(normalized_claim, 0, "Unknown Procedure Code Type specified in claim: %s" % external_procedure_code_type)
            return -1
#        unknown_procedure_code_type_id = self.procedure_code_types['unknown']

#        if (procedure_code_type_id == unknown_procedure_code_type_id):
#            return 0

# RETHINK THIS LOGIC
#        if (not procedure_code_type_id) or (procedure_code_type_id < 0):
#            # Try to see if there is a procedure that matches the code and get the procedure_code_type from it
#            proc_code_set = self.procedure_code_to_type_map.get(procedure_code)
#            if (proc_code_set and len(proc_code_set) == 1):
#                for proc_code_type_id in proc_code_set:
#                    procedure_code_type_id = proc_code_type_id
#
#            else:
#                print "SEVERE ERROR: No procedure_code_type %s on imported_claim_id %s, procedure_code %s" % (external_procedure_code_type, normalized_claim['imported_claim_id'], procedure_code)
#                sys.exit(2)

        if (not self.procedure_codes.get(procedure_code_type_id)):
            self.procedure_codes[procedure_code_type_id] = {}
        if (not self.procedure_code_to_type_map.get(procedure_code)):
            self.procedure_code_to_type_map[procedure_code] = set()
# PADDING
        procedure_code_id = self.procedure_codes.get(procedure_code_type_id).get(procedure_code)

        if not procedure_code_id:
            fac_procedure_codes = ModelFactory.get_instance(self.conn, 'procedure_codes')
            p_code_entry = {'code':procedure_code,'procedure_code_type_id':procedure_code_type_id}
            cid = st.start('procedure_codes.create')
            p_code_entry = fac_procedure_codes.create(p_code_entry)
            st.end(cid)
            procedure_code_id = p_code_entry['id']
            self.procedure_codes.get(procedure_code_type_id)[procedure_code] = procedure_code_id
            self.procedure_code_to_type_map[procedure_code].add(procedure_code_type_id)



        procedure_modifier_code = normalized_claim.get('procedure_code_modifier') if is_primary else normalized_claim.get('secondary_procedure_code_modifier')
        if procedure_modifier_code and not self.procedure_code_modifiers.get(procedure_modifier_code.lower()):
            # Create Procedure Modifier
            fac_procedure_code_modifiers = ModelFactory.get_instance(self.conn, 'procedure_modifiers')
            pm_entry = {'code':procedure_modifier_code}
            pm_entry = fac_procedure_code_modifiers.create(pm_entry)
            self.procedure_code_modifiers[procedure_modifier_code.lower()] = pm_entry['id']

        procedure_code_modifier_key = self.procedure_code_modifiers.get(procedure_modifier_code.lower(), -1) if procedure_modifier_code else -1

        procedure_modifiers = self.procedures.get(procedure_code_id)

        if (not procedure_modifiers):
            procedure_modifiers = {}
            self.procedures[procedure_code_id] = procedure_modifiers

        if procedure_modifiers.get(procedure_code_modifier_key):
            procedure_id = procedure_modifiers.get(procedure_code_modifier_key)
        else:
            fac_procedure_labels = ModelFactory.get_instance(self.conn, 'procedure_labels')

            p_entry = {'procedure_code_id':procedure_code_id,'procedure_code_type_id':procedure_code_type_id,'procedure_modifier_id':procedure_code_modifier_key}
            cid = st.start('procedures.create')
            p_entry = fac_procedure_labels.create(p_entry)
            st.end(cid)
            if (p_entry):
                procedure_id = p_entry.get('id')
                self.procedures[procedure_code_id][procedure_code_modifier_key] = p_entry.get('id')
            else:
                pprint.pprint('SEVERE ERROR: Creating procedure: %s' % p_entry)
                sys.exit(2)

        return procedure_id

    def resolve_provider_location(self, claim, stat):
        normalized_claim = self.__normalize_claim__(claim)
        
        dict_raw_address = self.__parse_raw_address__(normalized_claim)
        good_location = dict_raw_address.pop('good_location')
        is_billing = dict_raw_address.get('is_billing')
        if (not good_location or is_billing) and normalized_claim['external_id_type'] == 'npi':
            # This should not be necessary anymore since this will be handled
            # during refresh claim participations    
            
#            npi_location_id = self.__lookup_npi_address__(normalized_claim)
#            if npi_location_id: 
#                return npi_location_id
#            elif is_billing:
            return -1
        
#        dict_raw_address['street_address'] = normalized_claim.get('street_address')
#        dict_raw_address['unit'] = normalized_claim.get('unit')
#        dict_raw_address['city'] = normalized_claim['city']
#        dict_raw_address['state'] = normalized_claim['state']
#        dict_raw_address['zip'] = normalized_claim['zip']
#        dict_raw_address['source'] = normalized_claim['source'].rpartition('_')[0]
#        dict_raw_address['active_flag'] = 'active'
        
        addr_lru_key_list = [v for v in dict_raw_address.values() if v != {} and v != None and v != True and v != False]
        for v in addr_lru_key_list:
            if isinstance(v,dict):
                print v 
        addr_lru_key_list.sort()
        addr_lru_key = '|'.join(addr_lru_key_list)
        location_id = self.__get_lru__(addr_lru_key, 'location', -1) if len(addr_lru_key) > 0 else -1
        if (location_id > 0):
            return location_id

        master_location = None
        fac_locations = ModelFactory.get_instance(self.prod_conn, 'locations')
        cid = stat.start('locations.find_or_create', '%s' % time.time())
        try:
#            master_location = fac_locations.find_or_create(dict_raw_address)
            master_location = fac_locations.find_master(dict_raw_address)
            if not master_location:
                master_location = fac_locations.create(dict_raw_address)
        except:
            master_location = {}
            master_location['id'] = -1
        stat.end(cid)

        if master_location:
            location_id = master_location['id']
            if len(addr_lru_key) > 0: self.__put_lru__(addr_lru_key, 'location', location_id)

        return location_id

    def resolve_patient(self, claim, filter = None):
        normalized_claim = self.__normalize_claim__(claim, filter)
#        pprint.pprint(normalized_claim) 
        return {'patient_id':-1,'user_id':-1}

    def hash_internal_member(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        return normalized_claim['internal_member_hash'] if normalized_claim.get('internal_member_hash') else None
    
    def resolve_place_of_service(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        place_of_service_id = self.places_of_service.get(normalized_claim.get('place_of_service'))
        if not place_of_service_id:
            return -1
        return place_of_service_id

    def resolve_type_of_service(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        type_of_service_id = self.types_of_service.get(normalized_claim.get('type_of_service'))
        if not type_of_service_id:
            return -1
        return type_of_service_id

    def resolve_provider_network(self, claim):

        normalized_claim = self.__normalize_claim__(claim)
        ext_network_id = normalized_claim.get('provider_network_id').strip().lstrip('0') if normalized_claim.get('provider_network_id') else ''
        network_id = self.insurance_networks[self.insurance_company_id].get(ext_network_id) if ext_network_id else None
        if (not network_id):
#            self.__update_normalized_claim_status__(normalized_claim, 0, "Unknown Provider Network in claim: %s" % normalized_claim.get('provider_network_id'))
            return -1
        return network_id

    def resolve_drg(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        return {'drg':normalized_claim.get('diagnosis_related_group'),
                'drg_type':(normalized_claim.get('drg_type') if normalized_claim.get('drg_type') else FALLBACK_DRG_TYPES.get(self.insurance_company_name.lower() if self.insurance_company_name else None, 'UNKNOWN')) if normalized_claim.get('diagnosis_related_group') else None}

    def process_claim(self, claim, st):
#        print claim['id']
        cid = st.start("__normalize_claim__", "__normalize_claim__")
        normalized_claim = self.__normalize_claim__(claim)
        st.end(cid)
        
        # STEP1: Provider
        cid = st.start("resolve_provider", "resolve_provider")
        provider_id = self.resolve_provider(normalized_claim)
        st.end(cid)
        
        # STEP2: Procedure
        cid = st.start("resolve_procedure_labels", "resolve_procedure_labels")
        procedure_label_ids = self.resolve_procedure_labels(normalized_claim)
        st.end(cid)

        procedure_label_id = procedure_label_ids.get('procedure_label_id')
        secondary_procedure_label_id = procedure_label_ids.get('secondary_procedure_label_id')

        #STEP3: patient_id
        cid = st.start("resolve_patient", "resolve_patient")
        patient_info = self.resolve_patient(normalized_claim)
        patient_id = patient_info['patient_id']
        user_id = patient_info.get('user_id', -1)
        subscriber_patient_id = patient_info['subscriber_patient_id']
        
        if self.user_ids_to_log and (user_id in self.user_ids_to_log):
            print "imported_claim_id:%s,user_id:%s,patient_id:%s" % (normalized_claim['imported_claim_id'], user_id, patient_id)
        st.end(cid)

        # Possibly generate Exception record here or can also be generated in the end.

        #STEP4:provider_location_id
        cid = st.start("resolve_provider_location", "resolve_provider_location")
        location_id = self.resolve_provider_location(normalized_claim, st)
        st.end(cid)
 
        #STEP5:network_id
        cid = st.start("resolve_provider_network", "resolve_provider_network")
        network_id = self.resolve_provider_network(normalized_claim)
        st.end(cid)

        cid = st.start("resolve_place_of_service", "resolve_place_of_service")
        place_of_service_id = self.resolve_place_of_service(normalized_claim)
        st.end(cid)
        
        cid = st.start("resolve_type_of_service", "resolve_type_of_service")
        type_of_service_id = self.resolve_type_of_service(normalized_claim)
        st.end(cid)

        out_of_network = self.resolve_out_of_network(normalized_claim)
#        out_of_network = 1 if normalized_claim['out_of_network_indicator'] and normalized_claim['out_of_network_indicator'] and normalized_claim['out_of_network_indicator'].lower() == 'y' else 0;
#        inpatient = 1 if normalized_claim.get('place_of_service') and normalized_claim['place_of_service'].lower() == 'i' else 0;
        
#        place_of_service_code = normalized_claim.get('place_of_service').lower() if normalized_claim.get('place_of_service') else None
#        place_of_service_id = self.external_places_of_service.get(place_of_service_code, -1) if (self.external_places_of_service and len(self.external_places_of_service) > 0) else self.places_of_service.get(place_of_service_code, -1)
#        inpatient = 1 if place_of_service_id == 21 else 0
        
        #STEP6:inpatient
        inpatient = 1 if (normalized_claim.get('diagnosis_related_group') or int(place_of_service_id) == 21) else 0

        #STEP7:internal_member_hash
        cid = st.start("hash_internal_member", "hash_internal_member")
        internal_member_hash = self.hash_internal_member(normalized_claim)
        st.end(cid)
         
        timevalue = datetime.datetime.now()
        now = timevalue.isoformat(' ').split('.')[0]

        claim_literals = { 'provider_id':provider_id,
                           'procedure_label_id':(procedure_label_id if procedure_label_id else -1),
                           'secondary_procedure_label_id':(secondary_procedure_label_id if secondary_procedure_label_id else -1),
                           'patient_id':patient_id,
                           'subscriber_patient_id':subscriber_patient_id,
                           'provider_location_id':location_id,
                           'insurance_network_id':network_id,
                           'out_of_network':out_of_network,
                           'inpatient':inpatient,
                           'service_place_id':place_of_service_id,
                           'service_type_id':type_of_service_id,
                           'user_id':user_id,
                           'internal_member_hash': internal_member_hash,
                           'imported_at':now
                           }
        
        drg_info = self.resolve_drg(normalized_claim)
        for k,v in drg_info.iteritems():
            claim_literals[k] = v.strip() if v else None 
            
        fac_claims = ModelFactory.get_instance(self.conn, 'claims')
        claim_entry = fac_claims.table.filledrow(normalized_claim, literal_values = claim_literals)

        # SHA1 the member_id just before persisting the claim
        if claim_entry['member_id'] and len(claim_entry['member_id'].strip()) > 0:
            claim_entry['member_id'] = hashlib.sha1(claim_entry['member_id'].strip()).hexdigest()
        else:
            claim_entry['member_id'] = ''
        
        claim_entry['access_privileges'] = self.resolve_access_privilege(claim)
#        if (claim_entry['member_id']): # Removing member_id check. We are getting a handful of claims that do not have member_id
        cid = st.start('claims.create')
        claim_id = self.__create_claim__(claim_entry, claim)
        st.end(cid)

#            cid = st.start('claim_attributes.create')
#            self.__create_claim_attributes__(claim_id, claim)
#            st.end(cid)
    
    def augment_claim_participations(self, logger = None):
        return None
    ## Start WHSE-600
    def claim_provider_exception(self): 
        return None
   
    def insert_claim_provider_exceptions(self,map_load_properties,logger = None):
        logger = LOG if not logger else logger
	logutil.log(logger, logutil.INFO, "Inserting data into claim_provider_exceptions table")
        field_column_mappings = map_load_properties.get('field_column_mappings')
        t_table_name = Table(self.conn, self.stage_claim_table)
        ## Preparing insert columns names
        str_insert =['claim_id' ,'imported_claim_file_id' ,'source_claim_number' , 'source_claim_line_number' , 'employer_id' , 'insurance_company_id' ,'created_date']
        ## Preparint select columns names from the join
        str_select =['c.id','c.imported_claim_file_id' ,'c.source_claim_number','c.source_claim_line_number','c.employer_id','c.insurance_company_id','now()']
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
        if (field_column_mappings.get('employee_state')):
           str_insert.append('subscriber_state')
           str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('employee_state'), 'ic'))
        if ( field_column_mappings.get('employee_zip_code')):
           str_insert.append('subscriber_zip')   
           str_select.append(claims_util.yaml_formula_insert(t_table_name, field_column_mappings.get('employee_zip_code'), 'ic'))
        ## Query to Select the data after joining with staged claims with master claims table and insert into provider_exception_table.
        insert_claim_prv_excep = """insert ignore into claim_provider_exceptions( %s )
                                     select %s
                                        from claims c,
                                             %s ic
                                        where c.imported_claim_file_id = ic.imported_claim_file_id
                                          and c.imported_claim_id = ic.id
                                          and c.imported_claim_file_id = %s
                                          and c.provider_id = -1 """   %( ','.join(str_insert), ','.join(str_select), self.stage_claim_table,self.imported_claim_file_id)
        utils.execute_queries(self.conn, logger, [{'query':insert_claim_prv_excep,
                                           'description':'Inserting data into claim_provider_exceptions table.',
                                           'warning_filter':'ignore'}])
        
        ##For Testing print and in production uncomment
        ##print insert_claim_prv_excep
    
    def validation_report(self, logger = None):
        validation_queries = {}
        return True

    def clear_claims(self, logger = None):
        if (logger):
            logutil.log(logger, logutil.INFO, "Clearing claim_attributes, claim_specialties, claim_subscriber_identifiers, claims for imported_claim_file_id: %s" % self.imported_claim_file_id)
#        cur = self.conn.cursor()
        
        clear_ca = """DELETE ca.* 
                            FROM %s.claims c, 
                                 %s.claim_attributes ca
                           WHERE c.imported_claim_file_id = %s
                             AND c.id = ca.claim_id""" % (whcfg.claims_master_schema,
                                                        whcfg.claims_master_schema,
                                                        self.imported_claim_file_id)
#        cur.execute(clear_ca)
 
        clear_cs = """DELETE cs.* 
                            FROM %s.claims c, 
                                 %s.claim_specialties cs
                           WHERE c.imported_claim_file_id = %s
                             AND c.id = cs.claim_id""" % (whcfg.claims_master_schema,
                                                        whcfg.claims_master_schema,
                                                        self.imported_claim_file_id)
#        cur.execute(clear_cs)
        
        clear_csi = """DELETE csi.* 
                            FROM %s.claims c,
                                 %s.claim_subscriber_identifiers csi
                           WHERE c.imported_claim_file_id = %s
                             AND c.id = csi.claim_id""" % (whcfg.claims_master_schema,
                                                        whcfg.claims_master_schema,
                                                        self.imported_claim_file_id)

        clear_claims = """DELETE c.* 
                            FROM %s.claims c
                           WHERE c.imported_claim_file_id = %s""" % (whcfg.claims_master_schema,
                                                        self.imported_claim_file_id)
#        cur.execute(clear_claims)
        
        update_icf = """UPDATE %s.imported_claim_files 
                           SET normalized = 0,
                               has_eligibility = 0
                         WHERE id = %s""" % (whcfg.claims_master_schema,
                                             self.imported_claim_file_id)
#        cur.execute(update_icf)
        
        utils.execute_queries(self.conn, logger, [{'query':clear_ca,
                                                   'description':'Clearing claim_attributes for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                   'warning_filter':'error'},
                                                  {'query':clear_cs,
                                                   'description':'Clearing claim_specialties for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                   'warning_filter':'error'},
                                                  {'query':clear_csi,
                                                       'description':'Clearing claim_subscriber_identifiers for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                       'warning_filter':'error'}, 
                                                  {'query':clear_claims,
                                                   'description':'Clearing claims for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                   'warning_filter':'error'},
                                                  {'query':update_icf,
                                                   'description':'Updating imported_claim_files entry for imported_claim_file_id: %s.' % self.imported_claim_file_id,
                                                   'warning_filter':'error'}])
        
    def __update_user_reconciliation_table__(self):
        return None

    def __create_claim__(self, claim_entry, original_claim):
        self.batched_claims.append(claim_entry)
        self.imported_claim_ids.append(str(original_claim['id']))
        claim_id = len(self.batched_claims)
        self.__create_claim_attributes__(claim_id, original_claim)
        self.__create_claim_specialties__(claim_id, original_claim)
        self.__create_claim_subscriber_identifiers__(claim_id, original_claim)
        if not self.batch_mode:
            self.commit_batch()

    def commit_batch(self):
        fac_claims = ModelFactory.get_instance(self.conn, 'claims%s' % self.claims_table_suffix)
        fac_claim_attribs = ModelFactory.get_instance(self.conn, 'claim_attributes%s' % self.claims_table_suffix)
        fac_claim_specialties = ModelFactory.get_instance(self.conn, 'claim_specialties%s' % self.claims_table_suffix)
        fac_claim_subscriber_identifiers = ModelFactory.get_instance(self.conn, 'claim_subscriber_identifiers%s' % self.claims_table_suffix)

        if self.dry_run or len(self.batched_claims) == 0:
            return
        elif len(self.batched_claims) == 1:
            claim_id = fac_claims.table.insert(self.batched_claims[0], returnid = True, ignore_or_replace='ignore')
            if claim_id:
                for claim_attrib in self.batched_claim_attributes:
                    claim_attrib['claim_id'] = claim_id
                if len(self.batched_claim_attributes) > 0:
                    fac_claim_attribs.table.insert_multiple(self.batched_claim_attributes, ignore_or_replace='ignore')
                for claim_spec in self.batched_claim_specialties:
                    claim_spec['claim_id'] = claim_id
                if len(self.batched_claim_specialties) > 0:
                    fac_claim_specialties.table.insert_multiple(self.batched_claim_specialties, ignore_or_replace='ignore')
                for claim_subscriber_identifier in self.batched_claim_subscriber_identifiers:
                	claim_subscriber_identifier['claim_id'] = claim_id
                if len(self.batched_claim_subscriber_identifiers) > 0:
                	fac_claim_subscriber_identifiers.table.insert_multiple(self.batched_claim_subscriber_identifiers, ignore_or_replace='ignore')
        else:
#            pprint.pprint(self.batched_claims)
            fac_claims.table.insert_multiple(self.batched_claims)
#            print "SELECT id, imported_claim_id FROM claims WHERE imported_claim_id in (%s)" % ','.join(str(self.imported_claim_ids))
            inserted_ids = Query(self.conn, "SELECT min(id) as id, imported_claim_id FROM claims%s WHERE imported_claim_id in (%s) AND imported_claim_file_id = %s group by imported_claim_id" % (self.claims_table_suffix, ','.join(self.imported_claim_ids), self.imported_claim_file_id))
            inserted_id_map = dict([(str(v['imported_claim_id']), v['id']) for v in inserted_ids])
            for claim_attrib in self.batched_claim_attributes:
                idx = self.imported_claim_ids[claim_attrib['claim_id']-1]
                claim_attrib['claim_id'] = inserted_id_map[str(idx)]
            if len(self.batched_claim_attributes) > 0:
                fac_claim_attribs.table.insert_multiple(self.batched_claim_attributes, ignore_or_replace='ignore')
            for claim_spec in self.batched_claim_specialties:
                idx = self.imported_claim_ids[claim_spec['claim_id']-1]
                claim_spec['claim_id'] = inserted_id_map[str(idx)]
            if len(self.batched_claim_specialties) > 0:
                fac_claim_specialties.table.insert_multiple(self.batched_claim_specialties, ignore_or_replace='ignore')
            for claim_identifiers in self.batched_claim_subscriber_identifiers:
                idx = self.imported_claim_ids[claim_identifiers['claim_id']-1]
                claim_identifiers['claim_id'] = inserted_id_map[str(idx)]
            if len(self.batched_claim_subscriber_identifiers) > 0:
            	fac_claim_subscriber_identifiers.table.insert_multiple(self.batched_claim_subscriber_identifiers, ignore_or_replace='ignore')

        self.batched_claims = []
        self.imported_claim_ids = []
        self.batched_claim_attributes = []
        self.batched_claim_specialties = []
        self.batched_claim_subscriber_identifiers = []

    def __extract_procedure_code__(self, claim, is_primary = True):
        return None

    def __create_claim_attributes__(self, claim_id, claim):
        return None
    
    def __create_claim_subscriber_identifiers__(self, claim_id, claim):
        return None

    def __create_claim_specialties__(self, claim_id, claim):
        normalized_claim = self.__normalize_claim__(claim)
        if normalized_claim['provider_specialty_code']:
            specialty_ids = self.external_specialties_map.get(normalized_claim['provider_specialty_code'])
            if not specialty_ids: return
            for specialty_id in specialty_ids:
                claim_spec_entry = {'claim_id':claim_id,
                                    'specialty_id':specialty_id}
                self.batched_claim_specialties.append(claim_spec_entry)

class AetnaClaimsLoader(BaseClaimsLoader):

    def __initialize__(self):

        self.external_specialty_source = 'aetna'

        self.normalization_rules = {'M':FIELD_MAPPINGS['aetna'],
                                   'L':{
                                        'external_id_type':'AETNA',
                                        'source':'AETNA_CLAIM_%s' % (self.imported_claim_file_id),
                                        'insurance_company_id':self.insurance_company_id,
                                        'employer_id':self.employer_id,
                                        'external_sub_id':'0',
                                        'patient_identifier_type':'ssn'
                                        }
                                   }
        
        # ONLY FOR AENTA WALMART the patient_identifier_type is not SSN
        # Figure out a better way to do this
        if self.employer_id==36:
            self.normalization_rules['L']['patient_identifier_type'] = 'member_number'
        
        tab_external_specialties_map = Table(self.prod_conn, 'external_specialties_map')
        tab_external_specialties_map.search("source='%s'" % self.external_specialty_source)

        for ext_spec_entry in tab_external_specialties_map:
            if not self.external_specialties_map.get(ext_spec_entry['external_specialty_code'].lower()):
                self.external_specialties_map[ext_spec_entry['external_specialty_code'].lower()] = set([ext_spec_entry['specialty_id']])
            else:
                self.external_specialties_map[ext_spec_entry['external_specialty_code'].lower()].update(set([ext_spec_entry['specialty_id']]))
#        pprint.pprint(self.external_specialties_map)
        # Read Aetna Provider Types
        aetna_pt_ref_data = csv.reader(open(whcfg.providerhome + '/claims/import/reference_data/aetna_provider_types.csv', 'r'), delimiter = ',', quotechar = '"' )
        for aetna_pt in aetna_pt_ref_data:
            self.provider_type_map[aetna_pt[0].lower()] = aetna_pt[2]

    def resolve_patient(self, claim, filter = None):
        normalized_claim = self.__normalize_claim__(claim, filter)
        
        subscriber_ssn = normalized_claim['employee_ssn'] if normalized_claim['employee_ssn'] else None
        subscriber_first_name = normalized_claim['employee_first_name'].strip() if normalized_claim['employee_first_name'] else None
        subscriber_last_name = normalized_claim['employee_last_name'].strip() if normalized_claim['employee_last_name'] else None
        
        member_ssn = normalized_claim['member_ssn'] if normalized_claim['member_ssn'] else None
        member_first_name = normalized_claim['member_first_name'].strip() if normalized_claim['member_first_name'] else None
        member_dob = str(normalized_claim['member_dob']) if normalized_claim['member_dob'] else None
        member_last_name = normalized_claim['member_last_name'].strip() if normalized_claim['member_last_name'] else None
        
        member_to_employee_relationship = normalized_claim['member_relationship'].strip().upper() if normalized_claim['member_relationship'] else None
        
        is_relationship_available = True
        is_subscriber = member_to_employee_relationship == 'E' or member_to_employee_relationship == 'M'
        is_member_ssn_available = True
        is_subscriber_first_name_available = True
        
        pi = claims_util.PatientIdentifier()
        return_dict = {}
        if (self.normalization_rules['L']['patient_identifier_type'] == 'ssn'):
            return_dict = pi.resolve_generic_claim_patient(conn=self.conn,
                                                           subscriber_ssn=subscriber_ssn,
                                                           subscriber_first_name=subscriber_first_name, 
                                                           subscriber_last_name=subscriber_last_name,                                                         
                                                           member_ssn=member_ssn, 
                                                           member_first_name=member_first_name, 
                                                           member_dob=member_dob,
                                                           member_last_name=member_last_name, 
                                                           is_relationship_available=is_relationship_available,
                                                           is_subscriber=is_subscriber,
                                                           is_member_ssn_available=is_member_ssn_available,
                                                           is_subscriber_first_name_available=is_subscriber_first_name_available,
                                                           insurance_company_id=self.insurance_company_id,
                                                           employer_id=self.employer_id) 
        else:
            return_dict = pi.resolve_nonssn_claim_patient(conn=self.conn,
                                                           subscriber_identifier=subscriber_ssn,
                                                           subscriber_first_name=subscriber_first_name, 
                                                           subscriber_last_name=subscriber_last_name,                                                         
                                                           member_identifier=member_ssn, 
                                                           member_first_name=member_first_name, 
                                                           member_dob=member_dob,
                                                           member_last_name=member_last_name, 
                                                           is_relationship_available=is_relationship_available,
                                                           is_subscriber=is_subscriber,
                                                           is_member_identifier_available=is_member_ssn_available,
                                                           is_subscriber_first_name_available=is_subscriber_first_name_available,
                                                           insurance_company_id=self.insurance_company_id,
                                                           employer_id=self.employer_id,
                                                           identifier_type=self.normalization_rules['L']['patient_identifier_type'])        
        
        if return_dict.get('subscriber_patient_id') > -1:
            suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))
            if suppress_dependents:
                if return_dict.get('subscriber_patient_id') <> return_dict.get('patient_id'):
                    return_dict['patient_id'] = -1
                    return_dict['subscriber_patient_id'] = -1
                            
        # Pupulate user_id is subscriber_patient_id was resolvable. This code is temporary until the point Patient Master goes live.
#        return_dict['user_id'] = -1
#        if (return_dict and return_dict.get('subscriber_patient_id')):
#            user_entry = {'identity_patient_id':return_dict.get('subscriber_patient_id'), 'employer_id':self.employer_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
#            fac_users = ModelFactory.get_instance(self.conn, "users")
#            user_result = fac_users.find(user_entry)
#            if user_result:
#                return_dict['user_id'] = user_result['id']            
        
        return return_dict

    def resolve_patient_old(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        fac_users = ModelFactory.get_instance(self.conn, "users")
        fac_patients = ModelFactory.get_instance(self.conn, "patients")
        
        member_to_employee_relationship = normalized_claim['member_relationship']
        member_id = normalized_claim['member_id'] if normalized_claim['member_id'] else None
        patient_ssn = normalized_claim['member_ssn'] if normalized_claim['member_ssn'] else None
        employee_ssn = normalized_claim['employee_ssn'] if normalized_claim['employee_ssn'] else None

        # Given an employee_ssn we need the subscriber_patient_id, user_id
        # Given a member_id we need the patient_id
        # 
        # We are not guaranteed to have ssn for all dependents in the patients table 
        #        select count(1), ssn from patients group by ssn having count(1) > 2 order by count(1) desc;
        #        +----------+-----------+
        #        | count(1) | ssn       |
        #        +----------+-----------+
        #        |    33344 | NULL      |
        #        |      119 | 999999999 |
        #        +----------+-----------+
        # Therefore, we do not want to look-up the patients table using the member ssn, as we are not guaranteed
        # to be returned a result if the ssn of the dependent is not entered.
        #
        # We should instead be looking up for the patient_id of the subscriber by looking up the patients table
        # by employee_ssn
        # 
        # Also, there are a good number of records in patients that have the same ssn. However, only one of these
        # patient records has an entry in the policies table as subscriber_patient_id. 
        # Therefore, in order to look up the subscriber_patient_id we need to join the patients table with the 
        # policies table. TODO: We may need extensions to this when we start supporting multiple types of policies
        # and we somehow end up with 2 different policies for different subscriber_patient_id bug the same ssn - because
        # we may have accidentally created 2 footprints for the same person.
        # 
        # 
        
        patient_id = -1
        user_id = -1
        subscriber_patient_id = -1
        subscriber_patient = None
        return_dict = {'user_id':user_id,'patient_id':patient_id, 'subscriber_patient_id': subscriber_patient_id}
        
        if employee_ssn and employee_ssn in self.subscriber_patients:
            subscriber_patient = self.subscriber_patients[employee_ssn]
#            subscriber_patient_id = subscriber_patient['id']
        elif employee_ssn:
            q_subscribing_patient = """SELECT p.* FROM patients p,
                                              policies po
                                        WHERE p.ssn = %s
                                          AND po.subscriber_patient_id=p.id
                                        GROUP BY p.id"""
            
            subscriber_patient_result = Query(self.conn, q_subscribing_patient, (employee_ssn))
            if len(subscriber_patient_result) <> 1:
                return return_dict
            
            subscriber_patient = subscriber_patient_result.next()

        if subscriber_patient:
            self.subscriber_patients[employee_ssn] = subscriber_patient
            subscriber_patient_id = subscriber_patient['id']
#            self.patients[employee_ssn] = subscriber_patient_id
            
#            if patient_ssn and patient_ssn in self.patients:
#                # Check cache
#                patient_id = self.patients[patient_ssn]
#                
#            el
            if ((member_to_employee_relationship and (member_to_employee_relationship.strip() == 'E' or member_to_employee_relationship.strip() == 'M'))
                 or (patient_ssn == employee_ssn)):
                patient_id = subscriber_patient_id
#                if patient_ssn:
#                    self.patients[patient_ssn] = patient_id
            else:
                # Once we have the subscribing patient we can get the patient_id from policy_coverages

                q_patients = """SELECT p.*
                                  FROM policies po,
                                       policy_coverages pc,
                                       patients p
                                 WHERE po.subscriber_patient_id = %s
                                   AND pc.policy_id = po.id
                                   AND pc.patient_id = p.id
                                   AND p.id <> %s
                              GROUP BY p.id"""
                              
                patients_result = Query(self.conn, q_patients, (subscriber_patient_id, subscriber_patient_id))
                
                for patient in patients_result:
                    mfn = normalized_claim['member_first_name'].lower() if normalized_claim['member_first_name'] else None
                    pfn = patient['first_name'].lower() if patient['first_name'] else None
                    mln = normalized_claim['member_last_name'].lower() if normalized_claim['member_last_name'] else None
                    pln = patient['last_name'].lower() if patient['last_name'] else None
                    # Simple name comparison for now
                    if (mfn == pfn and mln == pln):
                        patient_id = patient['id']
#                        if patient_ssn:
#                            self.patients[patient_ssn] = patient_id
                        break
                    
            # Populate user_id
            # Population of user_id is not required as we will only need to care about subscriber_patient_id and patient_id.
            # We will populate the user_id of the primary subscriber for the timebeing. We should completely remove user_id
            # population once we have dependent access in place, as it will not be obvious which user_id to fill in here.
                
            user_result = None
            if employee_ssn and employee_ssn in self.users:
                user_result = self.users[employee_ssn]
                user_id = user_result['id']
            elif employee_ssn:
                user_entry = {'identity_patient_id':subscriber_patient_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
                user_result = fac_users.find(user_entry)
                if user_result:
                    if employee_ssn:
                        self.users[employee_ssn] = user_result
                    user_id = user_result['id']
                        
        return_dict['subscriber_patient_id'] = subscriber_patient_id
        return_dict['patient_id'] = patient_id
        return_dict['user_id'] = user_id
        
        return return_dict

    def hash_internal_member(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        if normalized_claim['member_first_name'] and normalized_claim['member_dob'] and normalized_claim['employee_ssn']:
            internal_member_hash = hashlib.sha1(normalized_claim['member_first_name'].lower() + str(normalized_claim['member_dob']) + normalized_claim['employee_ssn']).hexdigest() 
        else:
            internal_member_hash = None    
        return internal_member_hash
    
    def resolve_place_of_service(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        
        place_of_service_id = self.external_places_of_service.get(self.insurance_company_id).get(normalized_claim['place_of_service'].lower()) if normalized_claim.get('place_of_service') else None
        if not place_of_service_id:
            return -1
        return place_of_service_id

    def __parse_provider__(self, claim):

        normalized_claim = self.__normalize_claim__(claim)

        provider_name = normalized_claim.get('provider_name') if normalized_claim.get('provider_name') else ''
        parsed_provider = {'provider_name':provider_name}
        servicing_provider_type = normalized_claim['provider_type'].lower() if normalized_claim['provider_type'] else 'aaa'
        provider_type = 'group' if not self.provider_type_map.get(servicing_provider_type) else self.provider_type_map.get(servicing_provider_type)
        if provider_type == 'practitioner':
            try:
                if (provider_name.find(',') <= 0):
                    name_parts = provider_name.rsplit(None, 1)
                    provider_name = '%s, %s' % (name_parts[1],name_parts[0])
                parsed_name = parseName(provider_name)
                parsed_provider.update(parsed_name)
            except IndexError, e:
                provider_type = 'group'

        parsed_provider['provider_type'] = provider_type

        return parsed_provider

    def __extract_procedure_code__(self, claim, is_primary = True):
        
        if not is_primary:
            return None
        
        normalized_claim = self.__normalize_claim__(claim)

        external_procedure_code_type = None

        if normalized_claim['procedure_code_type']:
            external_procedure_code_type = normalized_claim['procedure_code_type'].lower()

        procedure_code = normalized_claim['procedure_code'].lower() if normalized_claim['procedure_code'] else None

        if (external_procedure_code_type == 'a' and procedure_code >= '00100' and procedure_code <= '01999'):
            external_procedure_code_type = 'c'

        if (procedure_code == 'a'):
            procedure_code = normalized_claim['ub92_revenue_center'].lower() if normalized_claim['ub92_revenue_center'] else None
            external_procedure_code_type = 'r'

        if normalized_claim['national_drug_code'] and procedure_code and (procedure_code.lower() == normalized_claim['national_drug_code'].lower()[0:5]):
            procedure_code = 'ndcrx'

        if (procedure_code == 'ndcrx'):
            procedure_code = normalized_claim['national_drug_code'].lower() if normalized_claim['national_drug_code'] else None
            external_procedure_code_type = 'n'

        procedure_code_info = {'external_procedure_code':procedure_code, 'external_procedure_code_type':external_procedure_code_type}
        return procedure_code_info

    def __create_claim_attributes__(self, claim_id, claim):
        for i in xrange(3):
            claim_attr_entry = {'claim_id':claim_id}
            if claim['action_or_reason_code_%d' % (i+1)] and len(claim['action_or_reason_code_%d' % (i+1)]) > 0:
                claim_attr_entry['name'] = 'action_or_reason_code_%d' % (i+1)
                claim_attr_entry['value'] = claim['action_or_reason_code_%d' % (i+1)]
                self.batched_claim_attributes.append(claim_attr_entry)
        for attr_name in ['adjustment_code', 'acas_pointer_back_to_previous_genseg', 'participating_provider_code']:
            if claim[attr_name] and len(claim[attr_name]) > 0:
                claim_attr_entry = {'claim_id':claim_id,
                                    'name':attr_name,
                                    'value':claim[attr_name]}
                self.batched_claim_attributes.append(claim_attr_entry)

    def __create_claim_subscriber_identifiers__(self, claim_id, claim):
        if claim['employee_ssn']:
            claim_identifier_entry = {'claim_id':claim_id, 'subscriber_identifier': claim['employee_ssn'][-9:]}
            self.batched_claim_subscriber_identifiers.append(claim_identifier_entry)
    
    def __custom_normalization__(self, normalized_claim, claim, filter):
        if self.employer_id==30: #T. Rowe Price override    
            normalized_claim['paid_amount'] = normalized_claim['paid_amount'] - normalized_claim['hra_amount'] 
        return True
    ## Start WHSE-600
    def claim_provider_exception(self):
       map_load_properties = {'field_column_mappings':FIELD_MAPPINGS.get('aetna')}
       self.insert_claim_provider_exceptions(map_load_properties) 
       return None
    ## END WHSE-600

class CignaClaimsLoader(BaseClaimsLoader):

    def __initialize__(self):

        self.external_specialty_source = 'cigna'
        
        if self.stage_claim_table == 'cigna_imported_claims_bob':
            self.claims_table_suffix = '_bob'
            
        self.normalization_rules = {'M':FIELD_MAPPINGS['cigna'],
                                   'L':{
                                        'external_id_type':'CIGNA',
                                        'source':'CIGNA_CLAIM_%s' % (self.imported_claim_file_id),
                                        'insurance_company_id':self.insurance_company_id,
                                        'employer_id':self.employer_id
                                        },
                                   'title_set':set(['ANP', 'ARNP', 'CC', 'CCDP', 'CFA', 'CNP', 'CM', 'CNM', 'CNIM', 'CP', 'CPO', 'CRNA', 'CRNFA', 'CRNP', 'CSA', 'CST', 'DC', 'DDS', 'DED', 'DMD', 'DPM', 'DPT', 'DO', 'FNP', 'LAC', 'LC', 'LCSW', 'LMHC', 'LMFT', 'LMSW', 'LPN', 'LPT', 'LSA', 'MA', 'MBBS', 'MD', 'MFCC', 'MFT', 'MPT', 'MS', 'MSN', 'MSPT', 'MSW', 'ND', 'NP', 'OD', 'OT', 'OTR', 'PA', 'PAC', 'PHD', 'PMH', 'PMHNP', 'PMHNPANP', 'PROF', 'PSYD', 'PT', 'RD', 'RN', 'RNFA', 'RPT', 'SLP']),
                                   'cbh_codes':set([('cn','gr','jc','pj','py','ss','sw','sy','yc','za')]),
                                   }

        tab_external_specialties_map = Table(self.prod_conn, 'external_specialties_map')
        tab_external_specialties_map.search("source='%s' AND external_specialty_code like '%sPhysician%s'" % (self.external_specialty_source,'%','%'))

        for ext_spec_entry in tab_external_specialties_map:
#            ext_sp_code = ext_spec_entry['external_specialty_code'].split(':')[1]
            ext_sp_code = ext_spec_entry['external_specialty_code']

            if not self.external_specialties_map.get(ext_sp_code.lower()):
                self.external_specialties_map[ext_sp_code.lower()] = set([str(ext_spec_entry['specialty_id'])])
            else:
                self.external_specialties_map[ext_sp_code.lower()].update(set([str(ext_spec_entry['specialty_id'])]))

    def resolve_out_of_network(self, claim):

        normalized_claim = self.__normalize_claim__(claim)
        return 0 if normalized_claim.get('out_of_network_indicator') and (normalized_claim['out_of_network_indicator'].lower() == 'i') else 1;

    def resolve_patient(self, claim, filter = None):
        normalized_claim = self.__normalize_claim__(claim, filter)
        
        employee_num = normalized_claim.get('employee_id')
        patient_num = normalized_claim.get('member_id') 
        subscriber_ssn = employee_num[0:9] if employee_num else patient_num[0:9] if patient_num else None
        
        member_first_name = normalized_claim['member_first_name'].strip() if normalized_claim['member_first_name'] else None
        member_dob = str(normalized_claim['member_dob']) if normalized_claim['member_dob'] else None
        member_last_name = normalized_claim['member_last_name'].strip() if normalized_claim['member_last_name'] else None
        
        member_to_employee_relationship = normalized_claim['member_relationship'].strip().upper() if normalized_claim['member_relationship'] else None
        
        is_relationship_available = True
        is_subscriber = member_to_employee_relationship == 'E'
        is_member_ssn_available = False
        is_subscriber_first_name_available = False
        
        pi = claims_util.PatientIdentifier()
        return_dict = pi.resolve_generic_claim_patient(conn=self.conn,
                                                       subscriber_ssn=subscriber_ssn,
                                                       subscriber_first_name=None, 
                                                       subscriber_last_name=None,                                                         
                                                       member_ssn=None, 
                                                       member_first_name=member_first_name, 
                                                       member_dob=member_dob,
                                                       member_last_name=member_last_name, 
                                                       is_relationship_available=is_relationship_available,
                                                       is_subscriber=is_subscriber,
                                                       is_member_ssn_available=is_member_ssn_available,
                                                       is_subscriber_first_name_available=is_subscriber_first_name_available,
                                                       insurance_company_id=self.insurance_company_id,
                                                       employer_id=self.employer_id)        
        
        if return_dict.get('subscriber_patient_id') > -1:
            suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))
            if suppress_dependents:
                if return_dict.get('subscriber_patient_id') <> return_dict.get('patient_id'):
                    return_dict['patient_id'] = -1
                    return_dict['subscriber_patient_id'] = -1        
        
        # Pupulate user_id is subscriber_patient_id was resolvable. This code is temporary until the point Patient Master goes live.
#        return_dict['user_id'] = -1
#        if (return_dict and return_dict.get('subscriber_patient_id')):
#            user_entry = {'identity_patient_id':return_dict.get('subscriber_patient_id'), 'employer_id':self.employer_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
#            fac_users = ModelFactory.get_instance(self.conn, "users")
#            user_result = fac_users.find(user_entry)
#            if user_result:
#                return_dict['user_id'] = user_result['id']            
        
        return return_dict
    
    def resolve_patient_old(self, claim):
        
        # TODO: Add employer_id to all lookups 
        
        normalized_claim = self.__normalize_claim__(claim)
        fac_users = ModelFactory.get_instance(self.conn, "users")
        fac_patients = ModelFactory.get_instance(self.conn, "patients")

        employee_num = normalized_claim.get('employee_id')
        patient_num = normalized_claim.get('member_id')        
        
        member_to_employee_relationship = normalized_claim['member_relationship']
        employee_ssn = employee_num[0:9] if employee_num else patient_num[0:9] if patient_num else None
        
        # Given an employee_ssn we need the subscriber_patient_id, user_id
        # Given a member_id we need the patient_id
        # 
        # We are not guaranteed to have ssn for all dependents in the patients table 
        #        select count(1), ssn from patients group by ssn having count(1) > 2 order by count(1) desc;
        #        +----------+-----------+
        #        | count(1) | ssn       |
        #        +----------+-----------+
        #        |    33344 | NULL      |
        #        |      119 | 999999999 |
        #        +----------+-----------+
        # Therefore, we do not want to look-up the patients table using the member ssn, as we are not guaranteed
        # to be returned a result if the ssn of the dependent is not entered.
        #
        # We should instead be looking up for the patient_id of the subscriber by looking up the patients table
        # by employee_ssn
        # 
        # Also, there are a good number of records in patients that have the same ssn. However, only one of these
        # patient records has an entry in the policies table as subscriber_patient_id. 
        # Therefore, in order to look up the subscriber_patient_id we need to join the patients table with the 
        # policies table. TODO: We may need extensions to this when we start supporting multiple types of policies
        # and we somehow end up with 2 different policies for different subscriber_patient_id bug the same ssn - because
        # we may have accidentally created 2 footprints for the same person.
        # 
        # 
        
        patient_id = -1
        user_id = -1
        subscriber_patient_id = -1
        subscriber_patient = None
        return_dict = {'user_id':user_id,'patient_id':patient_id, 'subscriber_patient_id': subscriber_patient_id}
        
        if employee_ssn and employee_ssn in self.subscriber_patients:
            subscriber_patient = self.subscriber_patients[employee_ssn]
#            subscriber_patient_id = subscriber_patient['id']
        elif employee_ssn:
            q_subscribing_patient = """SELECT p.* FROM patients p,
                                              policies po
                                        WHERE p.ssn = %s
                                          AND po.subscriber_patient_id=p.id
                                        GROUP BY p.id"""
            
            subscriber_patient_result = Query(self.conn, q_subscribing_patient, (employee_ssn))
            if len(subscriber_patient_result) <> 1:
                return return_dict
            
            subscriber_patient = subscriber_patient_result.next()

        if subscriber_patient:
            self.subscriber_patients[employee_ssn] = subscriber_patient
            subscriber_patient_id = subscriber_patient['id']
            
#            if patient_num and patient_num in self.patients:
#                # Check cache
#                patient_id = self.patients[patient_num]
#                
#            el
            if ((member_to_employee_relationship and member_to_employee_relationship.strip() == 'E')
                 or (patient_num == employee_num)):
                patient_id = subscriber_patient_id
#                if patient_num:
#                    self.patients[patient_num] = patient_id 
            else:
                # Once we have the subscribing patient we can get the patient_id from policy_coverages

                q_patients = """SELECT p.*
                                  FROM policies po,
                                       policy_coverages pc,
                                       patients p
                                 WHERE po.subscriber_patient_id = %s
                                   AND pc.policy_id = po.id
                                   AND pc.patient_id = p.id
                                   AND p.id <> %s
                              GROUP BY p.id"""
                              
                patients_result = Query(self.conn, q_patients, (subscriber_patient_id, subscriber_patient_id))
                
                for patient in patients_result:
                    mfn = normalized_claim['member_first_name'].lower() if normalized_claim['member_first_name'] else None
                    pfn = patient['first_name'].lower() if patient['first_name'] else None
                    mln = normalized_claim['member_last_name'].lower() if normalized_claim['member_last_name'] else None
                    pln = patient['last_name'].lower() if patient['last_name'] else None
                    # Simple name comparison for now
                    if (mfn == pfn and mln == pln):
                        patient_id = patient['id']
#                        if patient_num:
#                            self.patients[patient_num] = patient_id
                        break
                    
            # Populate user_id
            # Population of user_id is not required as we will only need to care about subscriber_patient_id and patient_id.
            # We will populate the user_id of the primary subscriber for the timebeing. We should completely remove user_id
            # population once we have dependent access in place, as it will not be obvious which user_id to fill in here.
                
            user_result = None
            if employee_ssn and employee_ssn in self.users:
                user_result = self.users[employee_ssn]
                user_id = user_result['id']
            elif employee_ssn:
                user_entry = {'identity_patient_id':subscriber_patient_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
                user_result = fac_users.find(user_entry)
                if user_result:
                    if employee_ssn:
                        self.users[employee_ssn] = user_result
                    user_id = user_result['id']
                        
        return_dict['subscriber_patient_id'] = subscriber_patient_id
        return_dict['patient_id'] = patient_id
        return_dict['user_id'] = user_id
        
        return return_dict

#    def resolve_patient(self, claim):
#        normalized_claim = self.__normalize_claim__(claim)
##        pprint.pprint(normalized_claim)
#        fac_users = ModelFactory.get_instance(self.conn, "users")
#        fac_patients = ModelFactory.get_instance(self.conn, "patients")
#        fac_pums = ModelFactory.get_instance(self.conn, "patient_user_mappings")
#
#        member_to_employee_relationship = normalized_claim['member_relationship']
#
#        employee_num = normalized_claim.get('employee_id')
#        patient_num = normalized_claim.get('member_id')
#
#        employee_ssn = employee_num[0:9] if employee_num else patient_num[0:9] if patient_num else None
#
#        user_result = None
#        if employee_ssn in self.users:
#            user_result = self.users[employee_ssn]
#        elif employee_ssn:
#            user_entry = {'ssn':employee_ssn}
#            user_result = fac_users.find(user_entry)
#            if user_result:
#                self.users[employee_ssn] = user_result
#
#
#        patient_id = -1
#        user_id = -1
#        if user_result:
#            user_id = user_result['id']
#            if (patient_num in self.patients):
#                patient_id = self.patients[patient_num]
#            elif (member_to_employee_relationship
#                and (member_to_employee_relationship.strip() == 'E')
#               ) or (patient_num == employee_num):
#                # Find Employee
#                patient_entry = {'identity_user_id':user_result['id']}
#                patient_result = fac_patients.find(patient_entry)
#                if patient_result:
#                    patient_id = patient_result['id'] 
#                    if (patient_num):
#                        self.patients[patient_num] = patient_id
#                else: 
#                    patient_id = -1
#            else:
#                pums_results = Query(self.conn, """SELECT PUM.patient_id, P.*
#                                              FROM patient_user_mappings PUM,
#                                                   patients P
#                                             WHERE PUM.user_id=%s
#                                               AND P.id=PUM.patient_id
#                                               AND P.identity_user_id is NULL""" % user_result['id'])
#                for pums_result in pums_results:
#                    mfn = normalized_claim['member_first_name'].lower() if normalized_claim['member_first_name'] else None
#                    pfn = pums_result['first_name'].lower() if pums_result['first_name'] else None
#                    mln = normalized_claim['member_last_name'].lower() if normalized_claim['member_last_name'] else None
#                    pln = pums_result['last_name'].lower() if pums_result['last_name'] else None
#                    # Simple name comparison for now
#                    if (mfn == pfn and mln == pln):
#                        patient_id = pums_result['id']
#                        if (patient_num):
#                            self.patients[patient_num] = patient_id
#                        break
#        return {'user_id':user_id,'patient_id':patient_id}

    def hash_internal_member(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        
        employee_num = normalized_claim.get('employee_id')
        patient_num = normalized_claim.get('member_id') 
        subscriber_ssn = employee_num[0:9] if employee_num else patient_num[0:9] if patient_num else None
        
        member_first_name = normalized_claim['member_first_name'].strip() if normalized_claim['member_first_name'] else None
        member_dob = str(normalized_claim['member_dob']) if normalized_claim['member_dob'] else None        
        if member_first_name and member_dob and subscriber_ssn:
            internal_member_hash = hashlib.sha1(member_first_name.lower() + str(member_dob) + subscriber_ssn[0:9]).hexdigest()
        else:
            internal_member_hash = None    
        return internal_member_hash
    
    def resolve_type_of_service(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
         
        type_of_service_id = self.external_types_of_service.get(self.insurance_company_id).get(normalized_claim['type_of_service'].lower()) if normalized_claim['type_of_service'] else None
        if not type_of_service_id:
            return -1
        return type_of_service_id

    def __parse_provider__(self, claim):

        normalized_claim = self.__normalize_claim__(claim)

        servicing_provider_type = normalized_claim['provider_type'].lower() if normalized_claim['provider_type'] else 'fa'
        provider_name = normalized_claim.get('provider_name') if normalized_claim.get('provider_name') else ''
        parsed_provider = {'provider_name':provider_name}
        provider_type = 'practitioner'

        if (servicing_provider_type == 'pr'):
            if (provider_name and len(provider_name) > 0):
                try:
                    parsed_name = parseName(provider_name)
                    parsed_provider.update(parsed_name)
                except IndexError, e:
                    title_set = normalized_claim.get('title_set') if normalized_claim.get('title_set') else set([]) 
                    name_parts = set(provider_name.split()) if provider_name else set([])
                    intersect = name_parts & title_set
                    if not intersect or len(intersect) == 0:
                        provider_type = 'facility'
                    else:
                        for key in intersect:
                            while key in name_parts:
                                name_parts.remove(key)
                        try:
                            if len(name_parts) > 2:
                                provider_name = '%s, %s %s.' % (name_parts[0],name_parts[1], name_parts[2])
                                parsed_name = parseName(provider_name)
                                parsed_provider.update(parsed_name)
                            elif len(name_parts) > 1:
                                provider_name = '%s, %s' % (name_parts[0],name_parts[1])
                                parsed_name = parseName(provider_name)
                                parsed_provider.update(parsed_name)
                        except IndexError, e:
                            parsed_name = set([])
                            
        if (servicing_provider_type == 'fa' or servicing_provider_type == 'an'):
            provider_type = 'facility'

        if (servicing_provider_type == 'as'):
            provider_type = 'group'

        parsed_provider['provider_type'] = provider_type

        return parsed_provider
    
    def resolve_procedure_labels(self, claim):
        
        normalized_claim = self.__normalize_claim__(claim)
        
        procedure_label_id = self.resolve_procedure_label(normalized_claim)
        secondary_procedure_label_id = self.resolve_procedure_label(normalized_claim, False)
        
        external_procedure_code_type = normalized_claim['procedure_code_type'].lower() if normalized_claim['procedure_code_type'] else None
        if external_procedure_code_type == 'rv' and secondary_procedure_label_id > -1:
            rev_procedure_label_id = procedure_label_id
            procedure_label_id = secondary_procedure_label_id
            secondary_procedure_label_id = rev_procedure_label_id
        
        return {'procedure_label_id':procedure_label_id,
                'secondary_procedure_label_id':secondary_procedure_label_id}
        
    def __extract_procedure_code__(self, claim, is_primary = True):
        normalized_claim = self.__normalize_claim__(claim)
        
        prefix = '' if is_primary else 'secondary_'
        
        external_procedure_code_type = normalized_claim['%sprocedure_code_type' % prefix].lower() if normalized_claim.get('%sprocedure_code_type' % prefix) else None
        procedure_code = normalized_claim['%sprocedure_code' % prefix].lower() if normalized_claim.get('%sprocedure_code' % prefix) else None
        
        if is_primary and (not procedure_code) and external_procedure_code_type == 'rv':
            procedure_code = normalized_claim['revenue_code'].lower()
        
        procedure_code_info = {'external_procedure_code':procedure_code, 'external_procedure_code_type':external_procedure_code_type}
        return procedure_code_info

    def __create_claim_attributes__(self, claim_id, claim):
        fac_claim_attributes = ModelFactory.get_instance(self.conn, 'claim_attributes')
        claim_attr_entry = {'claim_id':claim_id}
        if claim.get('RMK_CD') and len(claim['RMK_CD']) > 0:
            claim_attr_entry['name'] = 'RMK_CD'
            claim_attr_entry['value'] = claim['RMK_CD']
            self.batched_claim_attributes.append(claim_attr_entry)

        claim_attr_entry = {'claim_id':claim_id}
        if claim.get('RSN_NOT_COVRD_CD') and len(claim['RSN_NOT_COVRD_CD']) > 0:
            claim_attr_entry['name'] = 'RSN_NOT_COVRD_CD'
            claim_attr_entry['value'] = claim['RSN_NOT_COVRD_CD']
            self.batched_claim_attributes.append(claim_attr_entry)

    def __create_claim_subscriber_identifiers__(self, claim_id, claim):
        if claim['MBR_NUM']:
            claim_identifier_entry = {'claim_id':claim_id, 'subscriber_identifier': claim['MBR_NUM'][0:9]}
            self.batched_claim_subscriber_identifiers.append(claim_identifier_entry)


    def __update_user_reconciliation_table__(self):

        return "UPDATE ru_%s_%s SET employee_ssn=SUBSTRING(member_id, 1, 9)" % (self.stage_claim_table, self.imported_claim_file_id)

    def __custom_normalization__(self, normalized_claim, claim, filter):  
        
        if 'provider_specialty_code' in filter and normalized_claim['provider_specialty_code']:
            if normalized_claim['provider_specialty_code'].lower() in self.normalization_rules['cbh_codes']:
                normalized_claim['provider_specialty_code'] = 'cbh physician:%s' % normalized_claim['provider_specialty_code'].lower()
            else:
                normalized_claim['provider_specialty_code'] = 'physician:%s' % normalized_claim['provider_specialty_code'].lower()
            
        if self.stage_claim_table == 'cigna_imported_claims_bob':
            if not normalized_claim.get('allowed_amount'):
                paid_amount = normalized_claim['paid_amount'] if normalized_claim['paid_amount'] else 0
                copay_amount = normalized_claim['copay_amount'] if normalized_claim['copay_amount'] else 0
                coinsurance_amount = normalized_claim['coinsurance_amount'] if normalized_claim['coinsurance_amount'] else 0
                cob_amount = normalized_claim['cob_amount'] if normalized_claim['cob_amount'] else 0
                
                normalized_claim['paid_amount'] = paid_amount + cob_amount
                
                deductible_amount = normalized_claim['deductible_amount'] if normalized_claim['deductible_amount'] else 0
                
                normalized_claim['allowed_amount'] = paid_amount + copay_amount + coinsurance_amount + cob_amount + deductible_amount   
                
            if not normalized_claim['procedure_code_type'] or normalized_claim['procedure_code_type'].lower() == 'rv':
                # Done specifically for BOB to fall back to secondary procedure code when primary is a revenue code
                normalized_claim['procedure_code_type'] = normalized_claim['secondary_procedure_code_type'].lower() if normalized_claim['secondary_procedure_code_type'] else None
                normalized_claim['procedure_code'] = normalized_claim['secondary_procedure_code'].lower() if normalized_claim['secondary_procedure_code'] else None
                normalized_claim['procedure_code_modifier'] = normalized_claim['secondary_procedure_code_modifier'].lower() if normalized_claim['secondary_procedure_code_modifier'] else None
            
        
                
    def augment_claim_participations(self, logger = None):
        # We do not really augment claim participations for CIGNA. We will only do the relink
        load_properties = {'field_column_mappings':FIELD_MAPPINGS.get('cigna')}
        logutil.log(LOG if not logger else logger, logutil.INFO, 'Relinking lab claims.')
        claims_util.relink_lab_and_clinic_claims(whcfg.claims_master_schema, self.imported_claim_file_id, self.insurance_company_id, self.employer_id, self.stage_claim_table, load_properties, ['quest'], [], LOG if not logger else logger)
        return None
    ## Start WHSE-600
    def claim_provider_exception(self):
        map_load_properties = {'field_column_mappings':FIELD_MAPPINGS.get('cigna')}
        self.insert_claim_provider_exceptions(map_load_properties)
        return None  
    ## End WHSE-600 
class BcbsmaClaimsLoader(BaseClaimsLoader):

    def __initialize__(self):

        self.external_specialty_source = 'bcbsma'

        self.normalization_rules = {'M':FIELD_MAPPINGS['bcbsma'],
                                   'L':{
                                        'external_id_type':'BCBSMA',
                                        'source':'BCBSMA_CLAIM_%s' % (self.imported_claim_file_id),
                                        'insurance_company_id':self.insurance_company_id,
                                        'employer_id':self.employer_id
                                        }
                                   }

        self.external_specialties_map = {'Pediatrics, General':set([693,1271]),
                                            'Family Practice':set([502,503,428]),
                                            'Internal Medicine':set([567]),
                                            'Obstetrics/Gynecology':set([607,1269]),
                                            'Radiology, Other':set([747]), 
                                            'Physical Therapist':set([850]),
                                            'Pathology, Clinical':set([676]),
                                            'General Preventive Medicine':set([730]),
                                            'Gastroenterology':set([510]),
                                            'Dermatology':set([482]),
                                            'Surgery, Orthopedic':set([632]),
                                            'Pathology, Other':set([676]),
                                            'Psychologists':set([835]),
                                            'Hematology':set([552]),
                                            'Emergency Medicine':set([496]),
                                            'Cardiovascular Diseases':set([1144,1213]),
                                            'Allergy/Immunology':set([440]),
                                            'Geriatrics':set([547]),
                                            'Anesthesiology':set([448]),
                                            'Otorhino/Otolaryngology':set([645]),
                                            'Chiropractor':set([838]),
                                            'Clinical Social Worker':set([837]),
                                            'Urology':set([773]),
                                            'Podiatrist':set([849]),
                                            'Psychiatry':set([737]),
                                            'Surgery, General':set([523]),
                                            'Occupational Medicine':set([619]),
                                            'Gynecological Oncology':set([608]),
                                            'Pediatrics, Neurology':set([591]),
                                            'Physical Medicine & Rehab':set([717]),
                                            'Rheumatology':set([571]),
                                            'Pulmonary Diseases':set([570]),
                                            'Neonatal-Perinatal Medicine':set([575]),
                                            'Optometry':set([843]),
                                            'Radiology, Diagnostic':set([761]),
                                            'Nurse Midwife':set([1158]),
                                            'Infectious Diseases':set([563]),
                                            'Radiology, Therapeutic':set([761]),
                                            'Nephrology':set([569]),
                                            'Pediatrics, Cardiology':set([465]),
                                            'Surgery, Plastic':set([530]),
                                            'Surgery, Neurological':set([598]),
                                            'Immunopathology':set([687]),
                                            'Audiology':set([852]),
                                            'Surgery, General Vascular':set([535]),
                                            'Neurology':set([579]),
                                            'Reproductive Endocrinology':set([491]),
                                            'Medical Microbiology':set([688]),
                                            'Dermapathology':set([486]),
                                            'Pediatrics, Endocrinology':set([493]),
                                            'Surgery, Colon & Rectal':set([473]),
                                            'Surgery, Pediatric':set([529]),
                                            'Pediatrics, Hema/Oncology':set([558]),
                                            'Surgery, Hand':set([532]),
                                            'Pediatrics, Genetics':set([546]),
                                            'Pediatrics, Nephrology':set([578]),
                                            'Pediatric Urology':set([713]),
                                            'Allergy':set([441])
                                            }

    def resolve_patient(self, claim, filter = None):
        normalized_claim = self.__normalize_claim__(claim, filter)
#        pprint.pprint(normalized_claim)
        return {'user_id':-1,'patient_id':-1}


    def resolve_type_of_service(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        return -1

    def __query_external_procedure_code_types__(self):
        if not self.external_procedure_code_types:
            self.external_procedure_code_types = {self.insurance_company_id:{'cpt-4':1,
                                                                             'hcpcs':4,
                                                                             'revenue code':3}}

        return self.external_procedure_code_types
    
    def __parse_provider__(self, claim):

        normalized_claim = self.__normalize_claim__(claim)

        servicing_provider_type = normalized_claim['provider_type']
        provider_name = normalized_claim.get('provider_name') if normalized_claim.get('provider_name') else ''
        parsed_provider = {'provider_name':provider_name}
        provider_type = 'practitioner' if servicing_provider_type in set(["Medical Doctor",
                                                                            "Physical Therapist",
                                                                            "Optician",
                                                                            "Chiropractor",
                                                                            "Physician's Assistant",
                                                                            "Doctor of Optometry",
                                                                            "Podiatrist",
                                                                            "Psychologist, Doctoral Degree",
                                                                            "Registered Nurse",
                                                                            "Dentist"]) else 'facility'

        if (provider_type == 'practitioner'):
            try:
                parsed_name = parseName(provider_name)
                parsed_provider.update(parsed_name)
            except IndexError, e:
                provider_type = 'facility'
                
        parsed_provider['provider_type'] = provider_type

        return parsed_provider

    def __extract_procedure_code__(self, claim, is_primary = True):
        
        if not is_primary:
            return None
        
        normalized_claim = self.__normalize_claim__(claim)

        external_procedure_code_type = normalized_claim['procedure_code_type'].lower() if normalized_claim['procedure_code_type'] else None
        procedure_code = normalized_claim['procedure_code'].lower() if normalized_claim['procedure_code'] else None

        procedure_code_info = {'external_procedure_code':procedure_code, 'external_procedure_code_type':external_procedure_code_type}
        return procedure_code_info

    def __create_claim_attributes__(self, claim_id, claim):
        fac_claim_attributes = ModelFactory.get_instance(self.conn, 'claim_attributes')
        claim_attr_entry = {'claim_id':claim_id}
        if claim['primary_diagnosis_code'] and len(claim['primary_diagnosis_code']) > 0:
            claim_attr_entry['name'] = 'primary_diagnosis_code'
            claim_attr_entry['value'] = claim['primary_diagnosis_code']
#            cid = st.start('claim_attributes.create')
#            fac_claim_attributes.create(claim_attr_entry)
#            st.end(cid)
            self.batched_claim_attributes.append(claim_attr_entry)


    def __update_user_reconciliation_table__(self):

        return "UPDATE ru_%s_%s SET employee_ssn=SUBSTRING(member_id, 1, 9)" % (self.stage_claim_table, self.imported_claim_file_id)
    
class GenericClaimsLoader(BaseClaimsLoader):

    def __initialize__(self):
        
        if not self.load_properties: 
            sys.exit()
            
        specialties_entry = self.load_properties.get('specialties')
        self.external_specialty_source = specialties_entry.get('external_specialty_source') if specialties_entry else None
        self.external_specialty_source = self.external_specialty_source.lower() if self.external_specialty_source else None

        self.normalization_rules = {'M':self.load_properties.get('field_column_mappings'),
                                    'L':{
                                        'source':'%s_CLAIM_%s' % (self.claim_file_source_name.upper(), self.imported_claim_file_id),
                                        'employer_id':self.employer_id,
                                        'patient_identifier_type':'ssn'
                                        }
                                   }
        
        if self.load_properties.get('patient_identifier_type'):
            self.normalization_rules['L']['patient_identifier_type']=self.load_properties.get('patient_identifier_type')

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
        
        # Superimpose manual place of service from load properties file
        load_properties_place_of_service_mappings = self.load_properties.get('place_of_service',{})
        if not self.places_of_service:
            self.places_of_service = {}
        if not load_properties_place_of_service_mappings:
            load_properties_place_of_service_mappings = {} 
        for clh_code_id, external_code in load_properties_place_of_service_mappings.iteritems():
            self.places_of_service[str(external_code)] = clh_code_id
            
        # Superimpose manual type of service from load properties file
        load_properties_type_of_service_mappings = self.load_properties.get('type_of_service',{})
        if not self.types_of_service:
            self.types_of_service = {}
        if not load_properties_type_of_service_mappings:
            load_properties_type_of_service_mappings = {}
        for clh_code_id, external_code in load_properties_type_of_service_mappings.iteritems():
            if not self.types_of_service.get(external_code):
                self.types_of_service[str(external_code)] = set([clh_code_id])
            else:
                self.types_of_service[str(external_code)].update(set([clh_code_id]))        

        load_properties_member_relationships = self.load_properties.get('member_relationships',{})
        if not self.member_relationships:
            self.member_relationships = {}
        if not load_properties_member_relationships:
            load_properties_member_relationships = {}
            
        for member_relationship, external_codes in load_properties_member_relationships.iteritems():
            if not isinstance(external_codes, list):
                external_codes = [external_codes]
            for external_code in external_codes:
                self.member_relationships[str(external_code).lower()] = member_relationship.lower()

    def resolve_patient(self, claim, filter = None):
        normalized_claim = self.__normalize_claim__(claim, filter)
        
        subscriber_ssn = normalized_claim['employee_ssn'] if normalized_claim['employee_ssn'] else None
        subscriber_first_name = normalized_claim['employee_first_name'] if normalized_claim['employee_first_name'] else None
        subscriber_last_name = normalized_claim['employee_last_name'] if normalized_claim['employee_last_name'] else None
        
        member_ssn = normalized_claim['member_ssn'].strip() if normalized_claim['member_ssn'] else None
        member_first_name = normalized_claim['member_first_name'].strip() if normalized_claim['member_first_name'] else None
        member_dob = str(normalized_claim['member_dob']) if normalized_claim['member_dob'] else None
        member_last_name = normalized_claim['member_last_name'].strip() if normalized_claim['member_last_name'] else None
        
        ext_member_relationship = normalized_claim.get('member_relationship', None)
        member_to_subscriber_relationship = self.member_relationships.get(str(ext_member_relationship).lower()) if ext_member_relationship else None
        
        is_relationship_available = True if self.normalization_rules['M'].get('member_relationship', None) else False
        is_subscriber = True if member_to_subscriber_relationship == 'subscriber' else False
        is_member_ssn_available = True if self.normalization_rules['M'].get('member_ssn', None) else False
        is_subscriber_first_name_available = True if self.normalization_rules['M'].get('employee_first_name', None) else False
        
        pi = claims_util.PatientIdentifier()
        return_dict = {}
        if (self.normalization_rules['L']['patient_identifier_type'] == 'ssn'):
	    patient_info = {'conn':self.conn,
			    'subscriber_ssn':subscriber_ssn,
			    'subscriber_first_name':subscriber_first_name,
			    'subscriber_last_name':subscriber_last_name, 
			    'member_ssn':member_ssn, 
			    'member_first_name':member_first_name, 
			    'member_dob':member_dob,
			    'member_last_name':member_last_name, 
			    'is_relationship_available':is_relationship_available,
			    'is_subscriber':is_subscriber,
			    'is_member_ssn_available':is_member_ssn_available,
			    'is_subscriber_first_name_available':is_subscriber_first_name_available,
			    'insurance_company_id':self.insurance_company_id,
                            'employer_id':self.employer_id }
	    return_dict = pi.resolve_generic_claim_patient(**patient_info)

	    if return_dict.get('patient_id') == -1 and return_dict.get('subscriber_patient_id') > -1 and patient_info['member_first_name']:
		fn_array = patient_info['member_first_name'].split()
		if len(fn_array) > 1 and len(fn_array[-1]) == 1:
		    fn_array.pop() ##remove trailing middle initial
		    patient_info['member_first_name'] = ' '.join(fn_array)
		    return_dict = pi.resolve_generic_claim_patient(**patient_info)

        else:
            return_dict = pi.resolve_nonssn_claim_patient(conn=self.conn,
                                                           subscriber_identifier=subscriber_ssn,
                                                           subscriber_first_name=subscriber_first_name,
                                                           subscriber_last_name=subscriber_last_name,
                                                           member_identifier=member_ssn,
                                                           member_first_name=member_first_name,
                                                           member_dob=member_dob,
                                                           member_last_name=member_last_name,
                                                           is_relationship_available=is_relationship_available,
                                                           is_subscriber=is_subscriber,
                                                           is_member_identifier_available=is_member_ssn_available,
                                                           is_subscriber_first_name_available=is_subscriber_first_name_available,
                                                           insurance_company_id=self.insurance_company_id,
                                                           employer_id=self.employer_id,
                                                           identifier_type=self.normalization_rules['L']['patient_identifier_type'])
        if return_dict.get('subscriber_patient_id') > -1:
            suppress_dependents = ('dependent_identification' in claims_util.PatientIdentifier.SUPPRESSION_MAP.get('%s' %(self.employer_id),[]))
            if suppress_dependents:
                if return_dict.get('subscriber_patient_id') <> return_dict.get('patient_id'):
                    return_dict['patient_id'] = -1
                    return_dict['subscriber_patient_id'] = -1
                    
        # Pupulate user_id is subscriber_patient_id was resolvable. This code is temporary until the point Patient Master goes live.
#        return_dict['user_id'] = -1
#        if (return_dict and return_dict.get('subscriber_patient_id')):
#            user_entry = {'identity_patient_id':return_dict.get('subscriber_patient_id'), 'employer_id':self.employer_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
#            fac_users = ModelFactory.get_instance(self.conn, "users")
#            user_result = fac_users.find(user_entry)
#            if user_result:
#                return_dict['user_id'] = user_result['id']            
        
        return return_dict

                           
    def resolve_patient_old(self, claim):
        normalized_claim = self.__normalize_claim__(claim)
        fac_users = ModelFactory.get_instance(self.conn, "users")
        fac_patients = ModelFactory.get_instance(self.conn, "patients")
        
        ext_member_relationship = normalized_claim.get('member_relationship', None)
        member_to_employee_relationship = self.member_relationships.get(str(ext_member_relationship).lower()) if ext_member_relationship else None
        member_id = normalized_claim['member_id'] if normalized_claim['member_id'] else None
        patient_ssn = normalized_claim['member_ssn'] if normalized_claim['member_ssn'] else None
        employee_ssn = normalized_claim['employee_ssn'] if normalized_claim['employee_ssn'] else None
        
        patient_id = -1
        user_id = -1
        subscriber_patient_id = -1
        subscriber_patient = None
        return_dict = {'user_id':user_id,'patient_id':patient_id, 'subscriber_patient_id': subscriber_patient_id}
        
        if employee_ssn and employee_ssn in self.subscriber_patients:
            subscriber_patient = self.subscriber_patients[employee_ssn]
#            subscriber_patient_id = subscriber_patient['id']
        elif employee_ssn:
            q_subscribing_patient = """SELECT p.* FROM patients p,
                                              policies po
                                        WHERE p.ssn = %s
                                          AND po.subscriber_patient_id=p.id
                                        GROUP BY p.id"""
            
            subscriber_patient_result = Query(self.conn, q_subscribing_patient, (employee_ssn))
            if len(subscriber_patient_result) <> 1:
                return return_dict
            
            subscriber_patient = subscriber_patient_result.next()

        if subscriber_patient:
            self.subscriber_patients[employee_ssn] = subscriber_patient
            subscriber_patient_id = subscriber_patient['id']
#            self.patients[employee_ssn] = subscriber_patient_id
            
#            if patient_ssn and patient_ssn in self.patients:
#                # Check cache
#                patient_id = self.patients[patient_ssn]
#                
#            el
            if ((member_to_employee_relationship and (member_to_employee_relationship == 'subscriber'))
                 or (patient_ssn == employee_ssn)):
                patient_id = subscriber_patient_id
#                if patient_ssn:
#                    self.patients[patient_ssn] = patient_id
            else:
                # Once we have the subscribing patient we can get the patient_id from policy_coverages

                q_patients = """SELECT p.*
                                  FROM policies po,
                                       policy_coverages pc,
                                       patients p
                                 WHERE po.subscriber_patient_id = %s
                                   AND pc.policy_id = po.id
                                   AND pc.patient_id = p.id
                                   AND p.id <> %s
                              GROUP BY p.id"""
                              
                patients_result = Query(self.conn, q_patients, (subscriber_patient_id, subscriber_patient_id))
                
                for patient in patients_result:
                    mfn = normalized_claim['member_first_name'].lower() if normalized_claim['member_first_name'] else None
                    pfn = patient['first_name'].lower() if patient['first_name'] else None
                    mln = normalized_claim['member_last_name'].lower() if normalized_claim['member_last_name'] else None
                    pln = patient['last_name'].lower() if patient['last_name'] else None
                    # Simple name comparison for now
                    if (mfn == pfn and mln == pln):
                        patient_id = patient['id']
#                        if patient_ssn:
#                            self.patients[patient_ssn] = patient_id
                        break
                    
            # Populate user_id
            # Population of user_id is not required as we will only need to care about subscriber_patient_id and patient_id.
            # We will populate the user_id of the primary subscriber for the timebeing. We should completely remove user_id
            # population once we have dependent access in place, as it will not be obvious which user_id to fill in here.
                
            user_result = None
            if employee_ssn and employee_ssn in self.users:
                user_result = self.users[employee_ssn]
                user_id = user_result['id']
            elif employee_ssn:
                user_entry = {'identity_patient_id':subscriber_patient_id} # Read note above. identity_patient_id is not guaranteed to be the same as subscriber_patient_id once we have dependent access
                user_result = fac_users.find(user_entry)
                if user_result:
                    if employee_ssn:
                        self.users[employee_ssn] = user_result
                    user_id = user_result['id']
                        
        return_dict['subscriber_patient_id'] = subscriber_patient_id
        return_dict['patient_id'] = patient_id
        return_dict['user_id'] = user_id
        
        return return_dict


    def __query_external_procedure_code_types__(self):
        
        if not self.external_procedure_code_types:
            pc_map = {}
            procedure_code_type_mappings = self.load_properties.get('procedure_code_types')
            if procedure_code_type_mappings:
                for k,v in procedure_code_type_mappings.iteritems():
                    if v: pc_map[str(v).lower()] = self.procedure_code_types.get(k)            
            self.external_procedure_code_types = {self.insurance_company_id:pc_map}

        return self.external_procedure_code_types
    
    def __parse_provider__(self, claim):

        normalized_claim = self.__normalize_claim__(claim)

        servicing_provider_type = normalized_claim['provider_type']
        provider_name = normalized_claim.get('provider_name') if normalized_claim.get('provider_name') else ''
        parsed_provider = {'provider_name':provider_name}
        
        provider_type_mappings = self.load_properties.get('provider_types',{})
        facility_type_set = set(provider_type_mappings.get('facility',[]))
#        facility_type_set = set([str(v) for v in set(provider_type_mappings.get('facility',[]))])
        provider_type = 'facility' if (facility_type_set  and servicing_provider_type in facility_type_set) else 'practitioner'
                
        if (provider_type == 'practitioner'):
            try:
                parsed_name = parseName(provider_name)
                parsed_provider.update(parsed_name)
            except IndexError, e: 
                parsed_provider.update({'display_name':provider_name,
                                        'first_name':'',
                                        'middle_name':'',
                                        'last_name':''}) 
                
        parsed_provider['provider_type'] = provider_type

        return parsed_provider

    def resolve_out_of_network(self, claim):

        normalized_claim = self.__normalize_claim__(claim)
        out_of_network_mappings = self.load_properties.get('out_of_network_indicator')
        
        inn_codes = set([str(x).lower() for x in out_of_network_mappings.get('n')]) if isinstance(out_of_network_mappings.get('n'), list) else set([str(out_of_network_mappings.get('n')).lower()]) if out_of_network_mappings else None
        out_of_network = 1
        if inn_codes and normalized_claim['out_of_network_indicator'] and normalized_claim['out_of_network_indicator'].lower() in inn_codes:
            out_of_network = 0
        return out_of_network

    def __extract_procedure_code__(self, claim, is_primary = True):
        normalized_claim = self.__normalize_claim__(claim)
        
        prefix = '' if is_primary else 'secondary_'
         
        external_procedure_code_type = normalized_claim['%sprocedure_code_type' % prefix].lower() if normalized_claim.get('%sprocedure_code_type' % prefix) else None
        procedure_code = normalized_claim['%sprocedure_code' % prefix].lower() if normalized_claim.get('%sprocedure_code' % prefix) else None

        procedure_code_info = {'external_procedure_code':procedure_code, 'external_procedure_code_type':external_procedure_code_type}
        return procedure_code_info

    def __create_claim_attributes__(self, claim_id, claim):
        fac_claim_attributes = ModelFactory.get_instance(self.conn, 'claim_attributes')
        claim_attributes = self.load_properties.get('claim_attributes', {}) 
        if isinstance(claim_attributes, list):
            for attribute_name in  claim_attributes:
                claim_attr_entry = {'claim_id':claim_id}
                if claim.get(attribute_name) and len(claim[attribute_name]) > 0:
                    claim_attr_entry['name'] = attribute_name
                    claim_attr_entry['value'] = claim[attribute_name]
                    self.batched_claim_attributes.append(claim_attr_entry)
        else:
            for mapped_attribute_name, attribute_name in  claim_attributes.iteritems():
                claim_attr_entry = {'claim_id':claim_id}
                if attribute_name and claim.get(attribute_name) and len(claim[attribute_name]) > 0:
                    claim_attr_entry['name'] = mapped_attribute_name
                    claim_attr_entry['value'] = claim[attribute_name]
                    self.batched_claim_attributes.append(claim_attr_entry)            

    def augment_claim_participations(self, logger = None):
        if self.load_properties and self.load_properties.get('external_id_type','').lower() == 'npi': 
            logutil.log(LOG if not logger else logger, logutil.INFO, 'Augmenting Provider Participations with addresses from claims.')
            claims_util.refresh_claim_participations_npi(whcfg.claims_master_schema, [str(self.imported_claim_file_id)], LOG if not logger else logger)
    ## Start WHSE-600
    def claim_provider_exception(self):
         map_load_properties = self.load_properties
         self.insert_claim_provider_exceptions(map_load_properties)
         return None
    ## End WHSE-600
    def __update_user_reconciliation_table__(self):
        return None

    def __custom_normalization__(self, normalized_claim, claim, filter):               
        # This method gets called once on every claim line item at the time of normalization
        if 'procedure_code' in filter:
            normalized_procedure_code = normalized_claim['procedure_code']
    
            if isinstance(normalized_procedure_code, dict):
                
                # When different types of procedure codes are provided across more than one column
                normalized_claim['procedure_code'] = None 
                
                if normalized_claim['procedure_code_type']:
                    # There is a column that signals what type of procedure code to use
                    type = self.procedure_code_type_values.get(self.external_procedure_code_types[self.insurance_company_id].get(normalized_claim['procedure_code_type'].strip().lower()))
                    if type:
                        normalized_claim['procedure_code_type'] = type
                        normalized_claim['procedure_code'] = normalized_procedure_code.get(type)
                else:             
                    procedure_code = None
                    procedure_code_type = None
                    procedure_code_revenue = None
                    multiple_procedure_codes = False
                    for type, column_value in normalized_procedure_code.items():
                        if not column_value: continue
                        if (type == 'revenue'):
                            procedure_code_revenue = column_value
                        else:
                            procedure_code_other = column_value    
                            if procedure_code is not None and procedure_code_type != 'icd9': # Overwrite pc if icd9 is the 1st Procedure Code encountered
                                if type != 'icd9':
                                    # Only deem multiple procedure codes if the 2nd procedure_code encountered is not icd9
                                    # Multiple Procedure Codes on same row
                                    normalized_claim['procedure_code'] = None
                                    normalized_claim['procedure_code_type'] = None
                                    multiple_procedure_codes = True
                                    break
                            else:
                                procedure_code = procedure_code_other
                                procedure_code_type = type
                                normalized_claim['procedure_code'] = procedure_code_other
                                normalized_claim['procedure_code_type'] = type
                    
                    if procedure_code_revenue and not multiple_procedure_codes and (not normalized_claim['procedure_code'] or normalized_claim['procedure_code_type'] == 'icd9'):
                        normalized_claim['procedure_code'] = procedure_code_revenue
                        normalized_claim['procedure_code_type'] = 'revenue'                               
            
            procedure_code_type = normalized_claim['procedure_code_type'].strip().lower() if normalized_claim.get('procedure_code_type') else None
            procedure_code = normalized_claim['procedure_code'].strip() if normalized_claim.get('procedure_code') else None 
            
            if (self.external_procedure_code_types[self.insurance_company_id].get(procedure_code_type) == self.procedure_code_types['cpt']
                or self.external_procedure_code_types[self.insurance_company_id].get(procedure_code_type) == self.procedure_code_types['hcpc']):
                #Regardlesss of whether the source being a hcpc or cpt, pass through hcpc matcher
                hcpc_matcher = re.compile('^[a-z]{1}[0-9]{4}', flags=re.IGNORECASE)
                if procedure_code and hcpc_matcher.match(procedure_code) and self.load_properties.get('procedure_code_types').get('hcpc'):
                    procedure_code_type = self.load_properties.get('procedure_code_types').get('hcpc')
                else:
                    procedure_code_type = self.load_properties.get('procedure_code_types').get('cpt')
                
                normalized_claim['procedure_code_type'] = procedure_code_type
        
        if self.load_properties and self.load_properties.get('external_id_type'):
            normalized_claim['external_id_type'] = self.load_properties.get('external_id_type').lower()
        
class BcbsalClaimsLoader(GenericClaimsLoader):

    def __query_external_procedure_code_types__(self):
        if not self.external_procedure_code_types:
            self.external_procedure_code_types = {self.insurance_company_id:{'cpt':1,
                                                                             'hcpc':4,
                                                                             'revenue':3}}

        return self.external_procedure_code_types
    
    def __custom_normalization__(self, normalized_claim, claim):               
#  --------  MAPPING CLAIMS TO PROVIDERS  --------                             01
#  YOU CAN FIND THE PROVIDER NAME, ADDRESS, ETC., FOR A                        01
#  CLAIM BY USING THE PPC AND PROVIDER NUMBER. THERE WILL                      01
#  BE A FEW OCCASIONS WHERE THIS INFO IS NOT ON THE CLAIM, MAINLY              01
#  IN SUBSCRIBER-PAYABLE CLAIMS (TYPE-CLAIM = '1'.)                            01
#                                                                              01
#  FOR BLUE CROSS CLAIMS, THE PROVIDER NUMBER IS 'HOSP-NR.'                    01
#                                                                              01
#  FOR BLUE SHIELD CLAIMS, THE PROVIDER NUMBER IS 'DOCTOR.' IN                 01
#  THE CASE OF AN 'ITS' BLUE SHIELD CLAIM, AND 'DOCTOR' IS                     01
#  EQUAL TO '09909', THEN USE 'ITS-DOCTOR-NR.'                                 01
#                                                                              01
#  FOR DRUG CLAIMS, THE PROVIDER NUMBER IS 'PHARMACY.'                         01
#  FOR MAIL ORDER DRUG PRESCRIPTIONS, SEE MAIL-ORDR-DRUG-SW.                   01
# 
#
#  TYPE-BUSINESS                                                               01
#     1- BLUE CROSS                                                            01
#     2- BLUE SHIELD                                                           01
#     4- MAJOR MEDICAL                                                         01
#     7- DENTAL                                                                01
#     D- PRE-PAID DRUG  

#FOR HOSP_NR <> '000' use PPC+HOSP_NR
#
#FOR HOSP_NR = '000' use DOCTOR_NR+ITS_DOCTOR_NR where ITS_DOCTOR_NR is not null
#
#FOR HOSP_NR = '000' use NPI where ITS_DOCTOR_NR is null

        bus_type = claim['TYPE_BUS'].upper() if claim['TYPE_BUS'] else ''
        claim_type = claim['TYPE_CLAIM']
        
        # Default case bus_type=4
        ppc = claim['PPC']
        hosp_nr = claim['HOSP_NR']
        doctor_nr = claim['DOCTOR_NR']
        its_doctor_nr = claim['ITS_DOCTOR_NR']
        
        prov_details_query = """SELECT p.PROV_NAME, p.ADDR_1, p.ADDR_2, p.CITY, p.STATE, p.ZIP_1ST_5, p.NPI
                            FROM bcbsal_imported_claim_providers p
                           WHERE p.PPC=%s
                             AND p.HOSP_NR=%s
                             AND p.DOCTOR_NR=%s
                             """
                                     
        provider_nr = ''
        provider_type = 'practitioner'
        prov_dq_append = """AND p.ITS_DOCTOR_NR=%s"""
        substitute = (ppc, hosp_nr, doctor_nr, its_doctor_nr)
        
        if doctor_nr == '000':
            provider_nr = '%s_%s' % (ppc, hosp_nr)
            provider_type = 'facility'
        elif its_doctor_nr:
            provider_nr = '%s_%s' % (doctor_nr, its_doctor_nr)
        else:
           provider_nr = None 
        
        if not its_doctor_nr:
            prov_dq_append = """AND p.ITS_DOCTOR_NR is null"""
            substitute = (ppc, hosp_nr, doctor_nr)
            
        prov_details_query = prov_details_query + prov_dq_append
        prov_details = Query(self.conn, prov_details_query, substitute)
        
        provider_detail = prov_details.next() if prov_details else None

        if not provider_detail:
            logutil.log(LOG, logutil.INFO, "Unable to resolve Provider! Imported Claim ID: %s" % (normalized_claim['imported_claim_id']))
        else: 
            provider_name = provider_detail['PROV_NAME'] 
            normalized_claim['street_address'] = provider_detail['ADDR_1'] 
            normalized_claim['unit'] = provider_detail['ADDR_2'] 
            normalized_claim['city'] = provider_detail['CITY'] 
            normalized_claim['state'] = provider_detail['STATE'] 
            normalized_claim['zip'] = provider_detail['ZIP_1ST_5'] 
            if not provider_nr: provider_nr = provider_detail['NPI'] 
            normalized_claim['provider_pin'] = provider_nr
            normalized_claim['provider_name'] = provider_name
                            
        
        procedure_code = claim['CPT4_PROCEDURE_CODE'].strip() if claim['CPT4_PROCEDURE_CODE'] else None 
        procedure_code_type = 'cpt' if procedure_code else None 
        procedure_modifier = claim['CPT4_PROC_MOD_1'].strip()  if procedure_code  and claim['CPT4_PROC_MOD_1'] else None
        
        hcpc_matcher = re.compile('^[a-z]{1}[0-9]{4}', flags=re.IGNORECASE)
        if procedure_code and hcpc_matcher.match(procedure_code):
            procedure_code_type = 'hcpc'
        
        normalized_claim['procedure_code'] = procedure_code
        normalized_claim['procedure_code_type'] = procedure_code_type
        normalized_claim['procedure_code_modifier'] = procedure_modifier
        
