SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';


-- -----------------------------------------------------
-- Table `procedure_code_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `procedure_code_types` ;

CREATE  TABLE IF NOT EXISTS `procedure_code_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(255) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE UNIQUE INDEX `uq_name` ON `procedure_code_types` (`name` ASC) ;


-- -----------------------------------------------------
-- Table `betos`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `betos` ;

CREATE  TABLE IF NOT EXISTS `betos` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(45) NULL ,
  `description_1` VARCHAR(4000) NULL ,
  `description_2` VARCHAR(4000) NULL ,
  `description_3` VARCHAR(4000) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table `procedure_codes`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `procedure_codes` ;

CREATE  TABLE IF NOT EXISTS `procedure_codes` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(45) NOT NULL ,
  `procedure_code_type_id` INT NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `termination_date` DATE NULL ,
  `short_description` VARCHAR(255) NULL ,
  `long_description` VARCHAR(4000) NULL ,
  `betos_id` INT NULL ,
  `ingenix_generic_description` VARCHAR(255) NULL ,
  `ingenix_detailed_description` VARCHAR(4000) NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_procedure_code_procedure_source1`
    FOREIGN KEY (`procedure_code_type_id` )
    REFERENCES `procedure_code_types` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_procedure_codes_betos1`
    FOREIGN KEY (`betos_id` )
    REFERENCES `betos` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_procedure_code_procedure_code_type_id` ON `procedure_codes` (`procedure_code_type_id` ASC) ;

CREATE INDEX `idx_code` ON `procedure_codes` (`code` ASC) ;

CREATE INDEX `fk_procedure_codes_betos1` ON `procedure_codes` (`betos_id` ASC) ;

CREATE UNIQUE INDEX `uq_proceure_code_type_id_code` ON `procedure_codes` (`code` ASC, `procedure_code_type_id` ASC) ;


-- -----------------------------------------------------
-- Table `procedure_modifiers`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `procedure_modifiers` ;

CREATE  TABLE IF NOT EXISTS `procedure_modifiers` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(4000) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE INDEX `uq_code` ON `procedure_modifiers` (`code` ASC) ;


-- -----------------------------------------------------
-- Table `procedure_labels`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `procedure_labels` ;

CREATE  TABLE IF NOT EXISTS `procedure_labels` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `procedure_code_id` INT(11) NOT NULL ,
  `procedure_code_type_id` INT(11) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `procedure_modifier_id` INT NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_procedure_procedure_code1`
    FOREIGN KEY (`procedure_code_id` )
    REFERENCES `procedure_codes` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_procedures_procedure_code_types1`
    FOREIGN KEY (`procedure_code_type_id` )
    REFERENCES `procedure_code_types` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_procedure_labels_procedure_modifiers1`
    FOREIGN KEY (`procedure_modifier_id` )
    REFERENCES `procedure_modifiers` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_procedure_code_id` ON `procedure_labels` (`procedure_code_id` ASC) ;

CREATE INDEX `fk_procedure_code_type_id` ON `procedure_labels` (`procedure_code_type_id` ASC) ;

CREATE INDEX `fk_procedure_labels_procedure_modifiers1` ON `procedure_labels` (`procedure_modifier_id` ASC) ;


-- -----------------------------------------------------
-- Table `imported_claim_files`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imported_claim_files` ;

CREATE  TABLE IF NOT EXISTS `imported_claim_files` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `file_path` VARCHAR(255) NOT NULL ,
  `file_name` VARCHAR(255) NOT NULL ,
  `table_name` VARCHAR(45) NOT NULL ,
  `imported_at` DATETIME NOT NULL ,
  `employer_id` INT(11) NOT NULL ,
  `created_at` DATETIME NULL DEFAULT NULL ,
  `updated_at` DATETIME NULL DEFAULT NULL ,
  `oldest_paid_date` DATE NULL ,
  `newest_paid_date` DATE NULL ,
  `claim_file_source_name` VARCHAR(45) NOT NULL ,
  `claim_file_source_type` VARCHAR(45) NOT NULL ,
  `labels_bitmask` BIGINT NULL DEFAULT 1 ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_imported_claim_files_employers1`
    FOREIGN KEY (`employer_id` )
    REFERENCES `employers` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE UNIQUE INDEX `uq_file_path_file_name` ON `imported_claim_files` (`file_path` ASC, `file_name` ASC) ;

CREATE INDEX `fk_imported_claim_files_employers1` ON `imported_claim_files` (`employer_id` ASC) ;


-- -----------------------------------------------------
-- Table `service_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `service_types` ;

CREATE  TABLE IF NOT EXISTS `service_types` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(10) NOT NULL ,
  `description` VARCHAR(45) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `is_standard_type` TINYINT NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE UNIQUE INDEX `uq_code` ON `service_types` (`code` ASC) ;


-- -----------------------------------------------------
-- Table `service_places`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `service_places` ;

CREATE  TABLE IF NOT EXISTS `service_places` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(20) NOT NULL ,
  `description` VARCHAR(255) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `is_standard_code` TINYINT NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE UNIQUE INDEX `uq_code` ON `service_places` (`code` ASC) ;


-- -----------------------------------------------------
-- Table `claims`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `claims` ;

CREATE  TABLE IF NOT EXISTS `claims` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `imported_claim_id` INT NOT NULL ,
  `provider_id` INT(11) NULL ,
  `procedure_label_id` INT(11) NOT NULL ,
  `patient_id` INT(11) NULL ,
  `user_id` INT(11) NULL ,
  `member_id` VARCHAR(200) NOT NULL ,
  `insurance_company_id` INT(11) NOT NULL ,
  `provider_location_id` INT(11) NOT NULL ,
  `insurance_network_id` INT NOT NULL ,
  `out_of_network` TINYINT(1) NOT NULL ,
  `units_of_service` INT(11) NOT NULL ,
  `service_type_id` INT NULL ,
  `inpatient` TINYINT(1) NOT NULL ,
  `service_begin_date` DATE NOT NULL ,
  `service_end_date` DATE NOT NULL ,
  `payment_date` DATE NOT NULL ,
  `charged_amount` DECIMAL(12,2) NULL ,
  `allowed_amount` DECIMAL(12,2) NULL ,
  `savings_amount` DECIMAL(12,2) NULL ,
  `cob_amount` DECIMAL(12,2) NULL ,
  `coinsurance_amount` DECIMAL(12,2) NULL ,
  `deductible_amount` DECIMAL(12,2) NULL ,
  `paid_amount` DECIMAL(12,2) NULL ,
  `copay_amount` DECIMAL(12,2) NULL ,
  `imported_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `imported_claim_file_id` INT(11) NOT NULL ,
  `employer_id` INT(11) NOT NULL ,
  `service_place_id` INT NULL ,
  `parse_status` INT NOT NULL DEFAULT 1 ,
  `parse_comment` VARCHAR(255) NULL ,
  `not_covered_amount` DECIMAL(12,2) NULL ,
  `hra_amount` DECIMAL(12,2) NULL ,
  `major_diagnosis_code` VARCHAR(3) NULL ,
  `diagnosis_code_1` VARCHAR(6) NULL ,
  `diagnosis_code_2` VARCHAR(6) NULL ,
  `diagnosis_code_3` VARCHAR(6) NULL ,
  `diagnosis_code_4` VARCHAR(6) NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_claim_provider1`
    FOREIGN KEY (`provider_id` )
    REFERENCES `providers` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claim_procedure1`
    FOREIGN KEY (`procedure_label_id` )
    REFERENCES `procedure_labels` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_insurance_companies1`
    FOREIGN KEY (`insurance_company_id` )
    REFERENCES `insurance_companies` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_patients1`
    FOREIGN KEY (`patient_id` )
    REFERENCES `patients` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_locations1`
    FOREIGN KEY (`provider_location_id` )
    REFERENCES `locations` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_insurance_networks1`
    FOREIGN KEY (`insurance_network_id` )
    REFERENCES `insurance_networks` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_imported_claim_files1`
    FOREIGN KEY (`imported_claim_file_id` )
    REFERENCES `imported_claim_files` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_employers1`
    FOREIGN KEY (`employer_id` )
    REFERENCES `employers` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_service_types1`
    FOREIGN KEY (`service_type_id` )
    REFERENCES `service_types` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_place_of_service1`
    FOREIGN KEY (`service_place_id` )
    REFERENCES `service_places` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_users1`
    FOREIGN KEY (`user_id` )
    REFERENCES `users` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claims_xyz_imported_claims1`
    FOREIGN KEY (`imported_claim_id` )
    REFERENCES `xyz_imported_claims` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_provider_id` ON `claims` (`provider_id` ASC) ;

CREATE INDEX `fk_procedure_id` ON `claims` (`procedure_label_id` ASC) ;

CREATE INDEX `fk_insurance_company_id` ON `claims` (`insurance_company_id` ASC) ;

CREATE INDEX `fk_patient_id` ON `claims` (`patient_id` ASC) ;

CREATE INDEX `fk_location_id` ON `claims` (`provider_location_id` ASC) ;

CREATE INDEX `fk_insurance_network_id` ON `claims` (`insurance_network_id` ASC) ;

CREATE INDEX `fk_imported_claim_file_id` ON `claims` (`imported_claim_file_id` ASC) ;

CREATE INDEX `fk_employer_id` ON `claims` (`employer_id` ASC) ;

CREATE INDEX `fk_claims_service_types1` ON `claims` (`service_type_id` ASC) ;

CREATE INDEX `fk_claims_place_of_service1` ON `claims` (`service_place_id` ASC) ;

CREATE INDEX `fk_claims_users1` ON `claims` (`user_id` ASC) ;

CREATE UNIQUE INDEX `uq_imported_claim_id_imported_claim_file_id` ON `claims` (`imported_claim_file_id` ASC, `imported_claim_id` ASC) ;

CREATE INDEX `idx_member_id` ON `claims` (`member_id` ASC) ;

CREATE INDEX `idx_imported_claims1` ON `claims` (`imported_claim_id` ASC) ;


-- -----------------------------------------------------
-- Table `claim_attributes`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `claim_attributes` ;

CREATE  TABLE IF NOT EXISTS `claim_attributes` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `claim_id` INT(11) NOT NULL ,
  `name` VARCHAR(100) NOT NULL ,
  `value` VARCHAR(4000) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_claim_attributes_claims1`
    FOREIGN KEY (`claim_id` )
    REFERENCES `claims` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_claim_attributes_claims1` ON `claim_attributes` (`claim_id` ASC) ;

CREATE UNIQUE INDEX `uq_claim_id_name` ON `claim_attributes` (`claim_id` ASC, `name` ASC) ;


-- -----------------------------------------------------
-- Table `external_procedure_code_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `external_procedure_code_types` ;

CREATE  TABLE IF NOT EXISTS `external_procedure_code_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(255) NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `insurance_company_id` INT(11) NOT NULL ,
  `procedure_code_type_id` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_external_procedure_code_types_insurance_companies1`
    FOREIGN KEY (`insurance_company_id` )
    REFERENCES `insurance_companies` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_external_procedure_code_types_procedure_code_types1`
    FOREIGN KEY (`procedure_code_type_id` )
    REFERENCES `procedure_code_types` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `idx_name` ON `external_procedure_code_types` (`name` ASC) ;

CREATE INDEX `fk_external_procedure_code_types_insurance_companies1` ON `external_procedure_code_types` (`insurance_company_id` ASC) ;

CREATE INDEX `fk_external_procedure_code_types_procedure_code_types1` ON `external_procedure_code_types` (`procedure_code_type_id` ASC) ;

CREATE UNIQUE INDEX `uq_insurance_company_id_name` ON `external_procedure_code_types` (`insurance_company_id` ASC, `name` ASC) ;


-- -----------------------------------------------------
-- Table `external_service_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `external_service_types` ;

CREATE  TABLE IF NOT EXISTS `external_service_types` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(45) NULL ,
  `insurance_company_id` INT(11) NOT NULL ,
  `description` VARCHAR(45) NULL ,
  `service_type_id` INT NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_external_service_types_insurance_companies1`
    FOREIGN KEY (`insurance_company_id` )
    REFERENCES `insurance_companies` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_external_service_types_service_types1`
    FOREIGN KEY (`service_type_id` )
    REFERENCES `service_types` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_external_service_types_insurance_com` ON `external_service_types` (`insurance_company_id` ASC) ;

CREATE INDEX `fk_external_service_types_service_types` ON `external_service_types` (`service_type_id` ASC) ;

CREATE UNIQUE INDEX `uq_insurance_company_id_code` ON `external_service_types` (`insurance_company_id` ASC, `code` ASC) ;


-- -----------------------------------------------------
-- Table `external_service_places`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `external_service_places` ;

CREATE  TABLE IF NOT EXISTS `external_service_places` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `code` VARCHAR(45) NOT NULL ,
  `description` VARCHAR(45) NOT NULL ,
  `service_place_id` INT NOT NULL ,
  `created_at` DATETIME NULL ,
  `updated_at` DATETIME NULL ,
  `insurance_company_id` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_external_place_of_service_place_of_service1`
    FOREIGN KEY (`service_place_id` )
    REFERENCES `service_places` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_external_place_of_service_insurance_companies1`
    FOREIGN KEY (`insurance_company_id` )
    REFERENCES `insurance_companies` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_external_place_of_service_place_of_service1` ON `external_service_places` (`service_place_id` ASC) ;

CREATE INDEX `fk_external_place_of_service_insurance_companies1` ON `external_service_places` (`insurance_company_id` ASC) ;

CREATE UNIQUE INDEX `uq_insurance_company_id_code` ON `external_service_places` (`insurance_company_id` ASC, `code` ASC) ;


-- -----------------------------------------------------
-- Table `service_descriptions`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `service_descriptions` ;

CREATE  TABLE IF NOT EXISTS `service_descriptions` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `generic_description` VARCHAR(255) NOT NULL ,
  `detailed_description` VARCHAR(4000) NOT NULL ,
  `procedure_code_id` INT(11) NOT NULL ,
  `teen_girls_sensitive` TINYINT(1) NULL DEFAULT 0 ,
  `category` VARCHAR(100) NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_service_descriptions_procedure_codes1`
    FOREIGN KEY (`procedure_code_id` )
    REFERENCES `procedure_codes` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_service_descriptions_procedure_codes1` ON `service_descriptions` (`procedure_code_id` ASC) ;

CREATE UNIQUE INDEX `uq_procedure_code_id` ON `service_descriptions` (`procedure_code_id` ASC) ;


-- -----------------------------------------------------
-- Table `claim_specialties`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `claim_specialties` ;

CREATE  TABLE IF NOT EXISTS `claim_specialties` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `claim_id` INT(11) NOT NULL ,
  `specialty_id` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_claim_specialties_claims1`
    FOREIGN KEY (`claim_id` )
    REFERENCES `claims` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_claim_specialties_specialties1`
    FOREIGN KEY (`specialty_id` )
    REFERENCES `specialties` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_claim_specialties_claims1` ON `claim_specialties` (`claim_id` ASC) ;

CREATE UNIQUE INDEX `uq_claim_id_specialty_id` ON `claim_specialties` (`claim_id` ASC) ;

CREATE INDEX `fk_claim_specialties_specialties1` ON `claim_specialties` (`specialty_id` ASC) ;


-- -----------------------------------------------------
-- Table `labels`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `labels` ;

CREATE  TABLE IF NOT EXISTS `labels` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(45) NOT NULL ,
  `value` BIGINT NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE UNIQUE INDEX `value_UNIQUE` ON `labels` (`value` ASC) ;

CREATE UNIQUE INDEX `name_UNIQUE` ON `labels` (`name` ASC) ;


-- -----------------------------------------------------
-- Table `imported_claim_files_insurance_companies`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `imported_claim_files_insurance_companies` ;

CREATE  TABLE IF NOT EXISTS `imported_claim_files_insurance_companies` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `imported_claim_file_id` INT(11) NOT NULL ,
  `insurance_company_id` INT(11) NOT NULL ,
  `created_at` DATETIME NOT NULL ,
  `updated_at` DATETIME NOT NULL ,
  `number_of_claims` INT(11) NULL ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `fk_imported_claim_file_insurance_companies_imported_claim_fil1`
    FOREIGN KEY (`imported_claim_file_id` )
    REFERENCES `imported_claim_files` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_imported_claim_file_insurance_companies_insurance_companies1`
    FOREIGN KEY (`insurance_company_id` )
    REFERENCES `insurance_companies` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = MyISAM;

CREATE INDEX `fk_imported_claim_files_insurance_companies_imported_claim_fi1` ON `imported_claim_files_insurance_companies` (`imported_claim_file_id` ASC) ;

CREATE INDEX `fk_imported_claim_files_insurance_companies_insurance_company1` ON `imported_claim_files_insurance_companies` (`insurance_company_id` ASC) ;

CREATE UNIQUE INDEX `uq_imported_claim_file_id_insurance_company_id` ON `imported_claim_files_insurance_companies` (`imported_claim_file_id` ASC, `insurance_company_id` ASC) ;



SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
