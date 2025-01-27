from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig
import sys

if __name__ =="__main__":
    try:
        trainingpiplineconfig= TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpiplineconfig)
        data_ingestion= DataIngestion(dataingestionconfig)
        logging.info("Initiate Data Ingestion")
        dataingestionartifact=data_ingestion.initaiate_data_ingestion()
        logging.info("Data Initian completed")
        print(dataingestionartifact)

        datavalidationconfig= DataValidationConfig(trainingpiplineconfig)
        data_validation= DataValidation(dataingestionartifact, datavalidationconfig)

        logging.info("Initiate Data validation")
        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("Data validation completed")
        print(data_validation_artifact)


    except Exception as e:
        raise NetworkSecurityException(e,sys)
