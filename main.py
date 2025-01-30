from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig, DataTransformationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig
from networksecurity.components.data_transformation import DataTransformation
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

        data_transformation_config=DataTransformationConfig(trainingpiplineconfig)
        logging.info("data Transformation started")
        data_transformation=DataTransformation(data_validation_artifact,data_transformation_config)
        data_transformation_artifact=data_transformation.initiate_data_transformation()
        print(data_transformation_artifact)
        logging.info("data Transformation completed")

    except Exception as e:
        raise NetworkSecurityException(e,sys)
