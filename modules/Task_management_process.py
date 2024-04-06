from modules.dataset import Dataset
from modules.ML_model_prediction import Predict
from modules.inspection import Inspection
from modules.Services import Services
from modules.chi_manager import Chi

from logger import logger


def prediction_process_management(conjunto, us_services, pred_year,
                                  pred_month, months_ahead=2):
    try:
        # create dataset object
        dataset_obj = Dataset(conjunto, pred_year, pred_month)
        if not dataset_obj.has_error:
            dataset = dataset_obj.dataset

            if not dataset.empty:
                chi = Chi(conjunto, pred_year, pred_month, months_ahead)
                if not chi.has_error:
                    chi_goals = chi.conjunto_chi_goals

                    prediction = Predict(dataset, chi_goals, pred_year,
                                         pred_month, months_ahead)

                    # logger.info(prediction.feeder_preds)

                    # assigning the tasks
                    inspection = Inspection(prediction.feeder_preds)
                    inspection_df = inspection.inspection_df

                    if not inspection_df.empty:
                        tasks = inspection.task_assignment(chi_goals,
                                                           prediction.predicted_conj_chi,
                                                           us_services)
                        return tasks
                    else:
                        logger.info("Inspection Dataset is empty")
                        return None
                else:
                    logger.info("Error in reading CHI Goals.")
                    return None
            else:
                logger.info("Dataset is empty")
                return None
        else:
            logger.info("Error in generating dataset.")
            return None

    except Exception as ex:
        logger.info(f"Exception is occured: {ex}")
        return None


def suggestions_process_management(conjunto, pred_year, pred_month, months_ahead):
    try:
        # create dataset object
        dataset_obj = Dataset(conjunto, pred_year, pred_month)
        if not dataset_obj.has_error:
            dataset = dataset_obj.dataset

            if not dataset.empty:
                chi = Chi(conjunto, pred_year, pred_month, months_ahead)
                if not chi.has_error:
                    chi_goals = chi.conjunto_chi_goals

                    prediction = Predict(dataset, chi_goals, pred_year,
                                         pred_month, months_ahead)

                    # logger.info(prediction.feeder_preds)
                    services_obj = Services(prediction.feeder_preds, dataset, conjunto,
                                            months_ahead, chi_goals)

                    suggestions = services_obj.suggestions
                    if not services_obj.has_error:
                        # logger.info(f"services_chi = {services_chi}")
                        # logger.info(f"suggestions = {suggestions}")

                        return suggestions
                    else:
                        logger.info(
                            "An error is happen during suggestion process!")
                        return None
                else:
                    logger.info("Error in reading CHI Goals.")
                    return None
            else:
                logger.info("Dataset is empty")
                return None
        else:
            logger.info("Error in generating dataset")
            return None

    except Exception as ex:
        logger.info(f"Exception is occured: {ex}")
        return None
