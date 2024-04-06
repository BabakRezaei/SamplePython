import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import os
import json
from waitress import serve
from logger import logger

from modules.Task_management_process import suggestions_process_management
from modules.Task_management_process import prediction_process_management
from modules.send_response import send_response_suggestions
from modules.send_response import send_response_tasks


os.chdir(os.path.dirname(os.path.abspath(__file__)))


app_task_management = Flask(__name__)


# Define a route to receive the variables via POST request
@app_task_management.route('/task_manager', methods=['POST'])
def task_manager_route():
    ignored_conjuntos = []
    suggestions_df = pd.DataFrame()
    predictions_df = pd.DataFrame()

    # Get the variables from the client request
    try:
        data = request.json
        logger.info(f"The received payload is: {data}")
        data_list = data["array"]
        # check if the input is a list or not
        if not isinstance(data_list, list):
            data_list = [data_list]

        for req in data_list:
            algorithm = str(req["algorithm"]).lower()

            # Process the variables using the method
            if algorithm == "suggestion":
                conjunto = req["conjunto"]
                pred_year = int(req["pred_year"])
                pred_month = int(req["pred_month"])
                months_ahead = int(req["months_ahead"])

                suggestions = suggestions_process_management(
                    conjunto, pred_year, pred_month, months_ahead)

                if suggestions is None:
                    ignored_conjuntos.append(conjunto)
                else:
                    tmp_sug = pd.DataFrame.from_dict(suggestions, orient="columns")

                    suggestions_df = pd.concat([suggestions_df, tmp_sug], axis=1)

            elif algorithm == "prediction":
                conjunto = req["conjunto"]
                us_services = float(req["us_services"])
                pred_year = int(req["pred_year"])
                pred_month = int(req["pred_month"])

                tasks = prediction_process_management(conjunto, us_services, pred_year,
                                                      pred_month, 2)

                if tasks is None:
                    ignored_conjuntos.append(conjunto)
                elif tasks.empty:
                    ignored_conjuntos.append(conjunto)
                else:
                    tasks["data cadastro"] = tasks["data cadastro"].dt.strftime(
                        "%Y-%m-%d")
                    tasks["us servico"] = tasks["us servico"].astype(
                        str).replace(".", ",")
                    tasks["quantidade"] = tasks["quantidade"].astype(
                        str).replace(".", ",")
                    tasks["causa"] = tasks["causa"].fillna(0)
                    tasks["causa"] = tasks["causa"].astype(int)
                    tasks["causa"] = tasks["causa"].astype(str).replace(".", ",")

                    predictions_df = pd.concat([predictions_df, tasks], axis=0)

            else:
                response = jsonify({"Response": "Selected algorithm is not correct!"})
                response.status_code = 500
                logger.info("Error 500: Selected algorithm is not correct!")
                return response
    except Exception as ex:
        logger.info(f"an exception is occurred during reading the input. The input format is not correct. {ex}")
        logger.info("Error 500: Input error!")
        response = jsonify({"Response": "Input error!"})
        response.status_code = 500
        return response

    if not suggestions_df.empty:
        # Send the results to databus
        dicts_to_send = suggestions_df.to_dict(orient="dict")
        resp = send_response_suggestions(dicts_to_send)
        if resp is None:
            response = jsonify({"Response": "Suggestions are not sent!"})
            response.status_code = 500
            logger.info("Error 500: Suggestions are not sent!")
            return response
        else:
            response = jsonify({"Response": "Suggestions are sent suceessfully!"})
            response.status_code = 200
            logger.info("Suggestions are sent suceessfully!")
            return response

    elif not predictions_df.empty:
        # Send the results to databus
        dicts_to_send = predictions_df.to_dict(orient="records")
        resp = send_response_tasks(dicts_to_send)
        if resp is None:
            response = jsonify({"Response": "Tasks are not sent!"})
            response.status_code = 500
            logger.info("Error 500: asks are not sent!")
            return response
        else:
            response = jsonify({"Response": "Tasks are sent suceessfully!"})
            response.status_code = 200
            logger.info("Tasks are sent suceessfully!")
            return response

    else:
        response = jsonify({"Response": "Empty response! Check the log file!"})
        response.status_code = 204
        logger.info("Error 204: Empty response! Check the log file!")
        return response


# Start the Flask app
if __name__ == '__main__':
    load_dotenv()

    hostPath = os.getenv("python_app_url")
    portPath = os.getenv("python_port")

    logger.info('Running...')

    serve(app_task_management, host=hostPath, port=portPath, threads=4)

