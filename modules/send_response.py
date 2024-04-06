import json
import os
from dotenv import load_dotenv
from datetime import datetime

import modules.Requisitions as Requisitions

from logger import logger


def assembleResponseIntoDataBusFormat(Taskresults):
    # Create row data list
    rowData = []
    # Add tasks to the list
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for ServiceResult in Taskresults:
        data = {
            "@noun": "PREDICTIVE_ALGORITHM_RESULT",
            "CellsData": [
                {
                    "@index": 0,
                    "@value": ServiceResult["idservico"]
                },
                {
                    "@index": 1,
                    "@value": ServiceResult["idnsmanut"]
                },
                {
                    "@index": 2,
                    "@value": ServiceResult["status servico"]
                },
                {
                    "@index": 3,
                    "@value": ServiceResult["dispositivo"]
                },
                {
                    "@index": 4,
                    "@value": ServiceResult["se"]
                },
                {
                    "@index": 5,
                    "@value": ServiceResult["alimentador"]
                },
                {
                    "@index": 6,
                    "@value": ServiceResult["descricao"]
                },
                {
                    "@index": 7,
                    "@value": ServiceResult["quantidade"]
                },
                {
                    "@index": 8,
                    "@value": ServiceResult["us servico"]
                },
                {
                    "@index": 9,
                    "@value": ServiceResult["prioridade"]
                },
                {
                    "@index": 10,
                    "@value": ServiceResult["data cadastro"]
                },
                {
                    "@index": 11,
                    "@value": ServiceResult["hora do cadastro"]
                },
                {
                    "@index": 12,
                    "@value": ServiceResult["causa"]
                },
                {
                    "@index": 13,
                    "@value": ServiceResult["descricao da causa"]
                },
                {
                    "@index": 14,
                    "@value": ServiceResult["causa ciga"]
                },
                {
                    "@index": 15,
                    "@value": current_datetime
                }
            ]
        }
        rowData.append(data)

    dictResponseFormat = {
        "Header": {
            "Noun": "PREDICTIVE_ALGORITHM_RESULT",
            "Verb": "CHANGE"
        },
        "payload": {
            "items": [
                {
                    "Data": {
                        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                        "@xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "RowData": rowData
                    }
                }
            ],
            "format": None
        }
    }
    return json.dumps(dictResponseFormat)


def assembleSuggestionsIntoDataBusFormat(Suggestions):
    # Create row data list
    rowData = []
    # Add tasks to the list
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for feeder in Suggestions:
        data = {
            "@noun": "SERVICE_SUGGESTIONS_INSERT",
            "CellsData": [
                {
                    "@index": 0,
                    "@value": current_datetime
                },
                {
                    "@index": 1,
                    "@value": feeder
                },
                {
                    "@index": 2,
                    "@value": Suggestions[feeder]["conjunto"]
                },
                {
                    "@index": 3,
                    "@value": Suggestions[feeder]["from"]
                },
                {
                    "@index": 4,
                    "@value": Suggestions[feeder]["to"]
                },
                {
                    "@index": 5,
                    "@value": str(Suggestions[feeder]["chi_pred"]).replace('.', ',')
                },
                {
                    "@index": 6,
                    "@value": str(Suggestions[feeder]["chi_goal"]).replace('.', ',')
                },
                {
                    "@index": 7,
                    "@value": str(Suggestions[feeder]["chi_diff"]).replace('.', ',')
                },
                {
                    "@index": 8,
                    "@value": Suggestions[feeder]["poda_sugg"]
                },
                {
                    "@index": 9,
                    "@value": str(Suggestions[feeder]["poda_avg_us"]).replace('.', ',')
                },
                {
                    "@index": 10,
                    "@value": str(Suggestions[feeder]["poda_max_us"]).replace('.', ',')
                },
                {
                    "@index": 11,
                    "@value": Suggestions[feeder]["poda_max_year_us"]
                },
                {
                    "@index": 12,
                    "@value": str(Suggestions[feeder]["poda_cur_year_us"]).replace('.', ',')
                },
                {
                    "@index": 13,
                    "@value": str(Suggestions[feeder]["poda_year-1_us"]).replace('.', ',')
                },
                {
                    "@index": 14,
                    "@value": str(Suggestions[feeder]["poda_year-2_us"]).replace('.', ',')
                },
                {
                    "@index": 15,
                    "@value": str(Suggestions[feeder]["poda_year-3_us"]).replace('.', ',')
                },
                {
                    "@index": 16,
                    "@value": str(Suggestions[feeder]["poda_avg_interup"]).replace('.', ',')
                },
                {
                    "@index": 17,
                    "@value": Suggestions[feeder]["poda_max_interup"]
                },
                {
                    "@index": 18,
                    "@value": Suggestions[feeder]["poda_max_year_interup"]
                },
                {
                    "@index": 19,
                    "@value": Suggestions[feeder]["poda_year-1_interup"]
                },
                {
                    "@index": 20,
                    "@value": Suggestions[feeder]["poda_year-2_interup"]
                },
                {
                    "@index": 21,
                    "@value": Suggestions[feeder]["poda_year-3_interup"]
                },
                {
                    "@index": 22,
                    "@value": Suggestions[feeder]["faixa_sugg"]
                },
                {
                    "@index": 23,
                    "@value": str(Suggestions[feeder]["faixa_avg_us"]).replace('.', ',')
                },
                {
                    "@index": 24,
                    "@value": str(Suggestions[feeder]["faixa_max_us"]).replace('.', ',')
                },
                {
                    "@index": 25,
                    "@value": Suggestions[feeder]["faixa_max_year_us"]
                },
                {
                    "@index": 26,
                    "@value": str(Suggestions[feeder]["faixa_cur_year_us"]).replace('.', ',')
                },
                {
                    "@index": 27,
                    "@value": str(Suggestions[feeder]["faixa_year-1_us"]).replace('.', ',')
                },
                {
                    "@index": 28,
                    "@value": str(Suggestions[feeder]["faixa_year-2_us"]).replace('.', ',')
                },
                {
                    "@index": 29,
                    "@value": str(Suggestions[feeder]["faixa_year-3_us"]).replace('.', ',')
                },
                {
                    "@index": 30,
                    "@value": str(Suggestions[feeder]["faixa_avg_interup"]).replace('.', ',')
                },
                {
                    "@index": 31,
                    "@value": Suggestions[feeder]["faixa_max_interup"]
                },
                {
                    "@index": 32,
                    "@value": Suggestions[feeder]["faixa_max_year_interup"]
                },
                {
                    "@index": 33,
                    "@value": Suggestions[feeder]["faixa_year-1_interup"]
                },
                {
                    "@index": 34,
                    "@value": Suggestions[feeder]["faixa_year-2_interup"]
                },
                {
                    "@index": 35,
                    "@value": Suggestions[feeder]["faixa_year-3_interup"]
                },
                {
                    "@index": 36,
                    "@value": Suggestions[feeder]["estruturas_sugg"]
                },
                {
                    "@index": 37,
                    "@value": str(Suggestions[feeder]["estruturas_avg_us"]).replace('.', ',')
                },
                {
                    "@index": 38,
                    "@value": str(Suggestions[feeder]["estruturas_max_us"]).replace('.', ',')
                },
                {
                    "@index": 39,
                    "@value": Suggestions[feeder]["estruturas_max_year_us"]
                },
                {
                    "@index": 40,
                    "@value": str(Suggestions[feeder]["estruturas_cur_year_us"]).replace('.', ',')
                },
                {
                    "@index": 41,
                    "@value": str(Suggestions[feeder]["estruturas_year-1_us"]).replace('.', ',')
                },
                {
                    "@index": 42,
                    "@value": str(Suggestions[feeder]["estruturas_year-2_us"]).replace('.', ',')
                },
                {
                    "@index": 43,
                    "@value": str(Suggestions[feeder]["estruturas_year-3_us"]).replace('.', ',')
                },
                {
                    "@index": 44,
                    "@value": str(Suggestions[feeder]["estruturas_avg_interup"]).replace('.', ',')
                },
                {
                    "@index": 45,
                    "@value": Suggestions[feeder]["estruturas_max_interup"]
                },
                {
                    "@index": 46,
                    "@value": Suggestions[feeder]["estruturas_max_year_interup"]
                },
                {
                    "@index": 47,
                    "@value": Suggestions[feeder]["estruturas_year-1_interup"]
                },
                {
                    "@index": 48,
                    "@value": Suggestions[feeder]["estruturas_year-2_interup"]
                },
                {
                    "@index": 49,
                    "@value": Suggestions[feeder]["estruturas_year-3_interup"]
                }
            ]
        }
        rowData.append(data)

    dictResponseFormat = {
        "Header": {
            "Noun": "SERVICE_SUGGESTIONS_INSERT",
            "Verb": "CHANGE"
        },
        "payload": {
            "items": [
                {
                    "Data": {
                        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                        "@xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "RowData": rowData
                    }
                }
            ],
            "format": None
        }
    }
    return json.dumps(dictResponseFormat)


def send_response_suggestions(suggestions):
    load_dotenv()
    url = os.getenv("SEND_RESULTS")
    response = assembleSuggestionsIntoDataBusFormat(suggestions)

    return Requisitions.postRequestAuth(url, response)


def send_response_tasks(result):
    load_dotenv()
    url = os.getenv("SEND_RESULTS")
    response = assembleResponseIntoDataBusFormat(result)

    return Requisitions.postRequestAuth(url, response)
