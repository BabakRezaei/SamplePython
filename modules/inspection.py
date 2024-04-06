import pandas as pd
import os
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

import modules.common as common
from modules.connection_manager import read_from_db
from logger import logger


PRIORITIES = ['U', 'A', 'B', 'C']


class Inspection():
    def __init__(self, feeder_preds):
        load_dotenv()

        self.inspection_df = pd.DataFrame()

        self.feeder_preds = feeder_preds
        self.MONTHS_TO_CONSIDER = int(
            os.getenv("MONTHS_TO_CONSIDER_INSPECTIONS"))
        self.tasks_priorities = {}
        self.assigned_tasks = []

        inspection_path = os.getenv("INSPECTION_PATH")
        inspection_filename = os.getenv("INSPECTION_FILENAME")
        path = os.path.join(inspection_path, inspection_filename)
        raw_df = self.load_csv_file(path)

        # # READ FROM DATABASE
        # table = 'INSPECTION TABLE'
        # raw_df = read_from_db(table_name=table, encoding='latin-1')

        if not raw_df.empty:
            self.inspection_df = self.preparation_process(raw_df)
            del raw_df

    def load_csv_file(self, path):
        dataset = pd.DataFrame()
        try:
            dataset = pd.read_csv(path, encoding='latin-1')

        except Exception as ex:
            logger.info(f"Exception is raised: {ex}")

        return dataset

    def preparation_process(self, ins_df):
        df_cols = [common.replace_wild_characters_str(
            col) for col in list(ins_df.columns)]
        ins_df.columns = df_cols

        ins_df['data cadastro'] = pd.to_datetime(ins_df['data cadastro'])
        ins_df['date_time'] = ins_df['data cadastro'] + \
            pd.to_timedelta(ins_df['hora do cadastro'])
        ins_df['year'] = ins_df['date_time'].dt.year
        ins_df['month'] = ins_df['date_time'].dt.month

        ins_df[['descricao', 'descricao da causa']] = ins_df[[
            'descricao', 'descricao da causa']].apply(common.replace_wild_characters, axis=0)

        ins_df['quantidade'] = ins_df['quantidade'].str.replace(
            ',000', '').astype(int)
        ins_df['us servico'] = ins_df['us servico'].astype(str)
        ins_df['us servico'] = ins_df['us servico'].str.replace(
            ',', '.').astype(float)

        ins_df['feeder'] = ins_df['se'] + \
            ins_df['alimentador'].apply(lambda x: '{0:0>2}'.format(x))

        ins_df['date_time'] = pd.to_datetime(ins_df['date_time'])
        if ins_df['us servico'].isna().any():
            ins_df['us servico'] = ins_df['us servico'].fillna(0)

        return ins_df

    def assignServices(self, totalServices, availableTasks, priorities):
        totalAssigned = 0
        assignedTasks = []
        for priority in priorities:
            for i in range(len(availableTasks[priority])):
                qty = availableTasks[priority][i]['us servico']
                if totalAssigned + qty <= totalServices:
                    totalAssigned += qty
                    assignedTasks.append(
                        availableTasks[priority][i]['idservico'])
                    self.assigned_tasks.append(
                        availableTasks[priority][i]['idservico'])

        return [assignedTasks, totalAssigned]

    def find_tasks_periorities(self):
        # Find all tasks of all the feeders from last 6 months ahead
        for feeder in self.feeder_preds.keys():
            self.tasks_priorities[feeder] = {}

            # consider inspection reports for last 6 months
            months = self.MONTHS_TO_CONSIDER - 1
            from_date = self.feeder_preds[feeder]["pred_date"] - \
                relativedelta(months=months)

            tasks_df = self.inspection_df[(self.inspection_df['feeder'] == feeder) &
                                          (from_date <= self.inspection_df['date_time']) &
                                          (self.inspection_df['date_time'] <= self.feeder_preds[feeder]["pred_date"]) &
                                          (self.inspection_df['status servico'] == "PR") &
                                          (self.inspection_df['us servico'] != 0)]

            if not tasks_df.empty:
                tasks_df.reset_index(drop=True, inplace=True)

                for priority in PRIORITIES:
                    self.tasks_priorities[feeder][priority] = []
                    tmp_pri = tasks_df[(tasks_df['prioridade'] == priority)]

                    if not tmp_pri.empty:
                        tmp_pri.sort_values(by=['date_time'])
                        for _, row in tmp_pri.iterrows():
                            if not row['idservico'] in self.assigned_tasks:
                                tmp_dict = {}
                                tmp_dict['idservico'] = row['idservico']
                                tmp_dict['us servico'] = row['us servico']
                                tmp_dict['date_time'] = row['date_time']
                                self.tasks_priorities[feeder][priority].append(
                                    tmp_dict)
                    else:
                        self.tasks_priorities[feeder][priority] = []
            else:
                for priority in PRIORITIES:
                    self.tasks_priorities[feeder][priority] = []

    def manage_assign_services(self, feeder_periorities, periorities_to_execute, us_services):
        final_df = pd.DataFrame(columns=self.inspection_df.columns.to_list())
        if us_services != -1:
            remained_us_services = us_services
            for periority in feeder_periorities:
                # Check if there is any us service remained to assign to rest of feeders
                if remained_us_services > 0:
                    feeder = periority[0]

                    assigned_tasks, total_assigned = self.assignServices(remained_us_services,
                                                                         self.tasks_priorities[feeder],
                                                                         periorities_to_execute)
                    remained_us_services -= total_assigned

                    if total_assigned > 0:
                        tmp_df = self.inspection_df[self.inspection_df['idservico'].isin(
                            assigned_tasks)]
                        final_df = pd.concat([final_df, tmp_df])

                        # remove assigned tasks from available tasks and assigned the reminder to next month
                        for priority in periorities_to_execute:
                            if len(self.tasks_priorities[feeder][priority]) > 0:
                                tmp_tsks = []
                                for tsk in self.tasks_priorities[feeder][priority]:
                                    if tsk['idservico'] in assigned_tasks:
                                        tmp_tsks.append(tsk)
                                tmp_tsks.reverse()
                                for tsk in tmp_tsks:
                                    self.tasks_priorities[feeder][priority].remove(
                                        tsk)

            return final_df, remained_us_services

        else:
            for feeder in feeder_periorities.keys():
                remained_us_services = feeder_periorities[feeder]['available_US']
                # Check if there is any us service remained to assign to rest of feeders

                assigned_tasks, total_assigned = self.assignServices(remained_us_services,
                                                                     self.tasks_priorities[feeder],
                                                                     periorities_to_execute)
                remained_us_services -= total_assigned

                if total_assigned > 0:
                    tmp_df = self.inspection_df[self.inspection_df['idservico'].isin(
                        assigned_tasks)]
                    final_df = pd.concat([final_df, tmp_df])

                    feeder_periorities[feeder]['available_US'] = remained_us_services

                    # remove assigned tasks from available tasks and assigned the reminder to next month
                    for priority in periorities_to_execute:
                        if len(self.tasks_priorities[feeder][priority]) > 0:
                            tmp_tsks = []
                            for tsk in self.tasks_priorities[feeder][priority]:
                                if tsk['idservico'] in assigned_tasks:
                                    tmp_tsks.append(tsk)
                            tmp_tsks.reverse()
                            for tsk in tmp_tsks:
                                self.tasks_priorities[feeder][priority].remove(
                                    tsk)

            return final_df, remained_us_services

    def task_assignment(self, chi_goal, predicted_conj_chi, us_services):
        logger.info("Task assignment in process ...")
        assigned_tasks_df = pd.DataFrame()
        for indx in range(len(predicted_conj_chi)):
            if chi_goal[indx] >= predicted_conj_chi[indx]:
                for feeder in self.feeder_preds.keys():
                    available_US = round(
                        self.feeder_preds[feeder]['effect'] * us_services, 3)
                    self.feeder_preds[feeder]['available_US'] = available_US

                self.find_tasks_periorities()

                assigned_tmp_df, remained_us = self.manage_assign_services(
                    self.feeder_preds, PRIORITIES, -1)

                assigned_tmp_df.reset_index(drop=True, inplace=True)
                us_services = remained_us

            else:
                # sort feeders based on their delta_chi_percents, Ascending
                feeder_periorities = []
                month_index = f"M{indx + 1}"
                for feeder in self.feeder_preds.keys():
                    feeder_periorities.append(
                        (feeder, self.feeder_preds[feeder]['preds'][month_index]["delta_chi_percent"]))
                feeder_periorities.sort(key=lambda x: x[1], reverse=False)

                # trying to find tasks for each feeder and classify based on their priorities for last x months
                self.find_tasks_periorities()

                # start executing tasks with priorities U and A
                execute_priorities = ["U", "A"]
                df_UA, remained_us = self.manage_assign_services(feeder_periorities,
                                                    execute_priorities,
                                                    us_services)
                us_services = remained_us
                # start executing tasks with priorities B and C
                execute_priorities = ["B", "C"]
                df_BC, remained_us = self.manage_assign_services(feeder_periorities,
                                                    execute_priorities,
                                                    us_services)

                assigned_tmp_df = pd.concat([df_UA, df_BC])
                assigned_tmp_df.reset_index(drop=True, inplace=True)
                us_services = remained_us

            if not assigned_tmp_df.empty:
                assigned_tasks_df = pd.concat(
                    [assigned_tasks_df, assigned_tmp_df], ignore_index=True)

        logger.info("Task assignment is finished.")
        return assigned_tasks_df
