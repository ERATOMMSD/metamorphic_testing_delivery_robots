import io
import subprocess
import sys
import zipfile
from datetime import datetime, time
from typing import Literal, List, Tuple, Dict
import os
from pathlib import Path
import pandas
import shutil
from simulator import Simulator
from zipfile import ZipFile

from metamorphic.Request import Request


class SimulatorV2:
    def __init__(self, simulator_dir):
        self.simulator_dir = Path(simulator_dir)

    def run_simulation(self,
                       sim_name: str,
                       sim_id: str,
                       service_start_time: datetime,
                       service_end_time: datetime,
                       num_customer_requests: int,
                       num_robots: int,
                       robot_speed_kmph: float,
                       robot_loading_capacity: int,
                       area_name: str = "FujisawaSST",
                       seed: int = 0,
                       demand_mode: Literal["uniform", "distance", "file"] = "uniform",
                       demand_file: str = None,
                       utilization_time_period: List[Tuple[time, time]] = None,
                       num_operators: int = 1
                       ):
        if demand_mode != "file" and num_customer_requests is None:
            raise RuntimeError("Number of customer requests needs to be set when not using a requests file")
        if demand_mode == "file" and demand_file is None:
            raise RuntimeError("demand_file must be set when using demand_mode \"file\"")
        elif demand_file is not None and demand_mode != "file":
            print("Warning: Set a demand file but demand mode is not \"file\", demand file will be ignored.")

        if utilization_time_period is not None and len(utilization_time_period) > num_robots:
            utilization_time_period = utilization_time_period[:num_robots]

        command: List[str] = [str(self.simulator_dir.joinpath("bin", "run", "run").absolute()),
                              "--sim_name", sim_name,
                              "--sim_id", sim_id,
                              "--area_name", area_name,
                              "--service_start_time", service_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                              "--service_end_time", service_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                              "--num_customer_requests", str(num_customer_requests),
                              "--seed", str(seed),
                              "--demand_mode", demand_mode,
                              "--num_robots", str(num_robots),
                              "--robot_speed_kmph", str(robot_speed_kmph),
                              "--robot_loading_capacity", str(robot_loading_capacity),
                              "--num_operators", str(num_operators)
                              ]
        if demand_file is not None:
            command.extend(["--demand_file", str(demand_file)])
            if demand_mode != "file":
                print("WARNING: set a demand file but demand mode is not \"file\". Demand file will be ignored",
                      file=sys.stderr)
        if utilization_time_period is not None:
            command.extend(["--utilization_time_period",
                            ",".join(map(lambda tup: tup[0].isoformat("minutes") + "-" + tup[1].isoformat("minutes"),
                                         utilization_time_period))
                            ])

        try:
            completed_process = subprocess.run(command, check=True, capture_output=True,
                                               cwd=self.simulator_dir.joinpath("bin"))
            completed_process.check_returncode()
        except subprocess.CalledProcessError as e:
            print("Command: ", " ".join(command))
            print("-------------------- stdout --------------------")
            print(e.stdout)
            print("-------------------- stderr --------------------")
            print(e.stderr)
            raise e

        shutil.make_archive(str(self.simulator_dir.joinpath("bin", "result", sim_name + "_" + sim_id)),
                            "zip",
                            self.simulator_dir.joinpath("bin", "result"),
                            os.path.join(sim_name, sim_id))

        shutil.rmtree(self.simulator_dir.joinpath("bin", "result", sim_name, sim_id))

        with zipfile.ZipFile(self.simulator_dir.joinpath("bin", "result", sim_name + "_" + sim_id + ".zip")) as zip_file:
            return self.zip_to_results_dict(zip_file)

    @staticmethod
    def zip_to_requests(zip_file: ZipFile) -> List[Request]:
        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("customer_request.csv"),
                                       zip_file.filelist))
                           ) as customer_request_csv:
            return Request.read_test_from_csv_file(io.TextIOWrapper(customer_request_csv))

    @staticmethod
    def zip_to_results_dict(zip_file: ZipFile) -> Dict:
        sim_result = dict()
        file_name = os.path.basename(zip_file.filename)
        sim_result['sim_name'] = file_name.split("_")[0]
        sim_result['sim_id'] = file_name[file_name.find("_") + 1:]
        if sim_result['sim_id'].endswith(".zip"):
            sim_result['sim_id'] = sim_result['sim_id'][:-4]
        sim_result['seed'] = file_name.split("_")[5] if sim_result['sim_name'] == "followup" \
            else file_name.split("_")[4]
        sim_result['requests_per_hour'] = file_name.split("_")[2] if sim_result['sim_name'] == "followup" \
            else file_name.split("_")[1]

        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("cost.csv"), zip_file.filelist))) as cost_csv:
            sim_result["cost"] = pandas.read_csv(cost_csv,
                                                 header=0,
                                                 names=["robot_id", "tripmeter", "total_run_h", "total_load_h",
                                                        "total_rc_h", "total_ems_h", "total_op_shortage_h",
                                                        "utilization_rate"])
        sim_result[Simulator.UTILIZATION_RATE] = sim_result["cost"]["utilization_rate"]

        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("risk.csv"), zip_file.filelist))) as risk_csv:
            sim_result["risk"] = pandas.read_csv(risk_csv,
                                                 header=0,
                                                 names=["risk_id", "datetime",
                                                        "id0", "type0", "speed0", "lat0", "lng0", "x0", "y0",
                                                        "velocity_x0", "velocity_y0",
                                                        "id1", "type1", "speed1", "lat1", "lng1", "x1", "y1",
                                                        "velocity_x1", "velocity_y1",
                                                        "sensor_type"])
        sim_result[Simulator.NUM_RISKS] = len(sim_result["risk"])

        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("value.csv"), zip_file.filelist))) as value_csv:
            sim_result["value"] = pandas.read_csv(value_csv,
                                                  header=0,
                                                  names=["robot_id", "num_pickedup", "total_pickedup_quantity",
                                                         "num_delivered", "total_delivered_quantity"])
        sim_result[Simulator.NUM_DELIVERED] = sim_result["value"]["total_delivered_quantity"].sum()

        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("customer_request.csv"),
                                       zip_file.filelist))
                           ) as customer_request_csv:
            number_of_requests = sum(1 for _ in customer_request_csv) - 1
        sim_result[Simulator.DELIVERY_RATE] = sim_result[Simulator.NUM_DELIVERED] / number_of_requests

        with zip_file.open(next(filter(lambda fi: fi.filename.endswith("robot_requests_db.csv"), zip_file.filelist))) \
                as robot_requests_db_csv:
            sim_result["robot_requests_db"] = pandas.read_csv(robot_requests_db_csv,
                                                              header=0,
                                                              names=["robot_request_id", "customer_request_id",
                                                                     "order_time", "request_type", "target",
                                                                     "latitude", "longitude", "baggage_quantity",
                                                                     "desired_start_time", "desired_end_time",
                                                                     "status"],
                                                              usecols=["robot_request_id", "customer_request_id",
                                                                       "order_time", "request_type", "target", "status"]
                                                              )

        return sim_result
