import glob
import os
import sys
import zipfile
from pathlib import Path
import pandas
import tqdm
from joblib import Parallel, delayed

from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2


def original_to_dict(original_zip_path):
    original_zip_split = os.path.basename(original_zip_path).split("_")
    row = dict()
    row["seed"] = original_zip_split[-1].replace(".zip", "")
    row["num_customer_requests"] = original_zip_split[1]
    row["num_robots"] = original_zip_split[2]
    row["num_operators"] = original_zip_split[3]
    if "-" in original_zip_split[4]:
        row["utilization_time_period"] = original_zip_split[4:-1]
    else:
        row["utilization_time_period"] = []

    with zipfile.ZipFile(original_zip_path) as original_zip:
        original_results = SimulatorV2.zip_to_results_dict(original_zip)
    row[Simulator.NUM_DELIVERED] = original_results[Simulator.NUM_DELIVERED]
    row[Simulator.DELIVERY_RATE] = original_results[Simulator.DELIVERY_RATE]
    row[Simulator.UTILIZATION_RATE] = original_results[Simulator.UTILIZATION_RATE].values
    row[Simulator.NUM_RISKS] = original_results[Simulator.NUM_RISKS]

    return row


def followup_to_dict(followup_zip_path):
    followup_zip_split = os.path.basename(followup_zip_path).split("_")
    row = dict()
    row["rule"] = followup_zip_split[1]
    row["followup_idx"] = followup_zip_split[-1].replace(".zip", "")
    row["seed"] = followup_zip_split[-2]
    row["num_customer_requests"] = followup_zip_split[2]
    row["num_robots"] = followup_zip_split[3]
    row["num_operators"] = followup_zip_split[4]
    if "-" in followup_zip_split[5]:
        row["utilization_time_period"] = followup_zip_split[5:-2]
    else:
        row["utilization_time_period"] = []

    with zipfile.ZipFile(followup_zip_path) as followup_zip:
        followup_results = SimulatorV2.zip_to_results_dict(followup_zip)
    row[Simulator.NUM_DELIVERED] = followup_results[Simulator.NUM_DELIVERED]
    row[Simulator.DELIVERY_RATE] = followup_results[Simulator.DELIVERY_RATE]
    row[Simulator.UTILIZATION_RATE] = followup_results[Simulator.UTILIZATION_RATE].values
    row[Simulator.NUM_RISKS] = followup_results[Simulator.NUM_RISKS]

    return row


def zip_to_csv(zip_folder_path, output_folder_path):
    original_dicts = Parallel(n_jobs=30)(delayed(original_to_dict)(original_zip)
                                         for original_zip
                                         in tqdm.tqdm(glob.glob(str(Path(zip_folder_path).joinpath("original_*.zip"))))
                                         )

    original_dataframe = pandas.DataFrame(original_dicts)

    original_dataframe.to_csv(Path(output_folder_path).joinpath("original_results.csv"), index=False)

    followup_dicts = Parallel(n_jobs=30)(delayed(followup_to_dict)(followup_zip)
                                         for followup_zip
                                         in tqdm.tqdm(glob.glob(str(Path(zip_folder_path).joinpath("followup_*.zip"))))
                                         )

    followup_dataframe = pandas.DataFrame(followup_dicts)

    followup_dataframe.to_csv(Path(output_folder_path).joinpath("followup_results.csv"), index=False)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Need to provide path to folder with zips and path to output folder")
        exit(1)
    zip_folder_path = sys.argv[1]
    output_folder_path = sys.argv[2]
    if not Path(zip_folder_path).is_dir():
        print("Directory", zip_folder_path, "does not exist")
        exit(1)
    if not Path(output_folder_path).is_dir():
        print("Directory", output_folder_path, "does not exist")
        exit(1)

    zip_to_csv(zip_folder_path, output_folder_path)
