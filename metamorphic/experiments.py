import sys
import zipfile
from pathlib import Path
from itertools import product
from datetime import datetime, timedelta, time
from subprocess import CalledProcessError
from typing import Dict, List

from metamorphic.AddRequestRule import AddRequestRule
from metamorphic.AddSystematicRequestRule import AddSystematicRequestRule
from metamorphic.ChangeServiceTimeRule import ChangeServiceTimeRule
from metamorphic.ChangeUtilizationTimeRule import ChangeUtilizationTimeRule
from metamorphic.RemoveRequestRule import RemoveRequestRule
from metamorphic.MetamorphicRule import MetamorphicRule
from metamorphic.RemoveSystematicServedRequestRule import RemoveSystematicServedRequestRule
from metamorphic.RemoveSystematicUnservedRequestRule import RemoveSystematicUnservedRequestRule
from metamorphic.ServedCloserMax import ServedCloserMax
from metamorphic.ServedCloserMid import ServedCloserMid
from metamorphic.ServedCloserMin import ServedCloserMin
from metamorphic.ServedFurtherMax import ServedFurtherMax
from metamorphic.ServedFurtherMid import ServedFurtherMid
from metamorphic.ServedFurtherMin import ServedFurtherMin
from metamorphic.UnservedCloserMax import UnservedCloserMax
from metamorphic.UnservedCloserMid import UnservedCloserMid
from metamorphic.UnservedCloserMin import UnservedCloserMin
from metamorphic.UnservedFurtherMax import UnservedFurtherMax
from metamorphic.UnservedFurtherMid import UnservedFurtherMid
from metamorphic.UnservedFurtherMin import UnservedFurtherMin
from simulator.simulator_v2 import SimulatorV2

from joblib import Parallel, delayed


def run_seed(simulator: SimulatorV2,
             simulator_config: Dict,
             seed: int,
             rules: List[MetamorphicRule]):
    _simulator_config = dict(simulator_config)
    _simulator_config["seed"] = seed

    utilization_time_period_str = (("_" + "_".join(map(lambda tup: tup[0].isoformat("minutes") + "-"
                                                                   + tup[1].isoformat("minutes"),
                                                       _simulator_config["utilization_time_period"])))
                                   if "utilization_time_period" in _simulator_config else "").replace(":", "")
    original_zip_path = Path(sys.argv[1]).joinpath("bin",
                                                   "result",
                                                   "original"
                                                   + "_"
                                                   + str(_simulator_config["num_customer_requests"])
                                                   + "_"
                                                   + str(_simulator_config["num_robots"])
                                                   + "_"
                                                   + str(_simulator_config["num_operators"])
                                                   + utilization_time_period_str
                                                   + "_"
                                                   + str(seed)
                                                   + ".zip")
    try:
        if original_zip_path.is_file():
            print("Found original results for seed " + str(seed) +
                  " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
                  " num_robots " + str(_simulator_config["num_robots"]) +
                  " num_operators " + str(_simulator_config["num_operators"])
                  + " utilization_time_period " + utilization_time_period_str,
                  flush=True
                  )
            with zipfile.ZipFile(original_zip_path) as original_zip:
                original_result = SimulatorV2.zip_to_results_dict(original_zip)
        else:
            print("Running original test for seed " + str(seed) +
                  " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
                  " num_robots " + str(_simulator_config["num_robots"]) +
                  " num_operators " + str(_simulator_config["num_operators"])
                  + " utilization_time_period " + utilization_time_period_str,
                  flush=True
                  )
            original_result = simulator.run_simulation("original", str(_simulator_config["num_customer_requests"])
                                                       + "_" + str(_simulator_config["num_robots"])
                                                       + "_" + str(_simulator_config["num_operators"])
                                                       + utilization_time_period_str
                                                       + "_" + str(seed),
                                                       **_simulator_config)
    except CalledProcessError:
        print("Original run on seed " + str(seed) +
              " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
              " num_robots " + str(_simulator_config["num_robots"]) +
              " num_operators " + str(_simulator_config["num_operators"])
              + " utilization_time_period " + utilization_time_period_str
              + " crashed",
              flush=True)
        return
    with zipfile.ZipFile(original_zip_path) as original_zip_file:
        original_requests = SimulatorV2.zip_to_requests(original_zip_file)

    print("Running followup test for seed " + str(seed) +
          " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
          " num_robots " + str(_simulator_config["num_robots"]) +
          " num_operators " + str(_simulator_config["num_operators"])
          + " utilization_time_period " + utilization_time_period_str,
          flush=True)
    for rule in rules:
        followed_all = rule.is_followed(_simulator_config,
                                        original_input=original_requests,
                                        original_result=original_result)
        for followup_idx, followed in enumerate(followed_all):
            print_result(_simulator_config, followed, followup_idx, rule, seed)


def print_result(_simulator_config, followed, followup_idx, rule, seed):
    utilization_time_period_str = (("_" + "_".join(map(lambda tup: tup[0].isoformat("minutes") + "-"
                                                                   + tup[1].isoformat("minutes"),
                                                       _simulator_config["utilization_time_period"])))
                                   if "utilization_time_period" in _simulator_config else "").replace(":", "")
    if followed is None:
        print("Followup run " + str(followup_idx) + " on seed " + str(seed) +
              " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
              " num_robots " + str(_simulator_config["num_robots"]) +
              " num_operators " + str(_simulator_config["num_operators"]) +
              " utilization_time_period " + utilization_time_period_str +
              " for rule " + rule.name + " crashed",
              flush=True)
    elif followed:
        print("Rule " + rule.name + " followed for followup " + str(followup_idx) + " seed " + str(seed) +
              " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
              " num_robots " + str(_simulator_config["num_robots"]) +
              " num_operators " + str(_simulator_config["num_operators"]) +
              " utilization_time_period " + utilization_time_period_str,
              flush=True)
    else:
        print("Rule " + rule.name + " broken for followup " + str(followup_idx) + " seed " + str(seed) +
              " num_customer_requests " + str(_simulator_config["num_customer_requests"]) +
              " num_robots " + str(_simulator_config["num_robots"]) +
              " num_operators " + str(_simulator_config["num_operators"]) +
              " utilization_time_period " + utilization_time_period_str,
              flush=True)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Need to provide path to simulator and path to list of seeds.", file=sys.stderr)
        exit(1)

    simulator = SimulatorV2(sys.argv[1])

    seeds = set()
    with open(sys.argv[2], "r") as seeds_files:
        for seed in seeds_files:
            seeds.add(seed.rstrip())

    rules = []

    for mins in [5, 10, 15, 30, 45, 60]:
        rules.append(ChangeUtilizationTimeRule(simulator,
                                               timedelta(minutes=mins)))
        rules.append(ChangeUtilizationTimeRule(simulator,
                                               timedelta(minutes=-mins)))

    rules.append(AddRequestRule(simulator,
                                datetime.fromisoformat("2021-01-01T09:00:00"),
                                datetime.fromisoformat("2021-01-01T12:00:00"),
                                5))
    rules.append(AddSystematicRequestRule(simulator,
                                          datetime.fromisoformat("2021-01-01T09:00:00"),
                                          datetime.fromisoformat("2021-01-01T12:00:00"),
                                          5))
    rules.append(RemoveRequestRule(simulator,
                                   5))

    rules.append(RemoveSystematicServedRequestRule(simulator,
                                                   5))

    rules.append(RemoveSystematicUnservedRequestRule(simulator,
                                                     5))

    rules.append(ServedCloserMax(simulator,
                                 5))

    rules.append(ServedCloserMid(simulator,
                                 5))

    rules.append(ServedCloserMin(simulator,
                                 5))

    rules.append(ServedFurtherMax(simulator,
                                  5))

    rules.append(ServedFurtherMid(simulator,
                                  5))

    rules.append(ServedFurtherMin(simulator,
                                  5))

    rules.append(UnservedCloserMax(simulator,
                                   5))

    rules.append(UnservedCloserMid(simulator,
                                   5))

    rules.append(UnservedCloserMin(simulator,
                                   5))

    rules.append(UnservedFurtherMax(simulator,
                                    5))

    rules.append(UnservedFurtherMid(simulator,
                                    5))

    rules.append(UnservedFurtherMin(simulator,
                                    5))

    original_simulator_config = {"service_start_time": datetime.fromisoformat("2021-01-01T09:00:00"),
                                 "service_end_time": datetime.fromisoformat("2021-01-01T12:00:00"),
                                 "num_customer_requests": 25,
                                 "num_robots": 3,
                                 "robot_speed_kmph": 5,
                                 "robot_loading_capacity": 5,
                                 "num_operators": 2
                                 }
    configs_to_run = []

    for num_customer_requests in [20, 25, 30]:
        for num_robots in [2, 3, 4]:
            for num_operators in [1, 2, 3]:
                config_to_run = dict(original_simulator_config)
                config_to_run["num_customer_requests"] = num_customer_requests
                config_to_run["num_robots"] = num_robots
                config_to_run["num_operators"] = num_operators
                configs_to_run.append(config_to_run)
                if num_robots == 2:
                    config_to_run["utilization_time_period"] = [(time(hour=9), time(hour=10, minute=30)),
                                                                (time(hour=10, minute=30), time(hour=12))]
                elif num_robots == 3:
                    config_to_run["utilization_time_period"] = [(time(hour=9), time(hour=10, minute=30)),
                                                                (time(hour=9, minute=30), time(hour=11)),
                                                                (time(hour=10, minute=30), time(hour=12))]
                elif num_robots == 4:
                    config_to_run["utilization_time_period"] = [(time(hour=9), time(hour=10, minute=30)),
                                                                (time(hour=9, minute=30), time(hour=11)),
                                                                (time(hour=10), time(hour=11, minute=30)),
                                                                (time(hour=10, minute=30), time(hour=12))]

    n_jobs = int(sys.argv[3]) if len(sys.argv) >= 4 else 16

    Parallel(n_jobs=n_jobs
             # , backend="multiprocessing"
             )(
        delayed(run_seed)(simulator, simulator_config, seed, rules)
        for (seed, simulator_config) in product(seeds, configs_to_run)
    )
