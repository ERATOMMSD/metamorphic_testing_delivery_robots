import os
import tempfile
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path
from subprocess import CalledProcessError
from typing import List, Dict, Optional, Tuple

from simulator.simulator_v2 import SimulatorV2
from Request import Request


class MetamorphicRule(ABC):
    points_by_distance = ['T21', 'T19', 'T29', 'T01', 'T23', 'T17', 'T18', 'T27', 'T24', 'T43', 'T14', 'T26', 'T15',
                          'T05', 'T22', 'T20', 'T16', 'T08', 'T42', 'T06', 'T28', 'T12', 'T04', 'T25', 'T40', 'T35',
                          'T70', 'T38', 'T36', 'T02', 'T10', 'T34', 'T09', 'T03', 'T41', 'T11', 'T50', 'T37', 'T61',
                          'T33', 'T74', 'T39', 'T52', 'T48', 'T63', 'T31', 'T75', 'T73', 'T49', 'T46', 'T54', 'T72',
                          'T71', 'T76', 'T51', 'T47', 'T60', 'T45', 'T53', 'T64', 'T67', 'T62', 'T65', 'T66']

    def __init__(self,
                 failure_direction: bool,  # True if breaking the rule means the original result is not optimal
                 simulator: SimulatorV2):
        self.failure_direction = failure_direction
        self.simulator = simulator
        self.name = "dummy"

    @abstractmethod
    def _generate_followup_inputs(self,
                                  original_input: List[Request],
                                  original_result: Dict,
                                  simulator_configuration: Dict) -> List[Tuple[Optional[Dict], List[Request]]]:
        pass

    @abstractmethod
    def _is_followed(self,
                     original_result,
                     followup_result) -> bool:
        pass

    def is_followed(self,
                    simulator_config: Dict,
                    original_input: List[Request],
                    original_result=None) -> Optional[List[Optional[bool]]]:
        # We set the demand mode and file in this method
        _simulator_config = dict(simulator_config)
        if "demand_mode" in _simulator_config:
            _simulator_config.pop("demand_mode")
        if "demand_file" in _simulator_config:
            _simulator_config.pop("demand_file")
        utilization_time_period_str = (("_" + "_".join(map(lambda tup: tup[0].isoformat("minutes") + "-"
                                                                       + tup[1].isoformat("minutes"),
                                                           _simulator_config["utilization_time_period"])))
                                       if "utilization_time_period" in _simulator_config else "").replace(":", "")
        if not original_result:
            original_file_descriptor, original_path = tempfile.mkstemp(".csv")
            os.close(original_file_descriptor)
            Request.write_test_to_csv(original_path, original_input, version=2)
            try:
                original_result = self.simulator.run_simulation("original",
                                                                str(simulator_config["num_customer_requests"]) +
                                                                "_" + str(simulator_config["num_robots"]) +
                                                                "_" + str(simulator_config["num_operators"]) +
                                                                utilization_time_period_str +
                                                                "_" + str(_simulator_config["seed"]),
                                                                demand_file=original_path,
                                                                demand_mode="file",
                                                                **_simulator_config)
            except CalledProcessError:
                return None  # Simulation crashed, we don't know if the rule is followed
            finally:
                os.remove(original_path)

        followup_inputs = self._generate_followup_inputs(original_input, original_result, _simulator_config)
        ret = []

        for i, followup_input in enumerate(followup_inputs):
            if Path(self.simulator.simulator_dir).joinpath("bin", "result", "followup"
                                                                            + "_" + self.name
                                                                            + "_" + str(_simulator_config["num_customer_requests"])
                                                                            + "_" + str(_simulator_config["num_robots"])
                                                                            + "_" + str(_simulator_config["num_operators"])
                                                                            + utilization_time_period_str
                                                                            + "_" + str(_simulator_config["seed"])
                                                                            + "_" + str(i)
                                                                            + ".zip"
                                                           ).is_file():
                with zipfile.ZipFile(Path(self.simulator.simulator_dir)
                                             .joinpath("bin", "result", "followup"
                                                                        + "_" + self.name
                                                                        + "_" + str(_simulator_config["num_customer_requests"])
                                                                        + "_" + str(_simulator_config["num_robots"])
                                                                        + "_" + str(_simulator_config["num_operators"])
                                                                        + utilization_time_period_str
                                                                        + "_" + str(_simulator_config["seed"])
                                                                        + "_" + str(i)
                                                                        + ".zip"
                                                       )
                                     ) as followup_zip:
                    followup_result = SimulatorV2.zip_to_results_dict(followup_zip)
            else:
                followup_conf, followup_reqs = followup_input
                if not followup_conf:
                    followup_conf = _simulator_config
                if "demand_file" in followup_conf:
                    followup_conf.pop("demand_file")
                followup_file_descriptor, followup_path = tempfile.mkstemp(".csv")
                os.close(followup_file_descriptor)
                Request.write_test_to_csv(followup_path, followup_reqs, version=2)
                try:
                    followup_result = self.simulator.run_simulation("followup",
                                                                    (self.name + "_"
                                                                     + str(simulator_config["num_customer_requests"])
                                                                     + "_" + str(simulator_config["num_robots"])
                                                                     + "_" + str(simulator_config["num_operators"])
                                                                     + utilization_time_period_str
                                                                     + "_" + str(_simulator_config["seed"])
                                                                     + "_" + str(i)),
                                                                    demand_file=followup_path,
                                                                    demand_mode="file",
                                                                    **followup_conf)
                except CalledProcessError:
                    ret.append(None)  # Simulation crashed, we don't know if the rule is followed
                    continue
                finally:
                    os.remove(followup_path)
            ret.append(self._is_followed(original_result, followup_result))
        return ret
