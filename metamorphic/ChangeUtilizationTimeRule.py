import datetime
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class ChangeUtilizationTimeRule(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 time_delta: datetime.timedelta
                 ):
        super().__init__(False, simulator)
        self.time_delta = time_delta
        self.name = "ChangeUtilizationTime" + str(int(time_delta.total_seconds() / 60))

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result: Dict,
                                  simulator_configuration: Dict):
        if "utilization_time_period" not in simulator_configuration:
            return []

        followups = []

        old_utilization_time_period = simulator_configuration["utilization_time_period"]
        service_start_time = simulator_configuration["service_start_time"]
        service_end_time = simulator_configuration["service_end_time"]

        for i, robot_utilisation_time in enumerate(old_utilization_time_period):
            robot_start = robot_utilisation_time[0]
            new_start = datetime.datetime.combine(service_start_time.date(), robot_start) - self.time_delta
            robot_end = robot_utilisation_time[1]
            new_end = datetime.datetime.combine(service_end_time.date(), robot_end) + self.time_delta
            if new_start <= new_end:
                if service_start_time <= new_start <= service_end_time:
                    new_config = dict(simulator_configuration)
                    new_utilization_time_period = []
                    new_utilization_time_period.extend(old_utilization_time_period)
                    new_utilization_time_period[i] = (new_start.time(), new_utilization_time_period[i][1])
                    new_config["utilization_time_period"] = new_utilization_time_period
                    followups.append((new_config, original_input))

                if service_start_time <= new_end <= service_end_time:
                    new_config = dict(simulator_configuration)
                    new_utilization_time_period = []
                    new_utilization_time_period.extend(old_utilization_time_period)
                    new_utilization_time_period[i] = (new_utilization_time_period[i][0], new_end.time())
                    new_config["utilization_time_period"] = new_utilization_time_period
                    followups.append((new_config, original_input))

        return followups

    def _is_followed(self, original_result, followup_results):
        if self.time_delta.days >= 0:
            return original_result[Simulator.NUM_DELIVERED] <= followup_results[Simulator.NUM_DELIVERED]
        else:
            return original_result[Simulator.NUM_DELIVERED] >= followup_results[Simulator.NUM_DELIVERED]
