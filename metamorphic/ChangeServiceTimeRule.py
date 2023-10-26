import datetime
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class ChangeServiceTimeRule(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 time_delta: datetime.timedelta
                 ):
        super().__init__(False, simulator)
        self.time_delta = time_delta
        self.name = "ChangeServiceTime" + str(int(time_delta.total_seconds()/60))

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result: Dict,
                                  simulator_configuration: Dict):
        if "utilization_time_period" in simulator_configuration:
            return []
        new_service_end_time = simulator_configuration["service_end_time"] + self.time_delta
        if simulator_configuration["service_start_time"] >= new_service_end_time:
            return []

        new_simulator_configuration = dict(simulator_configuration)
        new_simulator_configuration["service_end_time"] = new_service_end_time

        new_requests = []
        for request in original_input:
            new_requests.append(Request(request.customer_request_id,
                                        request.order_time,
                                        request.pickup_target,
                                        request.delivery_target,
                                        request.baggage_quantity,
                                        request.pickup_desired_start_time,
                                        new_service_end_time,
                                        request.delivery_desired_start_time,
                                        new_service_end_time)
                                )

        return [(new_simulator_configuration, new_requests)]

    def _is_followed(self, original_result, followup_results):
        if self.time_delta.days >= 0:
            return original_result[Simulator.NUM_DELIVERED] <= followup_results[Simulator.NUM_DELIVERED]
        else:
            return original_result[Simulator.NUM_DELIVERED] >= followup_results[Simulator.NUM_DELIVERED]
