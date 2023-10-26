import copy
import math
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class ServedFurtherMax(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 number_followups: int = 1
                 ):
        super().__init__(False, simulator)
        self.number_followups = number_followups
        self.name = "ServedFurtherMax"

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result,
                                  simulator_configuration: Dict,
                                  seed: int = None):
        followup_inputs = []

        original_robot_requests_db = original_result["robot_requests_db"]
        served_requests_ids = original_robot_requests_db.loc[(original_robot_requests_db["request_type"] == "DELIVERY")
                                                             & (original_robot_requests_db["status"] == "COMPLETED")]["customer_request_id"]
        served_requests = list(filter(lambda r: r.customer_request_id in served_requests_ids.values, original_input))
        served_requests = sorted(served_requests,
                                 key=lambda r: self.points_by_distance.index(r.delivery_target))

        number_followups = min(len(served_requests), self.number_followups)

        for i in range(number_followups):
            followup_requests = []
            followup_requests.extend(original_input)
            index_to_modify = math.floor(i * (len(served_requests) - 1)
                                         / (number_followups - 1)) if number_followups > 1 else 0
            request_to_modify = served_requests[index_to_modify]
            points_further = self.points_by_distance[self.points_by_distance.index(request_to_modify.delivery_target)+1:]
            if len(points_further) > 0:
                changed_request = copy.copy(request_to_modify)
                changed_request.delivery_target = points_further[-1]
                followup_requests[followup_requests.index(request_to_modify)] = changed_request
                followup_inputs.append((None, followup_requests))

        return followup_inputs

    def _is_followed(self, original_result, followup_results):
        return original_result[Simulator.NUM_DELIVERED] >= followup_results[Simulator.NUM_DELIVERED]
