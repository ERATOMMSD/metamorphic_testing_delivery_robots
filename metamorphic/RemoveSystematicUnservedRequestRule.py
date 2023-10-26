import datetime
import math
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class RemoveSystematicUnservedRequestRule(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 number_followups: int = 1
                 ):
        super().__init__(False, simulator)
        self.number_followups = number_followups
        self.name = "RemoveSystematicUnservedRequest"

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result,
                                  simulator_configuration: Dict):
        followup_inputs = []

        original_robot_requests_db = original_result["robot_requests_db"]
        unserved_requests_ids = \
            original_robot_requests_db.loc[(original_robot_requests_db["request_type"] == "DELIVERY")
                                           & (original_robot_requests_db["status"] == "NEW")][
                "customer_request_id"]
        unserved_requests = list(
            filter(lambda r: r.customer_request_id in unserved_requests_ids.values, original_input))
        unserved_requests = sorted(unserved_requests,
                                   key=lambda r: self.points_by_distance.index(r.delivery_target))

        number_followups = min(self.number_followups, len(unserved_requests))

        for i in range(number_followups):
            index_to_remove = math.floor(i * (len(unserved_requests) - 1)
                                         / (number_followups - 1)) if number_followups > 1 else 0
            request_to_remove = unserved_requests[index_to_remove]
            followup_input = []
            followup_input.extend(original_input)
            followup_input.remove(request_to_remove)
            followup_inputs.append((None, followup_input))

        return followup_inputs

    def _is_followed(self, original_result, followup_results):
        return original_result[Simulator.NUM_DELIVERED] >= followup_results[Simulator.NUM_DELIVERED]
