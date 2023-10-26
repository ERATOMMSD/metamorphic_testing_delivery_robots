import datetime
import random
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class AddRequestRule(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 operation_start_time: datetime.datetime,
                 operation_end_time: datetime.datetime,
                 number_followups: int = 1
                 ):
        super().__init__(False, simulator)
        self.operation_start_time = operation_start_time
        self.operation_end_time = operation_end_time
        self.number_followups = number_followups
        self.name = "AddRandomRequest"

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result,
                                  simulator_configuration: Dict):
        new_id = max(map(lambda r: r.customer_request_id, original_input)) + 1

        pickup_targets = list(map(lambda r: r.pickup_target, original_input))
        delivery_targets = list(map(lambda r: r.delivery_target, original_input))

        tz = None
        if len(original_input) > 0:
            tz = original_input[0].pickup_desired_start_time.tzinfo

        random_generator = random.Random(simulator_configuration["seed"])

        followup_inputs = []

        for _ in range(self.number_followups):
            followup_input = []
            followup_input.extend(original_input)
            followup_input.append(Request.random(new_id,
                                                 self.operation_start_time.replace(tzinfo=tz),
                                                 self.operation_end_time.replace(tzinfo=tz),
                                                 pickup_targets,
                                                 delivery_targets,
                                                 random_generator))
            followup_inputs.append((None, followup_input))

        return followup_inputs

    def _is_followed(self, original_result, followup_results):
        return original_result[Simulator.NUM_DELIVERED] <= followup_results[Simulator.NUM_DELIVERED]
