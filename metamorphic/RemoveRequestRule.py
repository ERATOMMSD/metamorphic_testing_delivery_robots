import random
from typing import List, Dict

from metamorphic.MetamorphicRule import MetamorphicRule
from simulator import Simulator
from simulator.simulator_v2 import SimulatorV2
from Request import Request


class RemoveRequestRule(MetamorphicRule):

    def __init__(self,
                 simulator: SimulatorV2,
                 number_followups: int = 1
                 ):
        super().__init__(False, simulator)
        self.number_followups = number_followups
        self.name = "RemoveRandomRequest"

    def _generate_followup_inputs(self, original_input: List[Request],
                                  original_result,
                                  simulator_configuration: Dict):

        random_generator = random.Random(simulator_configuration["seed"])

        followup_inputs = []

        for _ in range(self.number_followups):
            followup_input = []
            followup_input.extend(original_input)
            index_to_remove = random_generator.randint(0, len(followup_input) - 1)
            followup_input.pop(index_to_remove)
            followup_inputs.append((None, followup_input))

        return followup_inputs

    def _is_followed(self, original_result, followup_results):
        return original_result[Simulator.NUM_DELIVERED] >= followup_results[Simulator.NUM_DELIVERED]
