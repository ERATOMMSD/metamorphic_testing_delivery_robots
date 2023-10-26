import csv
import datetime
import typing

import random
from typing import List, Iterable

class Request:
    operation_start_time: datetime.datetime = None
    operation_end_time: datetime.datetime = None
    pickup_targets: List[str] = None
    delivery_targets: List[str] = None

    def __init__(self,
                 customer_request_id: int,
                 order_time: datetime.datetime,
                 pickup_target: str,  # From a list. Defined in a file?
                 delivery_target: str,  # From a list. Defined in a file? Same list as pickup?
                 baggage_quantity: int,  # Always 1???
                 pickup_desired_start_time: datetime.datetime,
                 pickup_desired_end_time: datetime.datetime,
                 delivery_desired_start_time: datetime.datetime,
                 delivery_desired_end_time: datetime.datetime):  # Always end of operating time? or > delivery_start
        self.customer_request_id = customer_request_id
        self.order_time = order_time
        self.pickup_target = pickup_target
        self.delivery_target = delivery_target
        self.baggage_quantity = baggage_quantity
        self.pickup_desired_start_time = pickup_desired_start_time
        self.pickup_desired_end_time = pickup_desired_end_time
        self.delivery_desired_start_time = delivery_desired_start_time
        self.delivery_desired_end_time = delivery_desired_end_time

    def __eq__(self, other):
        if not isinstance(other, Request):
            return False
        else:
            return self.order_time == other.order_time \
                and self.pickup_target == other.pickup_target \
                and self.delivery_target == other.delivery_target \
                and self.baggage_quantity == other.baggage_quantity \
                and self.pickup_desired_start_time == other.pickup_desired_start_time \
                and self.pickup_desired_end_time == other.pickup_desired_end_time \
                and self.delivery_desired_start_time == other.delivery_desired_start_time \
                and self.delivery_desired_end_time == other.delivery_desired_end_time

    def __hash__(self):
        return hash((self.order_time, self.pickup_target, self.delivery_target, self.baggage_quantity,
                     self.pickup_desired_start_time, self.pickup_desired_end_time, self.delivery_desired_start_time,
                     self.delivery_desired_end_time))

    @classmethod
    def random(cls,
               customer_request_id: int,
               operation_start_time: datetime.datetime = None,
               operation_end_time: datetime.datetime = None,
               pickup_targets: List[str] = None,
               delivery_targets: List[str] = None,
               random_generator: random.Random = None):
        if operation_start_time is None:
            operation_start_time = Request.operation_start_time
        else:
            Request.operation_start_time = operation_start_time

        if operation_end_time is None:
            operation_end_time = Request.operation_end_time
        else:
            Request.operation_end_time = operation_end_time

        if pickup_targets is None:
            pickup_targets = Request.pickup_targets
        else:
            Request.pickup_targets = pickup_targets

        if delivery_targets is None:
            delivery_targets = Request.delivery_targets
        else:
            Request.delivery_targets = delivery_targets

        if random_generator is None:
            _random_generator = random.Random()
        else:
            _random_generator = random_generator

        order_time = operation_start_time - datetime.timedelta(days=_random_generator.random())
        pickup_target = _random_generator.choice(pickup_targets)
        delivery_target = _random_generator.choice(delivery_targets)
        return cls(customer_request_id,
                   order_time,
                   pickup_target,
                   delivery_target,
                   1,
                   operation_start_time,
                   operation_end_time,
                   operation_start_time,
                   operation_end_time
                   )

    def to_csv(self):
        return str(self.customer_request_id) + "," \
            + self.order_time.strftime("%Y-%m-%dT%H:%M:%S%z") + "," \
            + self.pickup_target + "," \
            + self.delivery_target + "," \
            + str(self.baggage_quantity) + "," \
            + self.pickup_desired_start_time.strftime("%Y-%m-%dT%H:%M:%S%z") + "," \
            + self.pickup_desired_end_time.strftime("%Y-%m-%dT%H:%M:%S%z") + "," \
            + self.delivery_desired_start_time.strftime("%Y-%m-%dT%H:%M:%S%z") + "," \
            + self.delivery_desired_end_time.strftime("%Y-%m-%dT%H:%M:%S%z")

    @staticmethod
    def write_test_to_csv(file_path: str,
                          test: Iterable,
                          version: int = 1):
        with open(file_path, 'w') as requests_file:
            if version == 1:
                requests_file.write("customer_request_id,order_time,pickup_target,delivery_target,"
                                + "baggage_quantity,pickup_desired_start_time,pickup_desired_end_time,"
                                + "delivery_desired_start_time,delivery_desired_end_time\n")
            else:
                requests_file.write("customer_request_id,order_time,pickup_target,delivery_target,"
                                    + "baggage_quantity,pickup_start_time,pickup_end_time,"
                                    + "delivery_start_time,delivery_end_time\n")
            sorted_test = sorted(list(test), key=lambda r: r.order_time.replace(tzinfo=None))
            for req in sorted_test:
                requests_file.write(req.to_csv())
                requests_file.write("\n")

    @staticmethod
    def read_test_from_csv_path(file_path: str):
        with open(file_path, 'r') as requests_file:
            test = Request.read_test_from_csv_file(requests_file)
        return test

    @staticmethod
    def read_test_from_csv_file(requests_file: typing.TextIO):
        test = []
        reader = csv.reader(requests_file)
        next(reader, None)  # skip header
        for row in reader:
            test.append(Request(int(row[0]),
                                datetime.datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S%z"),
                                row[2],
                                row[3],
                                int(row[4]),
                                datetime.datetime.strptime(row[5], "%Y-%m-%dT%H:%M:%S%z"),
                                datetime.datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%S%z"),
                                datetime.datetime.strptime(row[7], "%Y-%m-%dT%H:%M:%S%z"),
                                datetime.datetime.strptime(row[8], "%Y-%m-%dT%H:%M:%S%z")
                                )
                        )
        return test