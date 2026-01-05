from .column_flow import ColumnFlow

class DataFlow(ColumnFlow):
    def __init__(self, ctb):
        self.ctb = ctb
        ColumnFlow.__init__(self, ctb)

    def define_data_flow_event_mask(
        self,
        column_name: str,
        aux_function: str = "CFL_NULL",
        user_data: dict = None,
        event_list: list[str | int] = None,
        auto_start: bool = False
    ):
        """
        Define a column that triggers on a bitmask of events.
        event_list can contain strings (event names) or integers (explicit bit positions).
        """
        if user_data is None:
            user_data = {}
        if event_list is None:
            event_list = []

        bit_mask = 0
        for event in event_list:
            if isinstance(event, str):
                bit_pos = self.ctb.register_bitmask(event)
            elif isinstance(event, int):
                bit_pos = event
                # Optional: verify that the bit is already allocated
                # (uncomment if you want strict enforcement)
                # if bit_pos not in [v for v in self.ctb.bitmask_table.values()]:
                #     raise ValueError(f"Bit position {bit_pos} has not been allocated")
            else:
                raise TypeError(f"Event must be str or int, got {type(event).__name__}: {event}")

            bit_mask |= (1 << bit_pos)

        user_data["bit_mask"] = bit_mask

        self.ctb.add_boolean_function(aux_function)

        return self.define_column(
            column_name=column_name,
            main_function="CFL_DF_MASK_MAIN",
            initialization_function="CFL_DF_MASK_INIT",
            termination_function="CFL_DF_MASK_TERM",
            aux_function=aux_function,
            column_data={"bit_mask": bit_mask},
            auto_start=auto_start,
            label="CFL_DF_MASK"
        )

    def asm_set_bitmask(self, event_list: list[str | int]):
        """
        Generate assembly to set bits in the event mask.
        event_list can contain strings (event names) or integers (explicit bit positions).
        """
        if not isinstance(event_list, list):
            raise TypeError("event_list must be a list")

        bit_mask = 0
        for event in event_list:
            if isinstance(event, str):
                bit_pos = self.ctb.register_bitmask(event)
            elif isinstance(event, int):
                bit_pos = event
                # Optional: verify allocation (uncomment if needed)
                # if bit_pos not in [v for v in self.ctb.bitmask_table.values()]:
                #     raise ValueError(f"Bit position {bit_pos} has not been allocated")
            else:
                raise TypeError(f"Event must be str or int, got {type(event).__name__}: {event}")

            bit_mask |= (1 << bit_pos)

        bitmask_data = {"bit_mask": bit_mask}
        self.asm_one_shot_handler("CFL_SET_BITMASK", bitmask_data)

    def asm_clear_bitmask(self, event_list: list[str | int]):
        """
        Generate assembly to clear bits in the event mask.
        event_list can contain strings (event names) or integers (explicit bit positions).
        """
        if not isinstance(event_list, list):
            raise TypeError("event_list must be a list")

        bit_mask = 0
        for event in event_list:
            if isinstance(event, str):
                bit_pos = self.ctb.register_bitmask(event)
            elif isinstance(event, int):
                bit_pos = event
                # Optional: verify allocation (uncomment if needed)
                # if bit_pos not in [v for v in self.ctb.bitmask_table.values()]:
                #     raise ValueError(f"Bit position {bit_pos} has not been allocated")
            else:
                raise TypeError(f"Event must be str or int, got {type(event).__name__}: {event}")

            bit_mask |= (1 << bit_pos)

        bitmask_data = {"bit_mask": bit_mask}
        self.asm_one_shot_handler("CFL_CLEAR_BITMASK", bitmask_data)