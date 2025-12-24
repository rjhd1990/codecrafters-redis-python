import time

class StreamItems:
    def __init__(self, value_items: list = []):
        self.value_items = value_items
        self.last_id: tuple[int, int] = (0, -1)  # [Timestamp, sequence]

    def _parse_id(self, sid: str):
        if sid == "*":
            ms_time = int(round(time.time()*1000))
            seq = "*"
        else:
            ms_time, seq = sid.split("-")
        last_time, last_seq = self.last_id
        if seq == "*":
            if int(ms_time) == 0 and last_seq <= 0:
                seq = 1 # Special case: ms_time 0 starts at 1
            else:
                if last_time != int(ms_time):
                    seq = 0
                else:
                    seq = last_seq + 1
        return ms_time, seq

    def xadd(self, sid: str, item: dict = {}):
        if sid == "0-0":
            return False, "ERR The ID specified in XADD must be greater than 0-0"
        ms_time, seq = self._parse_id(sid)
        if int(ms_time) < self.last_id[0] or (
            int(ms_time) == self.last_id[0] and int(seq) <= self.last_id[1]
        ):
            return (
                False,
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            )
        item["id"] = sid
        self.value_items.append(item)
        self.last_id = [int(ms_time), int(seq)]
        return True, f"{ms_time}-{seq}"

    def view_item(self):
        return self.value_items
