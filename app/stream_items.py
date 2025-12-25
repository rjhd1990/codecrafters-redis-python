import time
import itertools
from typing import List
from dataclasses import dataclass


@dataclass
class StreamItem:
    id: str
    items: List


@dataclass
class StreamData:
    value_items: List[StreamItem] | None = None
    last_id: tuple[int, int] = (0, -1)  # [Timestamp, sequence]


class StreamHandler:
    def __init__(self, stream_data: StreamData | None = None):
        self.stream_data: StreamData = stream_data if stream_data else StreamData()
        self.value_items: List[StreamItem] = self.stream_data.value_items or []
        self.last_id: tuple[int, int] = self.stream_data.last_id

    def _parse_id(self, sid: str):
        if sid == "*":
            ms_time = int(round(time.time() * 1000))
            seq = "*"
        else:
            ms_time, seq = sid.split("-")
        last_time, last_seq = self.last_id
        if seq == "*":
            if int(ms_time) == 0 and last_seq <= 0:
                seq = 1  # Special case: ms_time 0 starts at 1
            else:
                if last_time != int(ms_time):
                    seq = 0
                else:
                    seq = last_seq + 1
        return ms_time, seq

    def xadd(self, sid: str, items: list = []):
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
        sid = f"{ms_time}-{seq}"
        self.value_items.append({"id": sid, "items": items})
        self.last_id = [int(ms_time), int(seq)]
        return True, sid

    def seralized_data(self):
        self.stream_data = StreamData(
            value_items=self.value_items, last_id=self.last_id
        )
        return self.stream_data

    def _seralize_id(self, sid):
        ms_time, seq = sid.split("-")
        if seq == "*":
            seq = 0
        return f"{ms_time}-{seq}"

    def search(self, start: str, end: str):
        print(start, end, self.value_items)
        if len(self.value_items) == 0:
            return []
        if start == "-":
            start = "0-0"
        if end == "+":
            end = f"{time.time()*1000}-{2**64 -1 }"
        
        if not "-" in start:
            start += -"0"
        if not "-" in end:
            end += f"-{2**64-1}"
            
        result = []
        for rec in self.value_items:
            rid = rec["id"]
            if start <= rid <= end:
                flattern_items = list(itertools.chain.from_iterable(rec["items"]))
                result.append([rid, flattern_items])
            if rid >= end:
                break 
        print(result)
        return result
            
