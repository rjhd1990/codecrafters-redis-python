class StreamItems:
    def __init__(self, value_items: list = []):
        self.value_items = value_items
        self.last_item_ms = 0
        self.last_item_seq = 0

    def xadd(self, sid: str, item: dict = {}):
        ms, seq = sid.split("-")
        if sid == "0-0":
            return False, "ERR The ID specified in XADD must be greater than 0-0"
        if int(ms) < self.last_item_ms or (
            int(ms) == self.last_item_ms and int(seq) <= self.last_item_seq
        ):
            return (
                False,
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            )
        item["id"] = sid
        self.value_items.append(item)
        self.last_item_ms = int(ms)
        self.last_item_seq = int(seq)
        return True, sid

    def view_item(self):
        return self.value_items
