class StreamItems:
    def __init__(self, value_items: list = []):
        self.value_items = value_items

    def add_item(self, id: str, item: dict = {}):
        item["id"] = id
        self.value_items.append(item)

    def view_item(self):
        return self.value_items
