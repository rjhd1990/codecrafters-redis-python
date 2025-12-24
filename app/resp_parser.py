def parse_rep(message):
    parts = message.split("\r\n")
    if not parts[0].startswith("*"):
        return []
    num_elements = int(parts[0][1:])
    index = 1
    result = []
    for i in range(num_elements):
        if index < len(parts) and parts[index].startswith("$"):
            index += 1
            result.append(parts[index])
            index += 1
    return result


class RESP_Encoder:
    def bulk_string(arg):
        return f"${len(arg)}\r\n{arg}\r\n".encode()

    def simple_string(message):
        return f"+{message}\r\n".encode()

    def error_string(message):
        return f"-{message}\r\n".encode()

    def array_string(array):
        ret =""
        def _array_converter(items):
            ret = f"*{len(items)}\r\n"
            for item in items:
                if isinstance(item, list):
                    ret += _array_converter(item)
                else:
                    ret += f"${len(item)}\r\n{item}\r\n"
            return ret
        ret = _array_converter(array)
        return ret.encode()