from opentelemetry.propagators.textmap import Getter, Setter

Message = dict[bytes, bytes]


class MessageGetter(Getter[Message]):
    def get(self, carrier: Message, key: str) -> list[str] | None:
        val = carrier.get(key.encode(), None)
        if val is None:
            return None
        return [val.decode()]

    def keys(self, carrier: Message) -> list[str]:
        return [key.decode() for key in carrier.keys()]


class MessageSetter(Setter[Message]):
    def set(
        self,
        carrier: Message,
        key: str,
        value: str,
    ) -> None:
        carrier[key.encode()] = value.encode()


message_getter: MessageGetter = MessageGetter()
message_setter: MessageSetter = MessageSetter()
