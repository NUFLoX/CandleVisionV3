class BybitGateway:
    def __init__(self, session):
        self.session = session

    async def place_order(self, payload):
        return self.session.place_order(
            category="linear",
            symbol=payload["symbol"],
            side=payload["side"],
            orderType="Limit",
            qty=payload["qty"],
            price=payload["price"],
            stopLoss=payload["stopLoss"],
            takeProfit=payload["takeProfit"],
            orderLinkId=payload["orderLinkId"],
        )
