import logging
from danmu import BilibiliClient, CommandMsg


logging.basicConfig(level=logging.DEBUG)

client = BilibiliClient(3647211)


@client.command(CommandMsg.DANMU_MSG)
def on_receive_danmu(ws, msg):
    """接收到弹幕时"""
    print(f"{msg['info'][2][1]} 说: {msg['info'][1]}")


@client.command(CommandMsg.SEND_GIFT)
def on_receive_gift(ws, msg):
    print(f"感谢 {msg['data']['uname']} 送的 {msg['data']['num']} 个 {msg['data']['giftName']}")


if __name__ == '__main__':
    try:
        client.run_forever()
    except KeyboardInterrupt:
        pass
