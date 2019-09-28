import asyncio
import json
import logging
import struct
import inspect
from collections import namedtuple
from typing import Dict, ByteString, Union

import aiohttp
from aiohttp import ClientConnectionError, ClientSession

from .exceptions import InitError
from .msg import Operation, CommandMsg

ROOM_INIT_URL = 'https://api.live.bilibili.com/room/v1/Room/room_init'
ROOM_CONFIG_URL = 'https://api.live.bilibili.com/room/v1/Danmu/getConf'

logger = logging.getLogger(__name__)


# 大端序列
# 4 bytes 的数据包长度, 2 bytes 的 header 长度, 2 bytes 的协议怎么, 当前为 1
# 4 bytes 的操作码: msg.Operation, 4 bytes 的 sequence 号, 可以是常数 1
MSG_HEADER_STRUCT = struct.Struct('>I2H2I')
MsgHeader = namedtuple('PacketHeader', ('packet_len', 'header_len', 'version', 'operation', 'seq'))


class BilibiliClient:
    heartbeat_interval = 20

    def __init__(self, room_id, loop=None):
        assert room_id is not None, "房间号不能为空"

        # 网址上的房间号
        self.room_id = room_id
        # 真正的房间号
        self.really_room_id = None
        # 房间用户的 uid
        self.uid = -1
        # websocket 消息服务器列表
        self.host_server_list = None
        # ip 服务器列表
        self.server_list = None
        # websocket 交互 token
        self.token = None

        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.session = None
        self.ws = None
        self.command_handlers = {}
        self.operation_handlers = {}

    async def init_room(self):
        logger.info('获取房间号的配置信息....')
        try:
            resp = await self.session.get(ROOM_INIT_URL, params={'id': self.room_id})
            if resp.status != 200:
                raise InitError(f'无法访问房间号<{self.room_id}>的配置信息: {resp.status} {resp.reason}')

            data = await resp.json()
            if data.get('code', -1) != 0:
                raise InitError(f'无法访问房间号<{self.room_id}>的配置信息: {data}')

            self.really_room_id = data['data']['room_id']
            self.uid = data['data']['uid']
        except ClientConnectionError as ex:
            logger.error(f'无法访问房间号<{self.room_id}>的配置信息: {ex}')
            raise ex

    def on_message_receive(self, operation: Operation, msg: Dict):
        if operation == Operation.AUTH_REPLY:
            self.handle_auth_reply(msg)
        elif operation == Operation.HEARTBEAT_REPLY:
            self.handle_heartbeat_reply(msg)
        else:
            command = msg.get('cmd')
            if command is None:
                # 没有消息体, 派发到用户自定义的 operation handler
                self.dispatch_operation(operation, msg)
            else:
                # 派发到 command handler
                try:
                    handler = self.command_handlers.get(CommandMsg(
                        msg.get('cmd')), self.inner_default_handler)
                except ValueError:
                    # 默认的 command handler
                    handler = self.inner_default_handler
                finally:
                    fut = handler(self.ws, msg)
                    if inspect.iscoroutine(fut):
                        asyncio.ensure_future(fut)

    async def run(self):
        async with ClientSession(loop=self.loop) as self.session:
            await self.init_room()
            await self.get_room_config()
            await self.connect()

    def run_forever(self):
        task = asyncio.ensure_future(self.run(), loop=self.loop)
        # task.add_done_callback(self.connect_callback)
        self.loop.run_forever()

    async def message_loop(self):
        async for msg in self.ws:
            offset = 0
            msg_len = len(msg.data)
            # 循环处理以应对一个消息帧有多个 command 时
            while offset < msg_len:
                # 只处理二进制消息
                if msg.type == aiohttp.WSMsgType.BINARY:
                    # 处理 header
                    header = MsgHeader(*MSG_HEADER_STRUCT.unpack_from(msg.data, offset=offset))
                    payload_start = header.header_len + offset
                    if payload_start < header.packet_len:
                        try:
                            payload = json.loads(msg.data[payload_start:offset + header.packet_len])
                        except json.JSONDecodeError as ex:
                            logger.debug("消息体并不是有效的 JSON: %s", ex)
                            payload = {}
                    else:
                        payload = {}

                    logger.debug("收到 Websocket 消息: 类型(%r), 长度(%d), Command(<%s>)",
                                 Operation(header.operation), header.packet_len,
                                 payload.get("cmd"))

                    self.on_message_receive(header.operation, payload)
                    offset += header.packet_len
                else:
                    logger.error("接收到不可处理的消息: %s", msg)
                    break

    def dispatch_operation(self, operation, msg):
        self.operation_handlers.get(operation)

    async def inner_default_handler(self, ws, command_msg):
        """内部默认的处理 command handler"""
        logger.debug("default command: %s", command_msg)

    async def connect(self):
        retry = 0
        num_of_server = len(self.host_server_list)
        while True:
            try:
                server = self.host_server_list[retry % num_of_server]
                async with self.session.ws_connect(
                        f'wss://{server["host"]}:{server["wss_port"]}/sub') as self.ws:
                    # 认证
                    await self.send_auth()
                    # 心跳包
                    heartbeat = asyncio.ensure_future(self.heartbeat_loop())
                    # 分发消息
                    await self.message_loop()
            except (ClientConnectionError, asyncio.CancelledError) as ex:
                logger.debug('Network error: %s', ex)
                break
            finally:
                try:
                    heartbeat.cancel()
                    await heartbeat
                except asyncio.CancelledError:
                    pass

    async def send_auth(self):
        data = {
            'uid': self.uid,
            'roomid': self.really_room_id,
            'protover': 1,
            'platform': 'web',
            'clientver': '1.4.0'
        }
        await self.send_packet(data, Operation.AUTH)

    async def heartbeat_loop(self):
        # 不知道什么的 heartbeat 数据, 似乎是js 空的 Object.toString(), 感觉没影响
        # data = b'\x5B\x6F\x62\x6A\x65\x63\x74\x20\x4F\x62\x6A\x65\x63\x74\x5D'
        data = b''
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval, loop=self.loop)
                await self.send_packet(data, Operation.HEARTBEAT)
            except ClientConnectionError as ex:
                logger.warning(f'heartbeat packet error with: {ex}')
                break

    async def send_packet(self, data: Union[Dict, ByteString] = None, operation=Operation.SEND_MSG):
        if data is None:
            data = b''
        elif isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')
        packet_len = MSG_HEADER_STRUCT.size + len(data)
        header = MSG_HEADER_STRUCT.pack(packet_len, MSG_HEADER_STRUCT.size,
                                        1, operation, 1)
        logger.debug(f'发送消息: 类型({operation!r}), 长度({packet_len})')
        await self.ws.send_bytes(header + data)

    async def get_room_config(self):
        try:
            resp = await self.session.get(ROOM_CONFIG_URL, params={'id': self.room_id})
            if resp.status != 200:
                raise InitError(f'无法访问房间号<{self.room_id}>的服务器信息: {resp.status} {resp.reason}')

            data = await resp.json()
            if data.get('code', -1) != 0:
                raise InitError(f'无法访问房间号<{self.room_id}>的服务器信息: {data}')

            self.host_server_list = data['data']['host_server_list']
            self.server_list = data['data']['server_list']
            self.token = data['data']['token']
        except ClientConnectionError as ex:
            logger.error(f'无法访问房间号<{self.room_id}>的服务器信息: {ex}')
            raise ex

    def handle_auth_reply(self, msg):
        """handle authentication reply message"""

    def handle_heartbeat_reply(self, msg):
        """handle beartbeat reply message"""

    def register_command(self, command, handler):
        return self.command(command)(handler)

    def command(self, command):
        def decorator(handler):
            self.command_handlers[command] = handler
            return handler
        return decorator

    def register_operation(self, operation, handler):
        return self.operation(operation)(handler)

    def operation(self, operation):
        def decorator(handler):
            self.operation_handlers[operation] = handler
            return handler
        return decorator
