import base64
import hashlib
import random
from functools import partial
from string import ascii_letters as ASCII

import msgpack
from asgiref.base_layer import BaseChannelLayer
from cached_property import threaded_cached_property
from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.exceptions import ChannelClosed, ConnectionClosed
from pika.spec import Basic, BasicProperties

try:
    from concurrent.futures import Future
except ImportError:
    from futures import Future

try:
    from threading import Thread, Lock, Event, get_ident
except ImportError:
    from threading import Thread, Lock, Event, _get_ident as get_ident

SEND = 0
RECEIVE = 1
NEW_CHANNEL = 2
GROUP_ADD = 3
GROUP_DISCARD = 4
SEND_GROUP = 5
DECLARE_DEAD_LETTERS = 6
EXPIRE_GROUP_MEMBER = 7


class Protocol(object):

    dead_letters = 'dead-letters'

    def __init__(self, expiry, group_expiry, get_capacity, ident, crypter):

        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.ident = ident
        self.crypter = crypter
        self.methods = {
            SEND: self.send,
            RECEIVE: self.receive,
            NEW_CHANNEL: self.new_channel,
            GROUP_ADD: self.group_add,
            GROUP_DISCARD: self.group_discard,
            SEND_GROUP: self.send_group,
            DECLARE_DEAD_LETTERS: self.declare_dead_letters,
            EXPIRE_GROUP_MEMBER: self.expire_group_member,
        }

    # Utilities.

    def register_channel(self, method, amqp_channel):

        self.amqp_channel = amqp_channel
        self.apply(*method)

    def apply(self, method_id, args, kwargs):

        self.methods[method_id](*args, **kwargs)

    def protocol_error(self, error):

        self.resolve.set_exception(error)

    def get_queue_name(self, channel):

        if '!' in channel:
            return channel[:channel.rfind('!') + 1]
        else:
            return channel

    # Send.

    def send(self, channel, message):

        queue = self.get_queue_name(channel)
        self.amqp_channel.queue_declare(
            partial(self.handle_publish, channel, message),
            queue=queue,
            arguments=self.queue_arguments,
        )

    def handle_publish(self, channel, message, method_frame):

        if method_frame.method.message_count >= self.get_capacity(channel):
            self.resolve.set_exception(RabbitmqChannelLayer.ChannelFull())
            return
        body = self.serialize(message)
        self.publish_message(channel, body)

    def publish_message(self, channel, body):

        queue = self.get_queue_name(channel)
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=body,
            properties=self.publish_properties(channel),
        )
        self.resolve.set_result(None)

    def publish_properties(self, channel=None):

        if channel and '!' in channel:
            headers = {'asgi_channel': channel.rsplit('!')[-1]}
        else:
            headers = None
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(headers=headers, expiration=expiration)
        return properties

    # Receive.

    def receive(self, channels, block):

        queues = set(map(self.get_queue_name, channels))
        known_queues = set()
        queues_declared = partial(self.queues_declared, queues, known_queues,
                                  channels, block)
        for queue in queues:
            self.amqp_channel.queue_declare(
                queues_declared,
                queue,
                arguments=self.queue_arguments,
            )

    def queues_declared(self, queues, known_queues, channels, block,
                        method_frame):

        known_queues.add(method_frame.method.queue)
        # If all queues are known at this moment, that basically means
        # we are in the last callback and can safely go further.  If
        # any queue isn't known, we simply skip processing at this
        # point.
        unknown_queues = queues - known_queues
        if unknown_queues:
            return
        if block:
            consumer_tags = {}
            for channel in channels:
                tag = self.amqp_channel.basic_consume(
                    partial(self.consume_message, consumer_tags),
                    queue=self.get_queue_name(channel),
                )
                consumer_tags[tag] = channel
        else:
            channels = list(channels)  # Daphne sometimes pass dict.keys()
            channel = channels[0]
            no_message = partial(self.no_message, channels[1:])
            self.amqp_channel.add_callback(no_message, [Basic.GetEmpty])
            self.amqp_channel.basic_get(
                partial(self.get_message, channel, no_message),
                queue=self.get_queue_name(channel),
            )

    def consume_message(self, consumer_tags, amqp_channel, method_frame,
                        properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag)
        channel = consumer_tags[method_frame.consumer_tag]
        if properties.headers and 'asgi_channel' in properties.headers:
            channel = channel + properties.headers['asgi_channel']
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def get_message(self, channel, no_message, amqp_channel, method_frame,
                    properties, body):

        amqp_channel.callbacks.remove(
            amqp_channel.channel_number,
            Basic.GetEmpty,
            no_message,
        )
        amqp_channel.basic_ack(method_frame.delivery_tag)
        if properties.headers and 'asgi_channel' in properties.headers:
            channel = channel + properties.headers['asgi_channel']
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def no_message(self, channels, method_frame):

        if channels:
            self.receive(channels=channels, block=False)
        else:
            self.resolve.set_result((None, None))

    @property
    def queue_arguments(self):

        return {
            'x-dead-letter-exchange': self.dead_letters,
            'x-expires': self.expiry * 2000,
        }

    # New channel.

    def new_channel(self, new_name):

        self.amqp_channel.queue_declare(
            self.new_channel_declared,
            queue=new_name,
            arguments=self.queue_arguments,
        )

    def new_channel_declared(self, method_frame):

        self.resolve.set_result(None)

    # Groups.

    def group_add(self, group, channel):

        self.expire_group_member(group, channel)

        def after_bind(method_frame):

            self.resolve.set_result(None)

        if '!' in channel:

            def bind_channel(method_frame):

                self.amqp_channel.queue_bind(
                    callback=after_bind,
                    queue=channel,
                    exchange=group,
                )

            def declare_member(method_frame):

                self.amqp_channel.queue_declare(
                    callback=bind_channel,
                    queue=channel,
                    arguments={
                        'x-dead-letter-exchange': self.dead_letters,
                        'x-expires': self.group_expiry * 1000,
                        'x-max-length': 0,
                    },
                )
        else:

            def bind_channel(method_frame):

                self.amqp_channel.queue_bind(
                    after_bind,
                    queue=channel,
                    exchange=channel,
                )

            def bind_group(method_frame):

                self.amqp_channel.exchange_bind(
                    bind_channel,
                    destination=channel,
                    source=group,
                )

            def declare_channel(method_frame):

                if '?' in channel:
                    bind_group(None)
                else:
                    self.amqp_channel.queue_declare(
                        bind_group,
                        queue=self.get_queue_name(channel),
                        arguments=self.queue_arguments,
                    )

            def declare_member(method_frame):

                self.amqp_channel.exchange_declare(
                    declare_channel,
                    exchange=channel,
                    exchange_type='fanout',
                    auto_delete=True,
                )

        self.amqp_channel.exchange_declare(
            declare_member,
            exchange=group,
            exchange_type='fanout',
            auto_delete=True,
        )

    def group_discard(self, group, channel):

        if '!' in channel:
            self.amqp_channel.queue_unbind(
                lambda method_frame: self.resolve.set_result(None),
                queue=channel,
                exchange=group,
            )
        else:
            self.amqp_channel.exchange_unbind(
                lambda method_frame: self.resolve.set_result(None),
                destination=channel,
                source=group,
            )

    def send_group(self, group, message):

        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
            properties=self.publish_properties(),
        )
        self.resolve.set_result(None)

    # Dead letters processing.

    def expire_group_member(self, group, channel):

        ttl = self.group_expiry * 1000
        self.amqp_channel.queue_declare(
            partial(self.push_marker, group, channel),
            queue=self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-max-length': 1,
                'x-message-ttl': ttl,
                # Give broker some time to expire message before
                # expire whole queue.
                'x-expires': ttl + 25,
            },
        )

    def push_marker(self, group, channel, method_frame):

        body = self.serialize({
            'group': group,
            'channel': channel,
        })
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=self.get_expire_marker(group, channel),
            body=body,
        )

    def get_expire_marker(self, group, channel):

        return 'expire.bind.%s.%s' % (group, channel)

    def declare_dead_letters(self):

        def consume(method_frame):

            self.amqp_channel.basic_consume(
                self.on_dead_letter,
                queue=self.dead_letters,
            )

        def do_bind(method_frame):

            self.amqp_channel.queue_bind(
                consume,
                queue=self.dead_letters,
                exchange=self.dead_letters,
            )

        def declare_queue(method_frame):

            self.amqp_channel.queue_declare(
                do_bind,
                queue=self.dead_letters,
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # Take the most recent death reason.
        queue = properties.headers['x-death'][0]['queue']
        reason = properties.headers['x-death'][0]['reason']
        if reason == 'expired' and self.is_expire_marker(queue):
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(group, channel)
        elif reason == 'expired' and not self.is_expire_marker(queue):
            if '!' in queue:
                queue = queue + properties.headers['asgi_channel']
                amqp_channel.queue_delete(queue=queue)
            else:
                amqp_channel.exchange_delete(exchange=queue)
        elif reason == 'maxlen' and '!' in queue:
            self.publish_message(queue, body)

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    # Serialization.

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)
        return value

    def deserialize(self, message):

        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, encoding='utf8')


class LayerChannel(Channel):

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = None
        super(LayerChannel, self).__init__(*args, **kwargs)

    def _on_deliver(self, method_frame, header_frame, body):

        try:
            super(LayerChannel, self)._on_deliver(method_frame, header_frame,
                                                  body)
        except Exception as error:
            if self.on_callback_error_callback:
                self.on_callback_error_callback(error)

    def _on_getok(self, method_frame, header_frame, body):

        try:
            super(LayerChannel, self)._on_getok(method_frame, header_frame,
                                                body)
        except Exception as error:
            if self.on_callback_error_callback:
                self.on_callback_error_callback(error)

    def _on_close(self, method_frame):

        super(LayerChannel, self)._on_close(method_frame)
        if self.on_callback_error_callback:
            self.on_callback_error_callback(
                ChannelClosed(
                    method_frame.method.reply_code,
                    method_frame.method.reply_text,
                ),
            )


class LayerConnection(SelectConnection):

    Channel = LayerChannel

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = kwargs.pop(
            'on_callback_error_callback',
        )
        self.lock = kwargs.pop('lock')
        super(LayerConnection, self).__init__(*args, **kwargs)

    def _process_frame(self, frame_value):

        with self.lock:
            return super(LayerConnection, self)._process_frame(frame_value)

    def _process_callbacks(self, frame_value):

        try:
            return super(LayerConnection, self)._process_callbacks(frame_value)
        except Exception as error:
            self.on_callback_error_callback(error)
            raise

    def _create_channel(self, channel_number, on_open_callback):

        return self.Channel(self, channel_number, on_open_callback)


class RabbitmqConnection(object):

    Parameters = URLParameters
    Connection = LayerConnection
    Protocol = Protocol

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        self.url = url
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.crypter = crypter

        self.protocols = {}
        self.lock = Lock()
        self.is_open = Event()
        self.parameters = self.Parameters(self.url)
        self.connection = self.Connection(
            parameters=self.parameters,
            on_open_callback=self.start_loop,
            on_close_callback=self.notify_futures,
            on_callback_error_callback=self.protocol_error,
            stop_ioloop_on_close=False,
            lock=self.lock,
        )

    def run(self):

        self.connection.ioloop.start()

    def start_loop(self, connection):

        self.is_open.set()
        self.process(None, (DECLARE_DEAD_LETTERS, (), {}), Future())

    def process(self, ident, method, future):

        if (ident in self.protocols and
                self.protocols[ident].amqp_channel.is_open):
            self.protocols[ident].resolve = future
            self.protocols[ident].apply(*method)
            return
        protocol = self.Protocol(self.expiry, self.group_expiry,
                                 self.get_capacity, ident, self.crypter)
        protocol.resolve = future
        amqp_channel = self.connection.channel(
            partial(protocol.register_channel, method),
        )
        amqp_channel.on_callback_error_callback = protocol.protocol_error
        self.protocols[ident] = protocol

    def notify_futures(self, connection, code, msg):

        try:
            for protocol in self.protocols.values():
                protocol.resolve.set_exception(ConnectionClosed())
        finally:
            self.connection.ioloop.stop()

    def protocol_error(self, error):

        for protocol in self.protocols.values():
            protocol.resolve.set_exception(error)

    def schedule(self, f, *args, **kwargs):

        if self.connection.is_closing or self.connection.is_closed:
            raise ConnectionClosed
        self.is_open.wait()
        future = Future()
        with self.lock:
            self.process(get_ident(), (f, args, kwargs), future)
        return future

    @property
    def thread_protocol(self):

        return self.protocols[get_ident()]


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    Connection = RabbitmqConnection

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.connection = self.Connection(url, expiry, group_expiry,
                                          get_capacity, crypter)

    def run(self):

        self.connection.run()

    def schedule(self, f, *args, **kwargs):

        return self.connection.schedule(f, *args, **kwargs)


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups']

    Thread = ConnectionThread

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 symmetric_encryption_keys=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        if symmetric_encryption_keys:
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run without 'cryptography' installed")
            sub_fernets = [
                self.make_fernet(key) for key in symmetric_encryption_keys
            ]
            crypter = MultiFernet(sub_fernets)
        else:
            crypter = None
        self._thread = self.Thread(url, expiry, group_expiry,
                                   self.get_capacity, crypter)

    @threaded_cached_property
    def thread(self):
        """
        Connection thread.  Holds connection heartbeats.  Ensure that
        thread is started.
        """

        self._thread.start()
        return self._thread

    def make_fernet(self, key):

        from cryptography.fernet import Fernet
        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    def send(self, channel, message):

        assert self.valid_channel_name(channel), 'Channel name is not valid'
        future = self.thread.schedule(SEND, channel, message)
        return future.result()

    def receive(self, channels, block=False):

        for channel in channels:
            fail_msg = 'Channel name %s is not valid' % channel
            assert self.valid_channel_name(channel, receive=True), fail_msg
        future = self.thread.schedule(RECEIVE, channels, block)
        return future.result()

    def new_channel(self, pattern):

        assert pattern.endswith('?')
        random_string = "".join(random.choice(ASCII) for i in range(20))
        new_name = pattern + random_string
        future = self.thread.schedule(NEW_CHANNEL, new_name)
        future.result()
        return new_name

    def group_add(self, group, channel):

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        future = self.thread.schedule(GROUP_ADD, group, channel)
        return future.result()

    def group_discard(self, group, channel):

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        future = self.thread.schedule(GROUP_DISCARD, group, channel)
        return future.result()

    def send_group(self, group, message):

        assert self.valid_group_name(group), 'Group name is not valid'
        future = self.thread.schedule(SEND_GROUP, group, message)
        try:
            return future.result()
        except ChannelClosed:
            # Channel was closed because corresponding group exchange
            # does not exist yet.  This mean no one call `group_add`
            # yet, so group is empty and we should not worry about.
            pass


# TODO: Is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.
