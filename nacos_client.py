import asyncio
import os
import socket
import threading
from typing import Optional

import nacos
import yaml
from flask import Flask
from werkzeug import serving


class NacosClient(object):
    def __init__(self, app: Optional[Flask] = None) -> None:
        # Register this extension with the flask app now (if it is provided)
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions["nacos-client"] = self

        # nacos client.
        self._client = nacos.NacosClient(
            app.config["NACOS_SERVER_ADDRESSES"],
            namespace=app.config["NACOS_NAMESPACE"],
        )

        self._client.debug = app.config.get("DEBUG", False)

        # nacos params.
        if os.environ.get("FLASK_RUN_HOST") == "0.0.0.0":
            self._ip = serving.get_interface_ip(socket.AF_INET)
        elif os.environ.get("FLASK_RUN_HOST") == "[::1]":
            self._ip = serving.get_interface_ip(socket.AF_INET6)
        else:
            self._ip = "127.0.0.1"

        self._port = os.environ.get("FLASK_RUN_PORT", 5001)

        self._data_id = app.config["NACOS_DATA_ID"]
        self._group = app.config["NACOS_GROUP"]

        # @see https://blog.csdn.net/mj_zm/article/details/127802203
        self._service_name = f"{self._group}@@{app.config['NACOS_SERVICE_NAME']}"

        self._set_nacos_naming_instance(app)
        self._add_nacos_naming_heartbeat_listener(app)
        self._set_configuration_options_from_nacos(app)
        self._add_nacos_config_listener(app)

    def _set_nacos_naming_instance(self, app: Flask):
        self._client.add_naming_instance(
            service_name=self._service_name,
            ip=self._ip,
            port=self._port,
            group_name=self._group,
        )

    def _set_configuration_options_from_nacos(self, app: Flask) -> None:
        config_data = self._client.get_config(self._data_id, self._group)
        self._config_data = yaml.load(str(config_data), Loader=yaml.FullLoader)
        app.logger.info("[nacos listener] configuration found.")
        self._set_configuration_options_from_object(self._config_data, app)

    def _add_nacos_config_listener(self, app: Flask):
        def _nacos_data_change_callback(config):
            config_data = yaml.load(str(config["content"]), Loader=yaml.FullLoader)
            NacosClient._set_configuration_options_from_object(config_data, app)

        self._client.add_config_watcher(
            data_id=self._data_id,
            group=self._group,
            cb=_nacos_data_change_callback,
        )

    def _add_nacos_naming_heartbeat_listener(self, app: Flask):
        def _send_nacos_hearbeat_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._send_nacos_heartbeat(app))

        threading.Thread(target=_send_nacos_hearbeat_in_thread).start()

    async def _send_nacos_heartbeat(self, app: Flask):
        while True:
            try:
                response = await asyncio.to_thread(
                    lambda: self._client.send_heartbeat(
                        service_name=self._service_name,
                        ip=self._ip,
                        port=self._port,
                        group_name=self._group,
                    )
                )
                # app.logger.debug(f"Send heartbeat to nacos: {response}")
            except Exception as e:
                app.logger.error(f"Failed to send heartbeat to nacos: {str(e)}")
            await asyncio.sleep(5)

    @staticmethod
    def _set_configuration_options_from_object(
        config_data: dict, app: Flask = None
    ) -> None:
        for key in config_data.keys():
            if key.isupper():
                app.logger.warning(f"[nacos listener] key({key}) not upper.")
                continue
            app.config[key] = config_data.get(key)
