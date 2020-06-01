from github import Github, Repository, GitRelease, ContentFile
from loguru import logger
import configparser
import sys
import os
import wget
import hashlib
import json
from paho.mqtt.client import Client as MQTTClient, MQTTMessageInfo
from paho.mqtt.subscribe import simple as subscribe
from threading import Thread
from time import time, sleep
import socket
import ctypes

DEFAULT_COMPLETE_CONFIG_PATH = 'config.ini'
CONFIGURATION_LAYOUT = {'github': ['token', 'repo', 'check_rate'],
						'logging': ['logs_path'],
						'updates': ['download_path', 'port'],
						'mqtt': ['broker', 'updates_topic', 'updates_acks_topic', 'installed_tags_topic', 'id'],
						'trusted_files': ['files']}


class RepoFile:
	def __init__(self, name, sha, download_link):
		self.name = name
		self.sha = sha
		self.download_link = download_link
		self.path = None  # if downloaded, contains file's complete path


class DeviceUpdater(Thread):
	def __init__(self, ip, port, files, broker, installed_tags_topic, mqtt_client, release_json):
		super().__init__()
		self.ip = ip
		self.port = int(port)
		self.files = files
		self.broker = broker
		self.installed_tags_topic = installed_tags_topic
		self.mqtt_client = mqtt_client
		self.topic = f'{self.ip}_updates'
		self.release_json = release_json

	def identity(self) -> str:
		return f'Thread {self.ip}'

	def run(self) -> None:
		self.send_new_release(self.release_json)
		logger.debug(f'[{self.identity()}]: sending {len(self.files)} files')
		for i, file in enumerate(self.files):
			logger.debug(f'[{self.identity()}]: [{i}/{len(self.files)-1}] sending {file.name}...')
			if not self.send_file(file):
				logger.error(f'[{self.identity()}]: file not sent, aborting update...')
				return
			sleep(1)  # Wait a bit to not overcharge probe
		logger.debug(f'[{self.identity()}]: ')
		logger.debug(f'[{self.identity()}]: stopping me...')

	def send_end_message(self):
		logger.debug(f'[{self.identity()}]: Sending end "END" message')
		message = self.mqtt_client.publish(self.topic, payload='END', retain=True)
		self.mqtt_wait_publish(message)
		if not message.is_published():
			self.mqtt_client.reconnect()
			message = self.mqtt_client.publish(self.topic, payload='END', retain=True)
			self.mqtt_wait_publish(message)
			if not message.is_published():
				logger.error(f'[{self.identity()}]: Message not sent')
				return
		logger.debug(f'[{self.identity()}]: Message sent')

	def send_new_release(self, release_json):
		logger.debug(f"[{self.identity()}]: Sending {release_json} on {self.topic}...")
		message = self.mqtt_client.publish(self.topic, payload='', retain=True)
		self.mqtt_wait_publish(message)
		message = self.mqtt_client.publish(self.topic, payload=release_json, retain=True)
		self.mqtt_wait_publish(message)
		if not message.is_published():
			self.mqtt_client.reconnect()
			message = self.mqtt_client.publish(self.topic, payload=release_json, retain=True)
			self.mqtt_wait_publish(message)
			if not message.is_published():
				logger.error(f'[{self.identity()}]: Message not sent')
				return
		logger.debug('Message sent')

	def mqtt_wait_publish(self, message: MQTTMessageInfo, timeout=1):
		start = time()
		t_out = timeout * 1000
		while not message.is_published() and time()-start < t_out:
			sleep(0.1)
		if message.is_published():
			return True
		return False

	def wait_open_port(self, s: socket.socket):
		timeout = 3  # seconds
		tries = int(timeout/0.1)
		for _ in range(tries):
			try:
				s.connect((self.ip, self.port))
				break
			except ConnectionRefusedError:
				sleep(0.1)
		raise ConnectionRefusedError

	def send_file(self, file: RepoFile) -> bool:
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				self.wait_open_port(s)
				with open(file.path, 'rb') as f:
					content = f.read()
				if s.sendall(content) is None:
					logger.debug(f'[{self.identity()}]: {file.name} sent.')
					return True
				else:
					logger.error(f'[{self.identity()}]: {file.name} not sent.')
			return False
		except socket.timeout as stout:
			logger.error(f"[{self.identity()}]: Timeout connecting to remote socket; {stout}")
			return False
		except ConnectionRefusedError as cre:
			logger.error(f"[{self.identity()}]: remote socket is closed; {cre}")
		except Exception as e:
			logger.error(f"[{self.identity()}]: Error reading file '{file.path}'; {e}")
			return False


class FileDownloader(Thread):
	def __init__(self, file: RepoFile, download_path: str, trusted: bool):
		super().__init__()
		self.file = file
		self.download_path = download_path
		self.response = None
		self.trusted = trusted

	def run(self) -> None:
		try:
			if not self._download_file(self.file, self.download_path) or not self._verify_file(self.file):
				self.response = False
			self.response = True
		except SystemExit:
			self.response = False

	def _download_file(self, file: RepoFile, download_path) -> bool:
		logger.debug(f'Downloading {file.name} into {download_path}...')
		try:
			file_path = os.path.join(download_path, file.name)
			wget.download(file.download_link, out=file_path)
			file.path = file_path
			return True
		except Exception as e:
			logger.error(f"Can't download {file.name} from {file.download_link}")
			return False

	def _verify_file(self, file: RepoFile) -> bool:
		return True  # integrity check temporarily disabled
		logger.debug(f'Verifying {file.name} integrity...')
		if self.trusted:
			logger.warning(f'Skipping {file.name} integrity check')
			return True
		try:
			with open(file.path, 'r') as file_opened:
				content = file_opened.read()
				size = os.stat(file.path)
				hasher = hashlib.sha1(f'blob {len(content)}\x00{content}'.encode("utf-8"))
				hash = hasher.hexdigest()
				if hash == file.sha:
					logger.debug('File integrity OK')
					return True
				else:
					logger.error(f'File hash mismatch\n'
								 f'Calculated:\t{hash}\n'
								 f'Correct:\t{file.sha}')
					return False
		except Exception as e:
			logger.error(f"Can't verify {file.name} integrity; {e}")
			return False

	def kill_thread(self):
		thread_id = self.get_id()
		res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))


class MicroUpdater:
	def __init__(self, config_path=DEFAULT_COMPLETE_CONFIG_PATH):
		log_format = '<green>{time: YYYY-MM-DD HH:mm:ss.SSS}</green> <level>{level}: {message}</level>'
		logger.remove()
		logger.add(sys.stdout, format=log_format, colorize=True)
		logger.info('Starting MicroUpdater...')
		self.config = configparser.ConfigParser()
		self.read_configuration(config_path)
		logger.add(os.path.join(self.config['logging']['logs_path'], 'log_{time: YYYY-MM-DD}.log'), format=log_format, colorize=True, compression='zip', rotation='00:00')
		self.github_client = None
		self.repo_obj = None
		self.cached_release = None
		self.github_init()
		self._clean_download_folder()
		self.mqtt_client = MQTTClient(client_id=self.config['mqtt']['id'])
		self.mqtt_init()
		self.threads = {}  # ip: thread
		self.server_thread = None
		self.server_loop = False

	def loop(self):
		while True:
			try:
				while True:
					next(thread for thread in self.threads.values() if thread.is_alive())
					sleep(1)
			except StopIteration:
				pass
			tag, files = self.check_repo()
			if tag is not None:
				files = [file for file in files if ".mpy" in file.name]  # Download compiled files only
				self._download_files(files)
				update_str, update_json = self._update_json(files=files, tag=tag)
				self.cached_release = tag
				self.start_server(files, update_json)
			sleep(int(self.config['github']['check_rate']))

	def server(self, files, update_json: dict):
		while self.server_loop:
			logger.debug('Server waiting for installed tag...')
			topic = self.config['mqtt']['installed_tags_topic']
			broker = self.config['mqtt']['broker']
			message = subscribe(topic, hostname=broker)
			payload = message.payload
			msg_str = payload.decode("utf-8")
			try:
				installed_tag_json = json.loads(msg_str)
				if 'ip' not in installed_tag_json or 'tag' not in installed_tag_json:
					logger.warning('Server received a malformed installed tag message, skipping it...')
					continue
			except:
				logger.warning('Server received a malformed installed tag message, skipping it...')
				continue
			logger.debug(f'New update installed tag from {installed_tag_json["ip"]}')
			if installed_tag_json['tag'] != update_json['tag']:
				logger.debug(f"Probe out of date: installed {installed_tag_json['tag']}, latest {json.loads(update_json)['tag']}")
				self.spawn_update_thread(installed_tag_json['ip'], files, update_json)

	def spawn_update_thread(self, ip: str, files, update_json):
		logger.debug(f'Spawning new thread for {ip} update...')
		broker = self.config['mqtt']['broker']
		topic = self.config['mqtt']['installed_tags_topic']
		port = self.config['updates']['port']
		th = DeviceUpdater(ip, port=port, files=files, broker=broker, installed_tags_topic=topic, release_json=update_json, mqtt_client=self.mqtt_client)
		th.start()
		self.threads[ip] = th
		logger.debug(f'Thread spawned and registered.')

	def mqtt_wait_publish(self, message: MQTTMessageInfo, timeout=1):
		start = time()
		t_out = timeout * 1000
		while not message.is_published() and time()-start < t_out:
			sleep(0.1)
		if message.is_published():
			return True
		return False

	def start_server(self, files, update_json: dict):
		logger.debug('Starting update server...')
		self.server_loop = False
		if self.server_thread is not None:
			self.server_thread.join()
		self.server_loop = True
		self.server_thread = Thread(target=self.server, args=(files, update_json))
		self.server_thread.start()
		logger.debug('Update server started.')

	def mqtt_init(self):
		self.mqtt_client.on_connect = self.mqtt_on_connect
		self.mqtt_client.on_disconnect = self.mqtt_on_disconnect
		self.mqtt_connect()
		self.mqtt_client.loop_start()

	def mqtt_connect(self):
		broker = self.config['mqtt']['broker']
		self.mqtt_client.connect(broker)

	def mqtt_on_connect(self, client, userdata, flags, rc) -> bool:
		if rc == 0:
			logger.debug(f'MQTT client connected to {self.config["mqtt"]["broker"]}')
			return True
		else:
			logger.error(f'Connection to the broker failed, response: {rc}')
			return False

	def mqtt_on_disconnect(self, *args):
		logger.warning(f'MQTT client disconnect from the broker')
		self.mqtt_client.reconnect()

	def read_configuration(self, config_path):
		logger.debug(f'Reading configuration file "{config_path}"')
		try:
			self.config.read(config_path)
		except Exception as e:
			logger.critical(f'Error reading configuration file; {e}')
			logger.critical('Closing...')
			exit(1)
		try:
			sections = self.config.sections()
			for section in CONFIGURATION_LAYOUT:
				assert section in sections
				for key in CONFIGURATION_LAYOUT[section]:
					assert key in self.config[section]
		except AssertionError:
			logger.critical(f'Configuration file malformed, creating sample as "{DEFAULT_COMPLETE_CONFIG_PATH}"...')
			for section in CONFIGURATION_LAYOUT:
				self.config[section] = {}
				for key in CONFIGURATION_LAYOUT[section]:
					self.config[section][key] = f'<{key}>'
			try:
				if os.path.isfile(DEFAULT_COMPLETE_CONFIG_PATH):
					logger.error("Can't create configuration sample, please provide a custom configuration file")
					exit(1)
				with open(DEFAULT_COMPLETE_CONFIG_PATH, 'w') as file:
					self.config.write(file)
			except Exception as e:
				logger.critical(f"Can't create a config sample as '{DEFAULT_COMPLETE_CONFIG_PATH}' in working directory; {e}")
			finally:
				exit(1)
		logger.info(f'Configuration loaded: \n'
					f'\tToken: {self.config["github"]["token"]}\n'
					f'\tLogs path: {self.config["logging"]["logs_path"]}')
		
	def github_init(self):
		logger.debug('Initializing github attributes...')
		github = Github(self.config['github']['token'])
		self.github_client = github.get_user()
		self.repo_obj = self.github_client.get_repo(self.config['github']['repo'])
		logger.debug('Github attributes initialized.')

	def check_repo(self):  # returns: latest_tag, files
		logger.debug(f'Checking "{self.config["github"]["repo"]}" latest release tag')
		try:
			latest_release = self.repo_obj.get_latest_release()
		except:
			logger.error(f"Can't get latest release")
			return None, None
		tag = latest_release.tag_name
		if self.cached_release != tag:
			logger.info(f"New update found: {tag}")
			contents = self.repo_obj.get_contents(path='', ref=tag)
			files = [RepoFile(file.name, file.sha, file.download_url) for file in contents]
			return tag, files
		else:
			return None, None

	def _clean_download_folder(self):
		download_path = self.config['updates']['download_path']
		logger.debug(f'Cleaning download folder "{download_path}"...')
		if not os.path.isdir(download_path):
			logger.warning(f'Download folder "{download_path}" does not exists, creating it...')
			os.mkdir(download_path)
			logger.debug('Download folder ready.')
			return

		files = [os.path.join(download_path, file) for file in os.listdir(download_path)]

		logger.debug(f'{len(files)} files will be deleted..')
		for idx, file in enumerate(files):
			logger.debug(f'[{idx}/{len(files)-1}] Deleting {file}')
			if os.path.isfile(file):
				os.remove(file)
			else:
				os.rmdir(file)
		if len(os.listdir(download_path)) > 0:
			logger.error("Can't clean download folder")
			exit(1)
		logger.debug('Download folder ready.')

	def _download_files(self, files) -> bool:
		download_path = self.config['updates']['download_path']
		logger.debug(f'Downloading {len(files)} files in {download_path}...')
		self._clean_download_folder()
		trusted_files = self.config['trusted_files']['files'].split(',')
		trusted_files = [file.strip() for file in trusted_files]
		files_threads = [FileDownloader(file, download_path, file.name in trusted_files) for file in files]
		for thread in files_threads:
			thread.start()

		for thread in files_threads:
			thread.join()
			if not thread.response:
				logger.error(f"Error downloading {thread.file.name}, aborting files download")
				for th in files_threads:
					if th.is_alive():
						th.kill_thread()
				return False

		logger.debug('Files downloaded')
		return True

	def remove_empty_lines(self, file: RepoFile) -> (str, bool):
		with open(file.path, 'r') as file_opened:
			content = file_opened.read()

		#print(content)
		content = content.rstrip()
		with open(file.path, 'w') as file_opened:
			file_opened.write(content)

	def _update_json(self, tag, files) -> (str, dict):
		msg = {}
		msg['tag'] = tag
		msg['files'] = [file.name for file in files]
		update_msg = json.dumps(msg)
		update_json = msg
		return update_msg, update_json


if len(sys.argv) > 1:
	updater = MicroUpdater(sys.argv[1])
else:
	updater = MicroUpdater()
updater.loop()
